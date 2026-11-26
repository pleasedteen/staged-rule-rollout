# 本地规则发布中间件（Local Rule-Release Middleware）

一个从零实现的 TypeScript / Node.js 服务，用来演示**按流量组分批发布规则配置**，并在
协调器崩溃、入口重启、执行服务重启、暂停、回退等故障下保持“请求版本钉住”的正确性。

- 无运行时第三方依赖：只用 Node.js 内置模块（`node:http`、`node:sqlite`、`node:crypto`…）。
- 持久化：优先使用 `node:sqlite`（Node ≥ 22.5），不可用时自动退化为原子重写的 JSON 文件。
- 一条命令在 80 秒内完成构建 + 全量验证并输出可读结果。

---

## 1. 快速开始

```bash
npm install          # 安装 typescript / @types/node（仅开发依赖）
npm run validate     # 构建 + 启动真实多进程集群 + 跑全部 92 项断言，约 24s
```

`npm run validate` 结束时打印类似：

```
ALL CHECKS PASSED (92/92), Elapsed: 23.5s (limit 80s)
```

同时把结构化结果写入 `.run/validation-report.json`。

单独启动各进程（三个角色可独立启停）：

```bash
npm run build
node dist/coordinator/main.js --data-dir .run --topology fixtures/topology.json --plan fixtures/plan.json
node dist/entry/main.js       --data-dir .run --topology fixtures/topology.json --name entry-a
# 单实例服务：--instance 可省略（实例名即服务名）
node dist/service/main.js     --data-dir .run --topology fixtures/topology.json --name ingest --instance ingest-1
node dist/service/main.js     --data-dir .run --topology fixtures/topology.json --name ingest --instance ingest-2
```

`--data-dir` 是所有进程共享的状态目录（集群库 + 服务日志 + 每实例本地库）。存储分三层：

- **集群库**（协调器与各入口共享）固定使用「锁文件 + 原子 rename」的 JSON 引擎，因为它被多个
  进程并发打开，而容器 / 网络文件系统对 SQLite 的多进程锁并不可靠（实测会 `disk I/O error` /
  `readonly database`）。
- **服务日志**（同一服务的所有实例共享）也用同一引擎，用于跨实例复用已落盘结果。
- **本地库**（每实例私有：配置缓存、boot 计数、传输计量）使用 SQLite
  （`node:sqlite`，`journal_mode=DELETE` 且关闭 mmap；单写者，安全）。

`RULES_STORE=file` 可让本地库也退回 JSON 文件；`RULES_STORE=sqlite` 在缺少 `node:sqlite` 时报错。

---

## 2. 架构

三个可独立启停的角色，全部通过本地 HTTP + 共享/本地持久化协作：

```
                 ┌───────────────┐
   ops / 校验 ───▶│  coordinator  │  /ops/*  /configs/*  /admin/*
                 └──────┬────────┘
        prepare/commit  │  (共享 CLI 控制面)
        ┌───────────────┼───────────────────┐
        ▼               ▼                   ▼
   ┌─────────┐    ┌─────────┐         ┌─────────┐
   │ entry-a │    │ entry-b │   ...   │ entry-c │   接入层（流量组）
   └────┬────┘    └────┬────┘         └────┬────┘
        │ /execute（携带钉住版本 + slot）
        ▼
   ┌──────────────┐   ┌──────────────┐   ...   执行服务（可独立重启）
   │ ingest       │──▶│ fx-math ...  │
   └──────────────┘   └──────────────┘
```

- **协调器 coordinator**：发布计划、批次状态机、向执行服务下发配置、向入口下发绑定
  （两阶段 prepare/commit）、崩溃恢复。
- **入口 entry**：受理请求（首次受理即钉住版本）、执行路由（串行 `next` + 并行 `branch`
  再 `join`）、按实例调用执行服务并在实例停机时自动换实例、跨入口恢复执行。
- **执行服务 service**：同一服务名可运行多个可独立启停的实例；无状态化计算 + 服务级
  写前日志（WAL），重启或换实例后复用已落盘结果。

### 2.1 一个服务多个实例

规则与路由仍按**服务名**配置（`ConfigDocument.services["fx-tag"]`），拓扑则可以给同一个
服务声明多个实例，每个实例是一个可独立启停的进程：

```json
{ "name": "fx-tag", "groups": ["currency"],
  "instances": [ { "name": "fx-tag-1", "port": 7412 },
                 { "name": "fx-tag-2", "port": 7432 } ] }
```

- 入口调用某服务时在其实例间路由：成功一次后**粘在**该实例（`instanceCursor`），该实例
  停机时同一请求的后续尝试立即换到兄弟实例，不需要等到原实例恢复。
- 配置按 **(实例, 版本)** 至多下发一次；实例多了只会多出固定的按实例字节，与请求量无关。
- **发布就绪按实例判定**：本批所需服务只要**至少一个实例**能执行目标版本即放行；只有全部
  实例都不可用时才阻塞，并仍只点名本批服务。
- 同一服务的所有实例共享一份服务级日志 `<data-dir>/journal/<service>.json`。某实例把结果
  落盘后、回执送达入口前挂掉时，重试落到兄弟实例会**直接复用原结果**（`reused`），
  再继续执行汇合与后续串行段。
- 只声明 `port`、不声明 `instances` 的旧拓扑仍然是“单实例服务”（实例名等于服务名），
  单实例部署完全兼容（`fixtures/topology-single.json` 与校验第 16 节覆盖）。

存储分两层，避免高频写竞争：

| 存储 | 路径 | 引擎 | 内容 |
|------|------|------|------|
| 集群库 | `<data-dir>/cluster.state.json` | 锁文件 + 原子 rename | 发布计划/批次状态、当前绑定、配置文档、请求受理账本与请求记录 |
| 服务日志 | `<data-dir>/journal/<服务名>.json` | 锁文件 + 原子 rename | 该服务所有实例共享的执行写前日志（跨实例复用结果） |
| 本地库 | `<data-dir>/local/<实例名>.sqlite` | SQLite（单写者） | 配置缓存、入口已提交绑定、boot 计数、传输计量 |

> 集群库与服务日志被多个进程并发读写，因此在容器 / 网络 / FUSE 文件系统上使用可移植的
> 锁文件引擎；这些文件系统对 SQLite 的多进程锁不可靠（会 `SIGBUS` / `disk I/O error` /
> `readonly database`）。
> 本地库只有一个写者，使用 SQLite（`journal_mode=DELETE` + 关闭 mmap），在“已提交”边界崩溃安全。
> 所有写入都走 `rename` 或 SQLite 事务，因此进程被 `kill -9` 后状态仍是完整快照。

---

## 3. 核心语义

### 3.1 版本在首次受理时钉住

`POST /accept { group, input }` 在入口处决定版本：读取该组**当前实际生效**的绑定，
写入共享账本（`ledger:<requestId>`）后才返回受理回执。此版本随后：

- 进入路由的每个执行段（`/execute` 携带 `version` 与 `slot`）；
- 分支 → 汇合时保持不变；
- 跨入口 `/retry` 重试时从账本读回，绝不重新读取“最新”绑定。

因此任何执行段若拿到与钉住版本不符的规则集，会显式报错（见 3.6），而不是悄悄混用。

### 3.2 分批发布与共享生效边界

计划由若干批次组成，每批覆盖若干流量组，`targetVersion` 为 v2。**同一批的所有组在同一
个 `epoch` 边界一起切换**（协调器先 `prepare` 全部入口，再 `commit` 同一 epoch）。
批次严格按顺序推进。

### 3.3 批次就绪只取决于本批所需服务（按实例）

推进第 N 批前，协调器只检查**本批各组路由实际引用的服务**能否执行目标版本（下发 + 复核）。
每个服务只要**至少一个实例**能执行目标版本即视为就绪；只有全部实例都不可用时才阻塞，且
`blocked` 列表仍只点名本批服务。后续批次专属的服务停机不影响当前批推进（章节 4、5 的
验证覆盖了这两面）。

### 3.4 暂停 / 显式继续

`/ops/pause` 后，`/ops/advance` 返回 `409 paused`；已生效批次保持原状。必须
`/ops/resume` 后才能继续切换。

### 3.5 回退（可在请求仍在执行时确认）

- `/ops/rollback { batchIndex }` 记录回退意图（`requested`）。
- `/ops/rollback/confirm` 在暂停期间、且有请求仍在执行时也可确认。
- 确认后，**新受理**的请求使用该组发布前（`before`）的版本；**已在执行**的请求继续用其
  钉住的版本跑完，结果正确。

### 3.6 混用版本会报错

fixture 中 v1 是数值管线、v2 是字符串管线，彼此的规则类型不兼容：

- 对 v1 的 `double` 段输入字符串 → `422 wrong_input_type`；
- 对不存在的版本执行 → `409 version_not_installed`。

### 3.7 在途请求遇实例停机自动换实例

入口对一个服务段的重试会按实例轮转：当前实例停摆后，**同一个请求的同一次段调用**立即把
同一 `requestId | slot | version | input` 送给兄弟实例。因为钉住版本与 slot 不变，兄弟实例
要么正常算出结果，要么（若原实例已落盘）直接复用原结果。分支、汇合与后续串行段都沿用
首次受理的版本，因此换实例不会改变任何段的版本解释。

### 3.8 已落盘但丢回执的执行段复用原结果

服务先把执行单元（`requestId|slot|version`，含输入）写入**服务级**日志再执行。若实例在
写完结果后、回执到达入口前退出，入口的重试落到兄弟实例：

- 兄弟实例命中 `done` 记录 → 返回原结果（响应 `cached: true`，段上 `reused: true`、
  `originInstance` 指向真正落盘的实例），**不重算**；
- 入口随后继续执行并行分支的其余部分、汇合与 `next` 串行段，整条路径仍是钉住版本。

校验第 11 节用服务端的 `POST /admin/fault {mode:"crash-after-journal"}` 精确制造这个窗口。

---

## 4. 崩溃与重启恢复

### 4.1 协调器崩溃

- 每次切换前，协调器先把 `pending`（完整目标绑定映射）**持久化**，再触碰入口。
- 入口以 prepare→commit 落盘；协调器 fan-out 后回读所有入口的 epoch，全部一致才标记批次
  `effective`。
- 协调器被 `kill -9` 后，接手进程（`main.ts` 启动即 `recover()`）：
  1. 若存在 `pending`：询问各入口实际 epoch，全部达到 → **采纳**该切换；否则丢弃未落地的意图；
  2. 任何 `switching` 批次按入口实际状态校正；
  3. 重新向入口下发当前绑定，使掉队入口追平。
- 查询 `GET /admin/state?detail=1` 可看到各批**实际生效版本**、发布状态、各入口 epoch。

### 4.2 协调器停机期间

入口按本地已提交的绑定继续受理（后台每 200 ms 尝试拉取绑定，失败则保持本地），请求正常完成。

### 4.3 执行服务 / 实例重启

- 服务对每个执行单元（`requestId|slot|version`）**先写日志再执行**；日志由该服务全部实例共享。
- 实例重启后 `boot()` 扫描**自己**留下的 `pending` 单元并确定性重算、落盘。
- 入口侧对被中断的调用重试（同版本、同 slot），换到兄弟实例或重启后的本实例，命中已完成
  结果 → 返回首次受理版本对应的输出（跨实例复用时标记 `reused`）。
- **验证覆盖了“实例停机时的在途请求自动换实例”**（章节 10）与“已落盘但丢回执的段复用
  原结果、再跑完剩余路径”（章节 11）。

### 4.4 入口崩溃

请求受理时版本已写入共享账本。任一入口 `/retry { requestId }` 都能从账本恢复并跑完，
沿用钉住版本（章节 12）。

---

## 5. 传输数据量与请求量解耦

配置按 **(实例, 版本)** 至多跨进程一次：入口/每个服务实例各自持久缓存；请求只传版本字符串
与输入，从不传全量配置。每个进程把“收到的全量配置字节”记入本地持久计量账本，
`GET /admin/transport` 按实例聚合为集群总量。

在固定拓扑（固定实例数）、固定版本集、固定启停序列下，增加请求量不改变该总量
（章节 13 验证 `delta == 0`）。实例数变化只会带来按实例的固定增量，与请求量无关。

---

## 6. HTTP 接口

### 协调器（默认 :7400）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET  | `/health` | 存活探针 |
| GET  | `/admin/state[?detail=1]` | 发布状态；`detail=1` 附各入口/服务实时视图 |
| GET  | `/admin/state/raw` | 完整内部状态（调试） |
| GET  | `/admin/bindings` | 当前组→版本绑定（含 epoch、digest） |
| GET  | `/configs` | 已发布版本列表（含 digest/size） |
| GET  | `/configs/:version` | 取配置文档 |
| POST | `/configs` | 发布新配置 `{doc}` |
| POST | `/ops/advance` | 推进到下一批（`{fault?}` 仅供故障夹具） |
| POST | `/ops/pause` / `/ops/resume` | 暂停 / 继续 |
| POST | `/ops/rollback {batchIndex?}` | 请求回退 |
| POST | `/ops/rollback/confirm` | 确认回退 |
| POST | `/ops/recover` | 手动触发恢复对账 |
| POST | `/ops/deploy {version, services?}` | 向执行服务下发指定版本 |
| GET  | `/admin/entries` / `/admin/services` | 入口 / 服务探测 |
| GET  | `/admin/records?ids=a,b` | 扇出查询请求记录 |
| GET  | `/admin/transport` | 聚合配置传输计量 |

### 入口（:7401/:7402/:7403）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/accept {group, input}` | 受理并异步执行，返回受理回执（含钉住版本） |
| POST | `/submit {group, input}` | 受理并等待完成，返回记录（便于手工验证） |
| GET  | `/requests/:id` | 查询请求记录（各段实际版本 + 最终结果） |
| POST | `/retry {requestId}` | 从账本恢复执行（可跨入口） |
| GET  | `/status` | 入口状态（epoch、组、版本、in-flight） |
| GET  | `/admin/bindings?detail=1` | 实际绑定 + 缓存版本 + in-flight |
| POST | `/admin/prepare` / `/admin/commit` | 两阶段绑定切换（协调器调用） |
| GET  | `/admin/inflight` | 正在执行的请求数 |

### 执行服务实例（每个实例一个端口，如 :7410 与 :7430 是 `ingest` 的两个实例）

`--name <服务名>` 必填；`--instance <实例名>` 在该服务有多个实例时必填，单实例可省略。

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/execute {requestId, version, slot, service, input}` | 执行一个段（结果命中落盘日志时返回 `cached:true`） |
| GET  | `/admin/versions` | 本实例已安装版本 + 日志统计 |
| POST | `/admin/config {doc, digest}` | 安装配置（校验 digest） |
| GET  | `/admin/journal` | 服务级写前日志统计 |
| GET  | `/admin/transport` | 本实例配置传输计量 |
| POST | `/admin/fault {mode:"crash-after-journal"}` | 故障夹具：写盘后自杀（模拟丢回执） |

---

## 7. 夹具（fixtures/）

| 文件 | 内容 |
|------|------|
| `topology.json` | 1 协调器 + 3 入口 + 13 执行服务，其中 5 个服务各有 2 个实例（共 18 实例）；组 currency/search/report/archive |
| `topology-single.json` | 单实例兼容夹具：只用 `port` 声明 13 个单实例服务 |
| `plan.json` | 3 批：currency；{search, report} 同批；archive |
| `configs/v1.json`、`configs/v2.json` | 两个版本；v1 数值、v2 字符串（互不兼容，混用即错） |
| `requests.json` | 各组示例请求 |
| `faults.json` | 故障场景复现步骤 |

组与批次：`batch-2-content` 用两个组（search+report）验证**同批共享边界**；
archive 服务仅被第 3 批需要，用于验证**后续批专属服务停机不影响当前批**。
`ingest / fx-math / fx-tag / fx-final / search-a` 各有两个实例，用于验证**按实例就绪**、
**在途换实例**与**跨实例复用结果**。

---

## 8. 运维操作示例

```bash
C=http://127.0.0.1:7400
curl -s $C/admin/state | jq .                      # 查看批次状态
curl -s -X POST $C/ops/advance | jq .              # 推进下一批
curl -s -X POST $C/ops/pause                        # 暂停
curl -s -X POST $C/ops/resume                       # 显式继续
curl -s -X POST $C/ops/rollback -d '{"batchIndex":0}' -H 'content-type: application/json'
curl -s -X POST $C/ops/rollback/confirm
curl -s $C/admin/state?detail=1 | jq '.batches,.entries,.services'
```

查看某请求各段实际版本与结果（含实际服务实例、是否复用）：

```bash
curl -s http://127.0.0.1:7401/requests/<requestId> \
  | jq '{version,status,output,
         segments:[.segments[]|{service,instance,slot,version,reused,originInstance,failovers,output}]}'
```

故障复现（详见 `fixtures/faults.json` 与 `docs/OPERATIONS.md`）：

```bash
kill -9 <coordinator-pid>          # 协调器崩溃
node dist/coordinator/main.js ...  # 重启，接手进程查询实际进度并继续
kill -9 <service-instance-pid>     # 某实例停机；在途请求自动换到兄弟实例
kill -9 <entry-pid>                # 入口崩溃；在另一入口 POST /retry {requestId}
```

---

## 9. 验证覆盖（`npm run validate`，92 项断言）

1. 启动 & 初始状态（全 v1）；**每个声明实例都可独立寻址**
2. 首次受理钉住版本；段版本一致（含并行分支）；结果匹配独立 oracle
3. **按实例就绪**：某服务一个实例停机仍放行第 1 批；各入口共享同一 epoch；新流量走 v2、旧组留 v1
4. 暂停语义；后续批专属服务停机不影响当前批；**一个实例停机仍可推进**；多组同批在同一 epoch 翻转
5. 本批服务的**全部实例**停机则批次被阻塞，且只点名本批服务
6. 协调器崩溃 → 入口继续受理 → 接手进程恢复并看到实际生效版本
7. 回退期间有请求在执行：确认回退成功且**观测到在途请求**，执行中请求仍用钉住 v2；新受理走 v1
8. 回退后重新推进 + 崩溃于“入口已提交、协调器未记账” → 接手进程采纳
9. 崩溃于“已写持久意图、尚未触碰入口” → 接手进程丢弃未落地切换
10. **实例停机时的在途请求自动换实例**：分支段换到兄弟实例、各段仍为钉住版本、结果正确
11. **已落盘但丢回执**：兄弟实例复用原段结果（`reused`、`originInstance` 正确），
    分支/汇合/后续串行段沿用首次受理版本，最终汇合输出匹配 oracle
12. 入口崩溃后跨入口 `/retry`：沿用钉住版本并得到正确结果
13. 请求量 24 → 全量配置传输字节 `delta == 0`；计量按实例分别记账；每请求仍执行完整路由
14. 版本混用被显式拒绝（类型错误 / 版本未安装 / 实例拒绝别的服务名）
15. 终态一致：全部批次生效、每个服务仍有健康实例；旧版本在发布完成后仍可执行
16. **单实例部署兼容**：只声明 `port` 的拓扑正常启动、路由正确、各段为钉住版本

---

## 10. 目录结构

```
src/
  shared/      types, http, store(集群=文件锁 / 本地=SQLite / 服务日志=文件锁), engine(路由/规则), configCache, peer, runtime(实例解析), util
  coordinator/ controller(发布状态机 + 按实例就绪), server(HTTP), main(进程)
  entry/       main(受理/执行/换实例/跨入口恢复/两阶段绑定)
  service/     main(实例执行 + 共享写前日志 + 故障注入)
  validate/    run(多进程校验器), oracles(独立期望值)
fixtures/      topology(多实例) / topology-single(单实例兼容) / plan / configs / requests / faults
docs/          OPERATIONS.md（运维与故障复现）
```
