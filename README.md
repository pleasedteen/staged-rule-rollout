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
npm run validate     # 构建 + 启动真实多进程集群 + 跑全部 72 项断言，约 20s
```

`npm run validate` 结束时打印类似：

```
ALL CHECKS PASSED (72/72), Elapsed: 19.7s (limit 80s)
```

同时把结构化结果写入 `.run/validation-report.json`。

单独启动各进程（三个角色可独立启停）：

```bash
npm run build
node dist/coordinator/main.js --data-dir .run --topology fixtures/topology.json --plan fixtures/plan.json
node dist/entry/main.js       --data-dir .run --topology fixtures/topology.json --name entry-a
node dist/service/main.js     --data-dir .run --topology fixtures/topology.json --name ingest
```

`--data-dir` 是所有进程共享的状态目录（集群库 + 每进程本地库）。存储分两层：

- **集群库**（协调器与各入口共享）固定使用「锁文件 + 原子 rename」的 JSON 引擎，因为它被多个
  进程并发打开，而容器 / 网络文件系统对 SQLite 的多进程锁并不可靠（实测会 `disk I/O error` /
  `readonly database`）。
- **本地库**（每进程私有：配置缓存、执行服务写前日志、boot 计数、传输计量）使用 SQLite
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
  再 `join`）、跨入口恢复执行。
- **执行服务 service**：无状态化计算 + 写前日志（WAL），重启后自愈未完成的执行单元。

存储分两层，避免高频写竞争：

| 存储 | 路径 | 引擎 | 内容 |
|------|------|------|------|
| 集群库 | `<data-dir>/cluster.state.json` | 锁文件 + 原子 rename | 发布计划/批次状态、当前绑定、配置文档、请求受理账本与请求记录 |
| 本地库 | `<data-dir>/local/<进程名>.sqlite` | SQLite（单写者） | 配置缓存、执行服务写前日志、入口已提交绑定、boot 计数、传输计量 |

> 集群库被协调器与全部入口并发读写，因此在容器 / 网络 / FUSE 文件系统上使用可移植的锁文件引擎；
> 这些文件系统对 SQLite 的多进程锁不可靠（会 `SIGBUS` / `disk I/O error` / `readonly database`）。
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

### 3.3 批次就绪只取决于本批所需服务

推进第 N 批前，协调器只检查**本批各组路由实际引用的服务**能否执行目标版本（下发 + 复核）。
后续批次专属的服务停机不影响当前批推进（章节 4、5 的验证覆盖了这两面）。

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

### 4.3 执行服务重启

- 服务对每个执行单元（`requestId|slot|version`）**先写日志再执行**。
- 重启后 `boot()` 扫描 `pending` 单元并确定性重算、落盘。
- 入口侧对被中断的调用重试（同版本、同 slot），服务命中已自愈结果 → 返回首次受理版本对应的输出。
- **验证覆盖了“执行服务重启后的首批并发请求”**（章节 10）。

### 4.4 入口崩溃

请求受理时版本已写入共享账本。任一入口 `/retry { requestId }` 都能从账本恢复并跑完，
沿用钉住版本（章节 11）。

---

## 5. 传输数据量与请求量解耦

配置按 **(进程, 版本)** 至多跨进程一次：入口/服务各自持久缓存；请求只传版本字符串与输入，
从不传全量配置。每个进程把“收到的全量配置字节”记入本地持久计量账本，
`GET /admin/transport` 聚合为集群总量。

在固定拓扑、固定版本集、固定启停序列下，增加请求量不改变该总量（章节 12 验证
`delta == 0`）。

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

### 执行服务（:7410–:7422）

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/execute {requestId, version, slot, service, input}` | 执行一个段 |
| GET  | `/admin/versions` | 已安装版本 + 日志统计 |
| POST | `/admin/config {doc, digest}` | 安装配置（校验 digest） |
| GET  | `/admin/journal` | 写前日志统计 |
| GET  | `/admin/transport` | 本服务配置传输计量 |

---

## 7. 夹具（fixtures/）

| 文件 | 内容 |
|------|------|
| `topology.json` | 1 协调器 + 3 入口 + 13 执行服务；3 个组 currency/search/report/archive |
| `plan.json` | 3 批：currency；{search, report} 同批；archive |
| `configs/v1.json`、`configs/v2.json` | 两个版本；v1 数值、v2 字符串（互不兼容，混用即错） |
| `requests.json` | 各组示例请求 |
| `faults.json` | 故障场景复现步骤 |

组与批次：`batch-2-content` 用两个组（search+report）验证**同批共享边界**；
archive 服务仅被第 3 批需要，用于验证**后续批专属服务停机不影响当前批**。

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
curl -s $C/admin/state?detail=1 | jq '.batches,.entries'
```

查看某请求各段实际版本与结果：

```bash
curl -s http://127.0.0.1:7401/requests/<requestId> | jq '{version,status,output,segments:[.segments[]|{service,slot,version,output}]}'
```

故障复现（详见 `fixtures/faults.json` 与 `docs/OPERATIONS.md`）：

```bash
kill -9 <coordinator-pid>          # 协调器崩溃
node dist/coordinator/main.js ...  # 重启，接手进程查询实际进度并继续
kill -9 <service-pid>              # 执行服务崩溃；重启后自愈未完成单元
kill -9 <entry-pid>                # 入口崩溃；在另一入口 POST /retry {requestId}
```

---

## 9. 验证覆盖（`npm run validate`，72 项断言）

1. 启动 & 初始状态（全 v1）
2. 首次受理钉住版本；段版本一致（含并行分支）；结果匹配独立 oracle
3. 第 1 批生效；各入口共享同一 epoch；其他组不受影响；新流量走 v2、旧组留 v1
4. 暂停语义；后续批专属服务停机不影响当前批；**多组同批在同一 epoch 翻转**；各入口按新版本服务
5. 本批服务停机则批次被阻塞，且只点名本批服务
6. 协调器崩溃 → 入口继续受理 → 接手进程恢复并看到实际生效版本
7. 回退期间有请求在执行：确认回退成功且**观测到在途请求**，执行中请求仍用钉住 v2；新受理走 v1
8. 回退后重新推进 + 崩溃于“入口已提交、协调器未记账” → 接手进程采纳
9. 崩溃于“已写持久意图、尚未触碰入口” → 接手进程丢弃未落地切换
10. **执行服务重启后的首批并发请求**：全部成功且各段均为钉住版本；重启前在途请求也在重启后正确完成
11. 入口崩溃后跨入口 `/retry`：沿用钉住版本并得到正确结果
12. 请求量 24 → 全量配置传输字节 `delta == 0`；每请求仍执行完整路由
13. 版本混用被显式拒绝（类型错误 / 版本未安装）
14. 终态一致：全部批次生效；旧版本在发布完成后仍可执行

---

## 10. 目录结构

```
src/
  shared/      types, http, store(集群=文件锁 / 本地=SQLite), engine(路由/规则), configCache, peer, runtime, util
  coordinator/ controller(发布状态机), server(HTTP), main(进程)
  entry/       main(受理/执行/恢复/两阶段绑定)
  service/     main(执行 + 写前日志自愈)
  validate/    run(多进程校验器), oracles(独立期望值)
fixtures/      topology / plan / configs / requests / faults
docs/          OPERATIONS.md（运维与故障复现）
```
