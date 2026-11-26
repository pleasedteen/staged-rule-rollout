# 运维与故障复现手册

面向运维同学：如何启动、观察、发布、暂停、回退，以及如何手工复现 `fixtures/faults.json`
中的每个故障场景。所有命令假设已在仓库根目录执行 `npm install && npm run build`。

约定：

```bash
C=http://127.0.0.1:7400          # 协调器
EA=http://127.0.0.1:7401         # entry-a
EB=http://127.0.0.1:7402         # entry-b
EC=http://127.0.0.1:7403         # entry-c
DATA=.run
TOP=fixtures/topology.json
```

---

## 1. 启动与停止

```bash
# 协调器（首次启动会安装 fixtures/plan.json 的计划与配置）
node dist/coordinator/main.js --data-dir $DATA --topology $TOP --plan fixtures/plan.json &

# 三个入口
for e in entry-a entry-b entry-c; do
  node dist/entry/main.js --data-dir $DATA --topology $TOP --name $e &
done

# 全部执行服务（每个服务可启动多个实例；单实例时 --instance 可省略）
for s in ingest fx-math fx-tag fx-final \
         search-index search-a search-b search-final \
         report-aggregate report-format report-final \
         archive-load archive-store; do
  node dist/service/main.js --data-dir $DATA --topology $TOP --name $s &
done

# 多实例服务：同一服务名再起一个实例，端口在 topology.json 的 instances 里声明
node dist/service/main.js --data-dir $DATA --topology $TOP --name fx-tag --instance fx-tag-2 &
```

停止：对**某个实例**发 `SIGTERM`（优雅）或 `SIGKILL`（模拟崩溃）只会停掉该实例；同一服务的
其它实例继续服务，在途请求会自动换到兄弟实例。三者（协调器/入口/服务）互不依赖，可任意顺序启停。
入口与协调器在协调器缺席时仍各自运行（入口保持最后已提交的绑定继续受理）。

观察：

```bash
curl -s $C/health
curl -s $C/admin/state | jq
curl -s "$C/admin/state?detail=1" | jq '{status, batches, entries, services}'
# 每个服务下 instances[] 给出每个实例的可达性与版本
```

---

## 2. 发布流程

```bash
# 推进第 1 批（currency → v2）
curl -s -X POST $C/ops/advance | jq '{ok,code,batchIndex,blocked}'

# 暂停（已生效批次保持原状）
curl -s -X POST $C/ops/pause | jq .status

# 暂停期间推进会被拒绝（409 paused）
curl -s -X POST $C/ops/advance | jq '{code}'

# 显式继续
curl -s -X POST $C/ops/resume | jq .status

# 继续推进第 2 批（search+report 同批）、第 3 批（archive）
curl -s -X POST $C/ops/advance | jq .
curl -s -X POST $C/ops/advance | jq .
```

每次推进都会更新 `bindings.epoch`；同批所有组共享该 epoch。

---

## 3. 故障复现

### 3.1 后续批专属服务停机不影响当前批（later-batch-service-down）

`archive-*` 只被第 3 批需要。停机后第 2 批仍可推进：

```bash
kill -9 $(pgrep -f 'service/main.js.*--instance archive-store-1')
curl -s -X POST $C/ops/advance | jq .          # 第 2 批：ok
curl -s -X POST $C/ops/advance | jq '{code,blocked}'   # 第 3 批：blocked，只点名 archive-*
# 恢复后即可推进第 3 批
node dist/service/main.js --data-dir $DATA --topology $TOP --name archive-store &
curl -s -X POST $C/ops/advance | jq .
```

### 3.1b 一个实例停机不阻塞本批（instance-level readiness）

`fx-math` 有两个实例。停掉一个，第 1 批照常推进（另一个实例能执行目标版本）；只有两个都停掉
才会被阻塞，并在 `blocked` 里点名 `fx-math`：

```bash
kill -9 $(pgrep -f 'service/main.js.*--instance fx-math-1')
curl -s "$C/admin/state?detail=1" | jq '.services[]|select(.name=="fx-math").instances'
curl -s -X POST $C/ops/advance | jq '{ok,blocked}'      # ok:true, blocked:[]
```

### 3.2 协调器崩溃（coordinator-crash）

```bash
kill -9 $(pgrep -f 'coordinator/main.js')
# 入口仍按实际生效版本受理请求：
curl -s -X POST $EB/submit -H 'content-type: application/json' \
     -d '{"group":"report","input":"offline"}' | jq '{version:.record.version,output:.record.output,status:.record.status}'
# 重启接手进程，它会查询各入口实际进度后继续
node dist/coordinator/main.js --data-dir $DATA --topology $TOP --plan fixtures/plan.json &
curl -s "$C/admin/state?detail=1" | jq '{recovered,recoveries,lastRecovery,batches}'
```

### 3.3 协调器在切换中途崩溃（coordinator-crash-mid-switch）

两种崩溃点，语义不同：

```bash
# (a) 已写持久意图、尚未触碰入口 → 接手进程应丢弃，批次回到 pending
curl -s -X POST $C/ops/advance -H 'content-type: application/json' -d '{"fault":"after-pending"}'
node dist/coordinator/main.js --data-dir $DATA --topology $TOP --plan fixtures/plan.json &
curl -s $C/admin/state | jq '{bindings:.bindings.versions, batches:[.batches[]|{name,status}]}'

# (b) 入口已全部 commit、协调器尚未记账 → 接手进程应采纳为 effective
curl -s -X POST $C/ops/advance -H 'content-type: application/json' -d '{"fault":"after-commit"}'
node dist/coordinator/main.js --data-dir $DATA --topology $TOP --plan fixtures/plan.json &
curl -s "$C/admin/state?detail=1" | jq '{bindings:.bindings, batches:[.batches[]|{name,status}]}'
```

### 3.4 回退期间仍有请求在执行（rollback-with-inflight）

`fx-tag` 有 500ms 延迟，制造“执行中”窗口：

```bash
# 受理一个 currency 请求（会进入 fx-tag 的延迟段）
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":21}' | jq -r .requestId)
sleep 0.1
curl -s -X POST $C/ops/pause >/dev/null
curl -s -X POST $C/ops/rollback -H 'content-type: application/json' -d '{"batchIndex":0}' | jq .ok
curl -s -X POST $C/ops/rollback/confirm | jq '{ok,inflightAtConfirm:.state.rollback.inflightAtConfirm}'
# 在途请求仍以钉住的 v2 完成：
curl -s $EA/requests/$RID | jq '{version,status,output}'
# 新受理请求使用回退后的 v1：
curl -s -X POST $EA/submit -H 'content-type: application/json' \
     -d '{"group":"currency","input":4}' | jq '{version:.record.version,output:.record.output}'
```

### 3.5 执行服务实例重启后自愈（service-restart-recovery）

```bash
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":123}' | jq -r .requestId)
sleep 0.1
kill -9 $(pgrep -f 'service/main.js.*--instance fx-tag-1')
sleep 0.15
node dist/service/main.js --data-dir $DATA --topology $TOP --name fx-tag --instance fx-tag-1 &
# 请求在实例重启（或换到 fx-tag-2）后仍以钉住版本完成：
curl -s $EA/requests/$RID | jq '{version,status,output,segments:[.segments[]|{service,instance,version,recovered,reused,originInstance,failovers}]}'
```

服务日志会打印 `healed journaled units after restart`。

### 3.5b 在途请求遇实例停机自动换实例（instance-failover）

同一个服务有多个实例时，入口成功一次后会粘在该实例；该实例停机后，同一请求的后续尝试立即
换到兄弟实例（不等待原实例恢复），钉住版本与 slot 不变：

```bash
# 先跑一个请求，确认入口粘在哪个 fx-tag 实例
curl -s -X POST $EA/submit -H 'content-type: application/json' \
     -d '{"group":"currency","input":90}' | jq '.record.segments[]|select(.service=="fx-tag")|.instance'
# 发起若干长请求（fx-tag 有 500ms 延迟），趁其在途停掉当前实例
for i in 1 2 3; do curl -s -X POST $EA/accept -d "{\"group\":\"currency\",\"input\":$i}" -H 'content-type: application/json' >/dev/null; done
kill -9 $(pgrep -f 'service/main.js.*--instance fx-tag-1')
# 各请求仍在 v2 上完成，fx-tag 段显示 recovered=true、instance=fx-tag-2#…
curl -s $EA/requests/<requestId> | jq '.segments[]|select(.service=="fx-tag")'
```

### 3.5c 已落盘但丢回执 → 兄弟实例复用原结果（lost-receipt-reuse）

服务先写日志再执行。让当前实例在“写完结果、回执未发出”的瞬间退出，重试落到兄弟实例，后者命中
日志直接返回原结果（`reused=true`），随后仍跑完汇合与后续串行段：

```bash
# 用故障注入武装当前 fx-tag 实例
curl -s -X POST http://127.0.0.1:7412/admin/fault -H 'content-type: application/json' \
     -d '{"mode":"crash-after-journal"}'
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":711}' | jq -r .requestId)
sleep 1
# fx-tag 段 instance 为兄弟实例、reused=true、originInstance 指向已退出实例；
# output 仍等于钉住 v2 的 oracle，其它段（fx-math/fx-final）也都在 v2
curl -s $EA/requests/$RID \
  | jq '{version,status,output,segments:[.segments[]|{service,instance,version,reused,originInstance,failovers,output}]}'
node dist/service/main.js --data-dir $DATA --topology $TOP --name fx-tag --instance fx-tag-1 &
```

### 3.6 入口崩溃后跨入口重试（entry-crash-cross-entry-retry）

```bash
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":333}' | jq -r .requestId)
kill -9 $(pgrep -f 'entry/main.js.*--name entry-a')
# 在另一个入口从共享账本恢复，沿用钉住版本：
curl -s -X POST $EC/retry -H 'content-type: application/json' -d "{\"requestId\":\"$RID\"}" | jq '{ok,version:.record.version,output:.record.output,retries:.record.retries}'
```

---

## 4. 观察与排错

- 批次与绑定：`GET /admin/state`、`GET /admin/state?detail=1`
- 请求各段版本与结果：`GET <entry>/requests/:id`
- 入口/服务实时能力：`GET /admin/entries`、`GET /admin/services`
- 配置传输计量：`GET /admin/transport`
- 日志：每个进程打印到 stdout，带时间戳与角色前缀（如 `[entry-a]`）。

常见返回码：

| code | 含义 |
|------|------|
| `paused` | 暂停中，不能推进 |
| `blocked` | 本批所需服务**所有实例**都不可用/不能执行目标版本（`blocked` 列表点名服务） |
| `rollback_pending` | 有回退待确认 |
| `config_unavailable` | 入口拿不到钉住版本的配置（协调器与本地缓存都没有） |
| `version_not_installed` | 执行服务实例没有该版本规则 |
| `wrong_input_type` | 规则类型与输入不符（版本混用的典型症状） |
| `no_instances` | 拓扑里该服务一个实例都没有 |

---

## 5. 存储选择

集群库（协调器 + 入口共享）与服务日志（同一服务的全部实例共享）固定使用锁文件 + 原子 rename
的 JSON 引擎；本地库每实例私有，使用 SQLite（单写者）。原因：容器 / 网络 / FUSE 文件系统对
SQLite 的多进程锁不可靠。服务日志必须共享，兄弟实例才能复用“已落盘但丢回执”的结果。

```bash
RULES_STORE=file    node dist/coordinator/main.js ...   # 本地库也退回 JSON 文件
RULES_STORE=sqlite  node dist/coordinator/main.js ...   # 无 node:sqlite 时直接报错
```

清空一次运行的全部状态：`rm -rf .run`（`npm run validate` 每次都会自动清空 `.run` 与
`.run-single`）。

> 如果上一轮进程没有退出，固定端口会被占用。校验器会检测到并提示：
> `pkill -f 'main\.js --data-dir'` 后再运行。
