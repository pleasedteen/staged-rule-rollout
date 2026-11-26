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

# 全部执行服务
for s in ingest fx-math fx-tag fx-final \
         search-index search-a search-b search-final \
         report-aggregate report-format report-final \
         archive-load archive-store; do
  node dist/service/main.js --data-dir $DATA --topology $TOP --name $s &
done
```

停止：对任意进程发 `SIGTERM`（优雅）或 `SIGKILL`（模拟崩溃）。三者互不依赖，可任意顺序启停。
入口与协调器在协调器缺席时仍各自运行（入口保持最后已提交的绑定继续受理）。

观察：

```bash
curl -s $C/health
curl -s $C/admin/state | jq
curl -s "$C/admin/state?detail=1" | jq '{status, batches, entries, services}'
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
kill -9 $(pgrep -f 'service/main.js.*--name archive-store')
curl -s -X POST $C/ops/advance | jq .          # 第 2 批：ok
curl -s -X POST $C/ops/advance | jq '{code,blocked}'   # 第 3 批：blocked，只点名 archive-*
# 恢复后即可推进第 3 批
node dist/service/main.js --data-dir $DATA --topology $TOP --name archive-store &
curl -s -X POST $C/ops/advance | jq .
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

### 3.5 执行服务重启后自愈（service-restart-recovery）

```bash
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":123}' | jq -r .requestId)
sleep 0.1
kill -9 $(pgrep -f 'service/main.js.*--name fx-tag')
sleep 0.15
node dist/service/main.js --data-dir $DATA --topology $TOP --name fx-tag &
# 请求在服务重启后仍以钉住版本完成：
curl -s $EA/requests/$RID | jq '{version,status,output,segments:[.segments[]|{service,version,recovered}]}'
```

服务日志会打印 `healed journaled units after restart`。

### 3.6 入口崩溃后跨入口重试（entry-crash-cross-entry-retry）

```bash
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"currency","input":333}' | jq -r .requestId)
kill -9 $(pgrep -f 'entry/main.js.*--name entry-a')
# 在另一个入口从共享账本恢复，沿用钉住版本：
curl -s -X POST $EC/retry -H 'content-type: application/json' -d "{\"requestId\":\"$RID\"}" | jq '{ok,version:.record.version,output:.record.output,retries:.record.retries}'
```

### 3.7 分支长停机、恢复前切版与回执丢失（gateway 组）

gateway 组复用 currency 的执行服务，但 v2 路由去掉了 fx-math/fx-tag 分支，因此第 4 批
可以在分支服务停机时切换。前置条件：第 1–3 批已推进（见第 2 节），gateway 仍在 v1、
第 4 批 pending。入口重试窗口默认 15s，手工复现时建议调短：

```bash
export RULES_ENTRY_RETRY_WINDOW_MS=2500   # 对入口进程生效后再启动它们

# (a) 停掉 v1 gateway 路由的分支服务 fx-tag，受理一个钉住 v1 的请求
kill -9 $(pgrep -f 'service/main.js.*--name fx-tag')
RID=$(curl -s -X POST $EA/accept -H 'content-type: application/json' \
      -d '{"group":"gateway","input":7}' | jq -r .requestId)
# 超过重试窗口后请求转入 waiting 等待恢复（而不是失败）；
# ingest / fx-math 两段已确认落账：
curl -s $EA/requests/$RID | jq '{status,segments:[.segments[]|{service,version}]}'

# (b) 永久规则错误不参与等待，立即终止并报告：
curl -s -X POST $EA/submit -H 'content-type: application/json' \
     -d '{"group":"gateway","input":"not-a-number"}' | jq '{status:.record.status,code:.record.errorCode}'

# (c) 原入口重启：接手进程以同一请求身份自动续跑（仍 waiting）
kill -9 $(pgrep -f 'entry/main.js.*--name entry-a')
node dist/entry/main.js --data-dir $DATA --topology $TOP --name entry-a &
curl -s $EA/requests/$RID | jq '{status,retries,resumedBy}'

# (d) 恢复前切换组绑定：第 4 批的 v2 路由不需要 fx-tag，推进成功
curl -s -X POST $C/ops/advance | jq '{ok,batchIndex}'
curl -s -X POST $EC/submit -H 'content-type: application/json' \
     -d '{"group":"gateway","input":9}' | jq '{version:.record.version,output:.record.output}'

# (e) 停掉“已确认分支”的服务 fx-math，再恢复 fx-tag：
#     汇合直接使用已确认的 fx-math 结果，不再调用 fx-math
kill -9 $(pgrep -f 'service/main.js.*--name fx-math')
node dist/service/main.js --data-dir $DATA --topology $TOP --name fx-tag &
curl -s $EA/requests/$RID | jq '{status,output,segments:[.segments[]|{service,version,instance}]}'

# (f) 回执丢失：fx-final 落盘后故意丢一张回执，入口重试命中日志而不重算
curl -s -X POST http://127.0.0.1:7413/admin/fault -H 'content-type: application/json' -d '{"loseReceipts":1}'
RID2=$(curl -s -X POST $EC/accept -H 'content-type: application/json' \
       -d '{"group":"gateway","input":21}' | jq -r .requestId)
curl -s $EC/requests/$RID2 | jq '{status,output}'
curl -s http://127.0.0.1:7413/admin/journal | jq '.computes.byKey'   # 该请求键的实算次数 == 1
```

---

## 4. 观察与排错

- 批次与绑定：`GET /admin/state`、`GET /admin/state?detail=1`
- 请求各段版本与结果：`GET <entry>/requests/:id`
  （状态机：`accepted → running → done/failed`；分支服务长停机时为 `waiting`，恢复后自动续跑）
- 入口/服务实时能力：`GET /admin/entries`、`GET /admin/services`
- 服务实算去重：`GET <service>/admin/journal` 的 `computes.byKey`（每键实算次数）
- 配置传输计量：`GET /admin/transport`
- 日志：每个进程打印到 stdout，带时间戳与角色前缀（如 `[entry-a]`）。

常见返回码：

| code | 含义 |
|------|------|
| `paused` | 暂停中，不能推进 |
| `blocked` | 本批所需服务不可用/不能执行目标版本（`blocked` 列表点名） |
| `rollback_pending` | 有回退待确认 |
| `config_unavailable` | 入口拿不到钉住版本的配置（协调器与本地缓存都没有） |
| `version_not_installed` | 执行服务没有该版本规则 |
| `wrong_input_type` | 规则类型与输入不符（版本混用的典型症状） |

---

## 5. 存储选择

集群库（协调器 + 入口共享）固定使用锁文件 + 原子 rename 的 JSON 引擎；本地库每进程私有，
使用 SQLite（单写者）。原因：容器 / 网络 / FUSE 文件系统对 SQLite 的多进程锁不可靠。

```bash
RULES_STORE=file    node dist/coordinator/main.js ...   # 本地库也退回 JSON 文件
RULES_STORE=sqlite  node dist/coordinator/main.js ...   # 无 node:sqlite 时直接报错
```

清空一次运行的全部状态：`rm -rf .run`（`npm run validate` 每次都会自动清空）。

> 如果上一轮进程没有退出，固定端口会被占用。校验器会检测到并提示：
> `pkill -f 'main\.js --data-dir'` 后再运行。
