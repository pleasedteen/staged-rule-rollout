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

每次推进都会更新 `bindings.epoch`；同批所有组共享该 epoch。切换为严格两阶段：协调器先让
**所有可达入口** prepare（暂存 + 预热，不翻转），全部成功后才并行 commit，因此不存在入口
先后切版本的中间态。若在 prepare 扇出期间暂停，切换会被中止，已暂存的 epoch 通过入口的
`POST /admin/abort {epoch}` 丢弃，迟到的 prepare/commit 不会触发任何翻转。

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

接手进程依据**持久证据**对账：入口每次 commit 前都会把确认写入共享集群库
（`binding-ack:<epoch>`）。因此即使此刻所有入口都探测不到，接手者也能凭 ack 查准批次状态：
全部确认则采纳，确证无任一落地则丢弃，证据不足则**保留** `pending`（`retained=true`，
可在 `/admin/state/raw` 查看）等下次恢复，绝不靠猜测误判。

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

有入口离线时确认回退同样成功（切换只扇出到可达入口）。若该离线入口在**协调器停机期间**
才恢复，它会直接从共享集群库读到回退后的权威绑定（不依赖协调器 HTTP），首条新请求即使用
回退版本；协调器重启后再把掉队入口追平。已受理请求的后续执行与结果始终沿用首次受理版本。

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
| `paused` | 暂停中，不能推进；或切换在 prepare 阶段被暂停中止 |
| `blocked` | 本批所需服务不可用/不能执行目标版本（`blocked` 列表点名） |
| `switch_pending` | 上一次切换仍在对账（如入口暂不可达），入口恢复后重试 |
| `not_prepared` | 入口收到没有对应 prepare 的 commit（迟到结果，拒绝翻转） |
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
