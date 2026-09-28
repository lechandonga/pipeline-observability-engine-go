# Pipeline Observability Engine（本地数据管道可观测性与告警状态系统）

一个**仅依赖 Go 标准库**的本地引擎，对管道运行事件（started / retried / failed /
succeeded）与指标（新鲜度 / 延迟 / 吞吐 / 失败重试 / SLA）做上报、状态归并和告警判定。
核心目标：在**乱序、重复上报、迟到、并发访问、重启恢复**下，任何时刻得到的结论都
一致、可复现、可解释。

- 模块路径：`github.com/lechandonga/pipeline-observability-engine-go`
- Go 版本：见 `go.mod`（本仓库在 Go 1.25+ 开发，CI 仅用标准库，无第三方依赖）

---

## 1. 架构与包结构

```
pkg/model    事件 / 指标 / 运行态 / 告警 / 拒绝原因等核心类型
pkg/clock    可注入时钟（SystemClock / FakeClock），测试确定性、无需 sleep
pkg/window   按 pipeline 独立的事件时间水位（watermark）、迟到/未来判定
pkg/dedup    有界 FIFO 去重集合（map + 双向链表）
pkg/store    追加写 WAL：每帧 fsync、CRC 校验、崩溃尾部截断、checkpoint 压缩
pkg/alert    版本化规则注册表 + 每 pipeline 告警剧集（episode）状态机
pkg/engine   单把 RWMutex 串起校验/去重/窗口/归并/告警/持久化/恢复/查询
cmd/poe-demo 可运行的端到端演示
```

归并流水线（单条上报）：

```
合法性校验 → 取写锁 → 去重预订 → 窗口判定 → 状态冲突预检（纯函数）
        → 先 fsync 落 WAL（成功并拿到 seq）→ 再改内存状态
        → 推进水位 → 评估告警 → 有界维护（GC/Sweep）→ 释放锁并应答
```

关键不变量：

1. **先持久化后改内存**：只有 WAL 追加（含 `fsync`）成功后才推进内存状态，
   所以“已确认（accepted）”的上报在崩溃/重启后一定还在；反之未落盘的数据
   绝不污染状态。
2. **拒绝不落盘、不留痕**：重复、迟到、超未来、非法、状态冲突的上报不会写入
   WAL，也会回滚它在去重集合中的临时占位，因此被拒 ID 可被后续合法上报复用，
   已成立的状态绝不被改写。
3. **单把读写锁**：所有上报持写锁，查询持读锁并做**深拷贝快照**；查询任一时
   刻只看到一致状态。读路径（`Snapshot`）是纯函数，不在持读锁时修改任何 map。

---

## 2. 时间窗口（乱序 / 迟到 / 超未来）

- 每个 pipeline 维护**独立**水位 `watermark = 已接受的最大事件时间戳`。一个
  pipeline 的迟到事件不影响另一个 pipeline。
- 设迟到容忍 `LateTolerance = L`，未来容忍 `FutureTolerance = F`，当前墙上时间为 `now`：
  - 可接受区间为 **[watermark − L, now + F]**（**左闭右闭**：恰好在
    `watermark − L` 边界上的事件仍接受，用半开区间 `ts.Before(low)` 判定）。
  - `ts < watermark − L` → 拒绝，原因 **`window_expired`（迟到/超出窗口）**。
  - `ts > now + F` → 拒绝，原因 **`future_too_far`（时间戳远超当前时间）**。
  - 其余乱序事件（早于水位但仍在窗口内）**正常按事件时间归并**，结果稳定。
- 只有**被接受**的上报才会 `Observe` 推进水位，因此一个伪造的超未来/迟到时间
  戳永远无法把水位推走、也无法让后续正常事件被误判。
- 第一次上报没有水位约束（只要不超未来）。

> 设计提示：`FutureTolerance` 很重要。没有它，单个巨大时间戳会把水位推到很
> 远的未来，从而静默吞掉之后所有真实上报。

---

## 3. 去重语义

- 每条上报（事件或指标）必须带全局唯一 `ID`。
- 去重集合是**有界 FIFO**：超过 `DedupCapacity` 时淘汰最旧 ID。容量应配置得
  明显大于“迟到窗口内可能到达的上报量”。被淘汰后同一 ID 再次到达会被视为新
  上报（这是有界内存的明确取舍）。
- 重复上报**只生效一次**：第二次起返回 **`duplicate`**，不重复累计（如 retry
  次数、告警计数、吞吐等都不会增加）。
- 去重状态本身随 checkpoint 持久化，**重启后仍然认得旧 ID**。

---

## 4. 运行态归并与“状态冲突”

单个 run 的生命周期归并（按事件时间，允许乱序）：

- `started`：run 不存在 → 进入 `running`；已存在 → **`state_conflict`**。
- `retried`：必须先有 `started`；`running`/`failed` 下合法（`Attempts++`，并
  按 run 去重计入“重试 run 数”）；对 `succeeded` 的 run 重试 → 冲突。
- `failed`：必须先有 `started`；`succeeded` 之后失败 → 冲突；重复终态失败 →
  冲突；合法则进入 `failed`，并计入“失败 run 数”。
- `succeeded`：必须先有 `started`；重复成功 → 冲突；合法则进入 `succeeded`，
  `EndedAt` 取较大时间戳，并把该 run 从“失败 run 数”中移除（最终成功不算失败）。

冲突返回 **`state_conflict`**，且与非法输入 **`invalid_data`**（空 ID / 空
pipeline / 空 run / 零时间戳 / 未知类型 / 负值 / SLA∉[0,1] 等）严格区分。

可区分的拒绝原因（`model.RejectReason`）：

| 原因 | 含义 |
| --- | --- |
| `duplicate` | 该上报 ID 已被接受过，重复上报不生效 |
| `window_expired` | 事件时间早于 `watermark − LateTolerance` |
| `future_too_far` | 事件时间晚于 `now + FutureTolerance` |
| `invalid_data` | 字段缺失/非法/未知枚举/数值越界 |
| `state_conflict` | 与已成立的 run 生命周期状态冲突 |

---

## 5. 指标与告警规则

### 5.1 指标族（`model.MetricKind`）

| 指标 | 取值 | “坏”的方向 |
| --- | --- | --- |
| `freshness`（新鲜度） | 距最新数据的秒数 | 越大越坏（`Above=true`） |
| `latency`（延迟） | 秒 | 越大越坏 |
| `throughput`（吞吐） | records/minute | 越小越坏（`Above=false`） |
| `failure_retry`（失败重试） | 由事件派生的失败 run 计数 | 越大越坏（阈值一般取 `1`，即 ≥1 触发） |
| `sla` | `[0,1]`，1 为合规 | 越小越坏 |

失败/重试计数按 **run 去重**，因此同一 run 的重复/多次事件不会重复累计；run
最终成功后其失败计数清零（清零本身是一次“恢复证据”，会立即恢复失败类告警，
而不是干等超时）。

### 5.2 规则（`alert.ThresholdRule`）

- `Threshold`：触发边界（`Above=true` 时 `v >= Threshold` 触发；`Above=false`
  时 `v <= Threshold` 触发）。
- `RecoverBelow`：迟滞（hysteresis）恢复边界，避免边界抖动反复告警。
  - `Above=true`：已触发后只有 `v <= RecoverBelow` 才恢复（要求 `RecoverBelow < Threshold`）。
  - `Above=false`：已触发后只有 `v >= RecoverBelow` 才恢复（要求 `RecoverBelow > Threshold`）。
  - 不配置则一跨过触发阈值的反向边界即恢复。
- `For`：违规需持续该时长（按事件时间）才真正 fire；持续期内恢复则该待定
  剧集静默消散，不产生告警。
- `RecoveryWindow`：活动告警若超过该时长没有新的违规证据，则按
  `LastBad + RecoveryWindow` **虚拟地投影为已恢复**。
- `CloseAfter`：恢复后经过该时长投影为已关闭。
- `Pipeline`：精确匹配；留空表示对**所有** pipeline 生效（各自独立判定）。

### 5.3 告警生命周期与去重

每个 `(ruleID, pipeline)` 是一条逻辑告警，状态为：

```
active（触发） → recovered（恢复） → closed（关闭）
   ↑____________flap 重开_________|   （关闭后再次违规 = 新剧集）
```

- 同一条违规证据重复到达**只产生一次** active（剧集内 `Count` 自增，不新增告警）。
- recovered 后、closed 前再次违规 → **重开同一剧集**，不产生重复告警。
- closed 之后再违规 → 全新剧集（新结论）。

### 5.4 配置变更不改写历史（版本钉住）

- 规则集是**版本化**的，每次 `UpdateRules` 原子替换并 `version++`，完整保留
  历史版本。
- 一个告警剧集在**创建时钉住 `(ruleID, ruleVersion)` 与其规则快照**；之后无论
  阈值怎么改，该剧集都按它创建时的规则（含迟滞/恢复/关闭时长）继续演进。
  新版本下的违规会产生一个独立的、按版本区分的新剧集，**绝不覆盖旧结论**。
- 非法的新规则集会被**整体原子拒绝**（校验失败时版本与状态都不变）。

### 5.5 投影 vs 落盘

剧集里**落盘的是带时间戳的事实**（首次违规 / 最后坏证据 / 最后好证据 /
fire / 恢复 / 关闭）。`RecoveryWindow`、`CloseAfter` 这类“随时间到达”的恢复/
关闭，在查询时做**纯虚拟投影**，因此重启前后对同一事实的结论完全一致。真正
的物理清理（GC）只在写路径（`Manager.Sweep`）进行。

---

## 6. 持久化与重启恢复

### 6.1 WAL 帧格式（大端）

```
magic(4B "POE1")
[ seq uint64 ][ kind 1B ][ crc32 uint32 ][ length uint32 ][ payload ... ]
```

- `kind`：`E`=事件(JSON)、`M`=指标(JSON)、`R`=规则快照、`S`=引擎 checkpoint。
- 每帧对 `kind+payload` 计算 CRC32-IEEE；**每次 Append 都 `fsync`**。
- `seq` 从 1 开始、单调递增、无空洞。

### 6.2 崩溃一致性

- **进程中途被杀死（写了半帧）**：重启 `Replay` 时识别到不完整的最后一帧
  （CRC 配合长度读取），忽略并把文件**截断回最后一个完整帧**，历史帧不受影响。
- **未调用 Close（模拟崩溃）**：没有 checkpoint，引擎通过**完整重放**所有帧
  确定性重建状态（见 `TestRecoverFromPlainWALWithoutCheckpoint`）。

### 6.3 Checkpoint 压缩（恢复不随历史无限变慢）

- 当距上次压缩累计帧数达到 `CompactionThreshold`，或在 `Close` 时，引擎把全
  量派生状态（规则历史、各 pipeline 状态、水位、去重 ID FIFO、告警剧集、
  最新 seq）写成一个 `S` 帧，并原子重写 WAL（`tmp + fsync + rename + fsync
  目录`）。
- 恢复时：读取**最后一个 checkpoint** 作为基线，再重放其后的尾部帧。因此
  重放量被压缩点限制，**查询与恢复成本只取决于保留的状态量，不随总历史线性
  增长**；运行态还有 `RunRetention` 做有界清理。

---

## 7. 并发模型

- 一把 `sync.RWMutex` 守护全部派生状态：上报写锁、查询读锁。
- 读路径只做深拷贝投影，**绝不在读锁内写 map**（曾通过压测修复了“查询即
  清理”的隐患，物理 GC 移到写路径 `Sweep`）。
- 压测（`TestConcurrentIngestAndQueryNoDeadlockOrRace`，8 writer × 300 +
  重复重放 + 4 个持续查询）在 `-race` 下验证：
  - 无数据竞争、无死锁（读者每次查询间 `yield`，不饿死写者）；
  - 每个唯一上报 ID 恰好 1 次接受、其余为 `duplicate`，**不重复累计、不丢告警**；
  - 数千次违规只产生 1 条去重后的 active 告警。

---

## 8. 配置项（`engine.Config`）

| 字段 | 默认值 | 说明 |
| --- | --- | --- |
| `StorePath` | 必填（演示用 `./demo-data/wal`） | WAL 路径；**留空 = 纯内存**（不支持重启恢复） |
| `LateTolerance` | `5m` | 迟到容忍；事件时间早于 `watermark−L` 拒绝 |
| `FutureTolerance` | `30s` | 未来容忍；事件时间晚于 `now+F` 拒绝 |
| `DedupCapacity` | `100000` | 去重集合容量（FIFO 淘汰） |
| `KeepClosedFor` | `24h` | 已关闭告警在查询中保留多久后被 GC |
| `RunRetention` | `30d` | 已结束 run（含计数）按事件时间的保留期；运行中的 run 不清理 |
| `CompactionThreshold` | `512` | 距上次压缩累计多少帧后做一次 checkpoint 重写；`0`=只在 Close 压缩 |

时钟通过 `engine.New(cfg, clk)` 注入；生产传 `clock.NewSystemClock()`，测试传
`clock.NewFakeClock(t)`。

---

## 9. 本地验证方法

仅依赖本机与本仓库（无外部服务、无第三方 Go 依赖）。

```bash
# 1) 全部单元测试
go test ./...

# 2) 带竞态检测（需要本机有 C 工具链；CGO_ENABLED=1）
CGO_ENABLED=1 go test ./... -race

# 3) 详细日志（关键用例会打印“输入事件 → 判定结果 → 原因”）
go test ./pkg/engine/ -v
go test ./pkg/alert/  -v

# 4) 端到端演示（乱序/重复/非法/冲突/迟到/告警 fire+recover/重启恢复）
go run ./cmd/poe-demo -dir ./demo-data
# 查看落盘（Close 后通常只剩 magic + 一个 checkpoint 帧）
xxd ./demo-data/wal | head
```

### 9.1 测试覆盖矩阵

| 需求 | 测试 |
| --- | --- |
| 乱序归并 | `TestOutOfOrderMerge` |
| 重复只生效一次 / 拒绝回滚不占 ID | `TestDuplicateAppliesOnce`、并发用例的重复计数 |
| 迟到 / 超未来 / 边界 | `TestLateAndFutureRejected`、`pkg/window` 全部用例 |
| 非法输入 | `TestInvalidInput` |
| 状态冲突 | `TestRunStateTransitionsAndConflicts` |
| 每 pipeline 独立水位 | `TestWatermarksPerPipeline` |
| 告警 fire/dedup/For/迟滞/抖动/超时恢复关闭 | `pkg/alert/manager_test.go` |
| 新鲜度/延迟/吞吐/失败重试/SLA 端到端 | `engine_alert_test.go`、`engine_metrics_test.go` |
| 配置变更不改写历史 + 原子拒绝坏配置 | `TestConfigChangeDoesNotRewriteHistory`、`TestRuleVersionPinning`、`TestRestartPreservesRuleVersionPinning` |
| 重启恢复 / 无 checkpoint 崩溃恢复 / 压缩 | `TestRestartRecovery`、`TestRecoverFromPlainWALWithoutCheckpoint`、`pkg/store/*_test.go` |
| 并发读写不丢/不重/不卡死 | `TestConcurrentIngestAndQueryNoDeadlockOrRace`（`-race`） |
| 查询成本有界（不随历史变慢） | `TestQueryCostBoundedByRetentionNotHistory` |
| 半帧（torn tail）截断 | `TestTornTailTruncated` |

关键单测通过 `t.Logf` 打印**输入、accepted/rejected、seq、拒绝原因与细节**；
失败时可据 `window_expired` / `duplicate` / `invalid_data` / `state_conflict` /
`future_too_far` 直接区分失败类别。

---

## 10. 兼容性与恢复语义（速查）

- **WAL 前 4 字节 magic = `POE1`**；magic 不符会拒绝打开，避免误读其它文件。
- 帧均带 CRC，损坏（非尾部撕裂）返回 `store.ErrCorrupt`，不会静默错算。
- 新增字段以 JSON 承载，旧 checkpoint 缺失的新字段按零值/默认配置解释；
  本项目尚不承诺跨大版本的文件迁移，跨版本请先在测试环境重放验证。
- 重启后：已确认的运行态、指标最新值、去重 ID、水位、规则历史、告警剧集及
  其钉住版本、最新 seq 全部恢复；随后继续写入时 seq 单调递增、无空洞。
