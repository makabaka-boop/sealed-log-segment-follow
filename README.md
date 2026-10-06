# logfollow — 分段 NDJSON 日志跟随服务（SSE）

生产者把日志依次写成 `segment-0001.ndjson`、`segment-0002.ndjson` …：开启下一段后旧段**封存不再修改**，只有序号最高的一段允许追加。
本服务以**只读**方式跟随这些分段，通过 Server-Sent Events 把一条条完整记录推给排障客户端，并支持断线续读——绝不会把上一段尾和下一段首拼成一条记录。

## 磁盘布局契约

挂载到容器 `/logs`（只读）的目录：

```
logs/
├── runtime.id             # 生产者“服务运行代号”，单行文本
├── segment-0001.ndjson    # 封存后不再改动
├── segment-0002.ndjson
└── ...                    # 最多 6 段，每段最多 8 KiB
```

约束：

- 最多 `MaxSegments = 6` 个分段，每段最多 `MaxSegmentSize = 8 KiB`。
- 只有**序号最高**的段可追加；旧段封存。
- 文件名严格为 `segment-NNNN.ndjson`（四位数字）；其他文件忽略。
- **不**支持复制-截断（copy-truncate）式轮转：同名文件被原地缩小或被 rename 覆盖，都会被显式判为异常并终止该订阅，而不是静默跟随。
- 记录是 NDJSON：一条完整记录 = 一行以 `\n` 结尾的字节。半行（尚无 `\n`）继续等待，不投递。

## 运行

```bash
# 直接运行
go run ./cmd/logfollow -dir ./logs -addr :8080

# 容器（docker-compose 已把 ./logs 只读挂载到 /logs）
docker compose up --build
```

订阅：

```bash
curl -N http://localhost:8080/stream

# 断线续读：带上上一条记录的 SSE id
curl -N -H 'Last-Event-ID: v1:eyJ...' http://localhost:8080/stream
# 或等价地
curl -N 'http://localhost:8080/stream?cursor=v1:eyJ...'
```

健康检查：`GET /healthz` → `{"status":"ok"}`。

## SSE 事件

每条普通事件都带 `id`（不透明续读令牌）、`event` 类型与 `data`（JSON）。

### `event: record` — 解析成功的完整行

```
id: v1:<base64url>
event: record
data: {"run":"<运行代号>","segment":2,"offset":4026,"record":{ ...规范化后的 JSON... }}
```

- `segment`：分段号。
- `offset`：**该记录结束的字节偏移**（该记录换行符之后的位置，相对所属分段）。这就是续读起点。
- `record`：`json.Compact` 规范化后的对象。
- 偏移按**字节**计，绝不按字符数；多字节 UTF-8 被追加切开时，半行留在缓冲里直到收齐再解析。

### `event: bad_json` — 完整行但 JSON 非法（非致命）

```
event: bad_json
data: {"run":"...","segment":1,"offset":30,"line":"{\"oops\":","error":"...parse error..."}
```

坏 JSON 作为错误事件交付，**不会吞掉后续行**，跟随继续。

### `event: fatal` — 终止该订阅（连接随后关闭）

```
event: fatal
data: {"kind":"...","error":"...","cursor":{"run":"...","seg":2,"off":4026}}
```

`cursor` 是出错时最后已知的**可恢复**位置（上一条完整记录之后），仅供诊断。

## 续读（Last-Event-ID）

客户端把最后收到的 `id` 在重连时放进 `Last-Event-ID` 头（或 `?cursor=`）。服务端**先做预检**再开始推流：

1. 令牌格式可解析；
2. `run` 与当前 `runtime.id` 一致；
3. 游标分段仍存在；
4. 从游标分段到当前最高段，序号连续、无缺口；
5. 偏移不超过文件大小；
6. 偏移 0 合法；否则**直接读盘校验该偏移前一个字节必须是 `\n`**——游标只能是真实换行边界，不能由字符数推算。

预检失败返回 HTTP 状态码与机器可读原因，**绝不静默跳到当前段**：

| HTTP | reason | 含义 |
|---|---|---|
| 400 | `malformed_cursor` | 令牌无法解析/版本不符 |
| 409 | `run_mismatch` | 运行代号与当前生产者不同 |
| 409 | `segment_missing` | 游标指向的分段已被删除 |
| 409 | `segment_gap` | 游标段与最高段之间序号有缺口 |
| 409 | `not_line_boundary` | 偏移不是真实换行边界 |
| 409 | `offset_out_of_range` | 偏移超过该段当前大小 |

## 运行期致命原因（`fatal.kind`）

| kind | 触发条件 |
|---|---|
| `truncated` | 已封存段尾部没有换行：末尾字节是半行，证据不完整 → 报告截断并停止 |
| `segment_gap` | 跟随路径上出现分段序号缺口 |
| `segment_missing` | 正在跟随的段被删除且无连续后继 |
| `segment_replaced` | 同名文件被 rename 覆盖（inode/dev 变化）或原地缩小（copy-truncate） |
| `run_changed` | 跟随期间 `runtime.id` 变成另一个运行代号 |
| `slow_consumer` | 该连接的有界事件缓冲在宽限期内一直满 |
| `internal_error` | 目录/文件系统层面的意外错误 |

### 同名文件替换如何被发现

扫描目录取的是目录项本身的 `(dev, inode)`（对路径 `lstat`，**不重新 open**），跟随者持有的是打开描述符的 `(dev, inode)`。
rename 覆盖后目录项指向新 inode，而描述符仍是旧 inode，二者不一致即判定 `segment_replaced`——只看文件名会漏掉这种替换。原地 `truncate` 则通过“打开描述符尺寸缩小到读位置之后”检出。

## 慢读者与连接隔离

- 每个订阅拥有**独立的 goroutine 与有界缓冲**（默认 256 条，可经 `LOGFOLLOW_BUFFER` 调）。
- 缓冲满时，生产者为该订阅最多等待宽限期（`LOGFOLLOW_FULL_WAIT`，默认 2s）；仍满则以 `slow_consumer` 终止这一条订阅。
- 慢订阅只拖它自己，不阻塞其他订阅者；终止帧的写出也有期限（`LOGFOLLOW_FATAL_GRACE`），不泄漏 goroutine。
- 每条连接的取消相互独立（请求 `Context`）。

## 配置

| 环境变量 / flag | 默认 | 说明 |
|---|---|---|
| `LOGFOLLOW_ADDR` / `-addr` | `:8080` | 监听地址 |
| `LOGFOLLOW_DIR` / `-dir` | `/logs` | 日志目录（容器内只读挂载） |
| `LOGFOLLOW_POLL` / `-poll` | `30ms` | 目录轮询周期（新增/增长/替换/run 变化） |
| `LOGFOLLOW_BUFFER` / `-buffer` | `256` | 每连接事件缓冲上限 |
| `LOGFOLLOW_FULL_WAIT` / `-full-wait` | `2s` | 慢读者宽限 |
| `LOGFOLLOW_HEARTBEAT` / `-heartbeat` | `15s` | SSE 心跳注释帧周期 |
| `LOGFOLLOW_FATAL_GRACE` / `-fatal-grace` | `5s` | 终止帧写出期限 |

实现采用“只读打开 + 定时 `ReadDir` 扫描 + `ReadAt` 字节偏移”，不依赖 inotify（在 bind mount / 各类文件系统上更稳）。

## 测试

```bash
go test -race ./...
```

测试用**真实文件追加与切段**配合“读取屏障”（先观察到某段屏障记录才继续 roll），覆盖：

- 字节偏移精确（多字节 UTF-8，而非字符数）；半行不提前投递；坏 JSON 不吞后续；
- 段尾/段首不拼接；跨六段全程连续；**切开 UTF-8 多字节序列**；
- 断线用 `Last-Event-ID` 重连，不错放、不丢记录、可幂等重放；
- 删除历史段：运行期继续跟随当前段，但指向被删段的游标明确 409；
- 分段缺口、运行代号变化：预检 409 与运行期 `fatal`，都不静默跳转；
- 同名 rename 覆盖与原地缩小（copy-truncate）被检出；
- 慢读者超缓冲被明确中断，且不拖住另一条订阅；各连接独立取消；
- 明确核对每个事件的字节位置，并对“证据缺口”（截断/缺口）给出明确错误。
