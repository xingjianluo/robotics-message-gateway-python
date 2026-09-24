# 机器人消息网关（本地可运行，无外部中间件）

一个进程内、零外部依赖（仅 FastAPI/Starlette）的发布/订阅网关。多个客户端通过
**HTTP** 与 **WebSocket** 接入，得到一致、可验证的消息结果。

- 层级主题、`+`/`#` 通配订阅、**保留消息（Retained）**
- 按客户端 + 主题范围的访问控制（ACL），支持**热更新**并即时作用于既有连接
- 未确认消息的**会话持久化**、断线重连按原序续投（不丢、不重、不乱）
- 慢消费者背压、有界内存、可观测丢弃
- 可区分原因的拒绝错误码、可观测指标

## 快速开始

```bash
uv sync --group dev

# 方式一：模块入口
uv run python main.py
# 方式二：uvicorn（支持 --reload）
uv run uvicorn main:app --host 127.0.0.1 --port 8000
```

启动后：

- 服务索引：<http://127.0.0.1:8000/>
- 交互式文档：<http://127.0.0.1:8000/docs>
- 指标：<http://127.0.0.1:8000/metrics>

### 开箱即用账号

| client_id | token | 权限 |
|---|---|---|
| `robot-r1` | `token-r1` | 发 `robots/r1/status`；收发 `robots/r1/cmd` |
| `dashboard` | `token-dash` | 订阅 `robots/+/status` 与 `robots/#` |
| `ops` | `token-ops` | 超管：`#` 全主题收发（可更新 ACL） |

## 主题与消息语义

- 层级以 `/` 分隔，例如 `robots/r1/status`；不允许空层级（`a//b`、`a/`、`/a` 非法）。
- **发布主题**不允许通配符；**订阅过滤器**允许：
  - `+` 匹配且仅匹配**单个层级**：`robots/+/status` 匹配 `robots/r1/status`，不匹配 `robots/r1/x`。
  - `#` 必须独占**最后一个层级**，匹配其父层级及其下任意多层：
    `robots/#` 同时匹配 `robots`、`robots/a`、`robots/a/b/c`；`#` 匹配一切。
- 每条发布获得网关内**全局单调递增** `seq`，作为全局顺序与去重依据。
- **保留消息**：`retain=true` 的发布按“具体主题”保存**最后一条**。
  - 新订阅建立时，立即收到过滤器匹配的**每个主题各一条**保留消息（`retained=true`），
    多主题时按主题名字典序投递，结果确定。
  - 用 `payload=null` 的保留发布清除该主题保留（墓碑）。
  - 实时扇出的消息 `retained=false`，即使该消息同时被保存为保留消息。
- **重复订阅**同一过滤器是幂等的：不新增订阅、不重复投保留消息、后续消息不重复。

非法主题返回可区分错误码：`empty_topic` / `topic_too_long` / `empty_level` /
`bad_wildcard_position` / `illegal_character`。

## 传输协议

### HTTP

- `POST /publish`：Basic 认证（`Authorization: Basic base64(client_id:token)`），
  也支持 `?client_id=&token=`。
  ```json
  { "topic": "robots/r1/status", "payload": {"v": 1}, "retain": false }
  ```
- `GET /retained?filter=robots/+/status`：查询匹配的保留消息（默认 `#`）。
- `GET /metrics`：指标与各会话积压。
- `GET /sessions/{client_id}`：单个会话状态。
- `POST /admin/credentials`：超管热更新整份凭据/ACL（Basic 认证为 `ops`）。

HTTP 拒绝状态码：`401`（`missing_credentials` / `invalid_credentials`）、
`403`（`publish_forbidden` / `subscribe_forbidden`）、`400`（主题/载荷错误），
响应体形如 `{"status":"error","code":"...","message":"..."}`。

### WebSocket

连接：`ws://127.0.0.1:8000/ws?client_id=dashboard&token=token-dash`
（也支持 `Authorization: Basic ...`）。

客户端帧：

```jsonc
{"action":"subscribe",   "id":"c1", "filter":"robots/+/status"}
{"action":"unsubscribe", "id":"c2", "filter":"robots/+/status"}
{"action":"publish",     "id":"c3", "topic":"robots/r1/status", "payload":{}, "retain":false}
{"action":"ack",         "id":"c4", "delivery_id":12}
```

服务端帧：

```jsonc
{"type":"ack",     "id":"c1", "action":"subscribe", "status":"ok", ...}
{"type":"error",   "id":"c3", "status":"error", "code":"publish_forbidden", "message":"..."}
{"type":"message", "delivery_id":12, "seq":3, "topic":"...", "payload":...,
                   "publisher":"...", "retained":false, "duplicate":false}
```

握手期关闭码：`4401` 缺失凭据、`4403` 非法凭据、`4409` 重复连接（旧连接不受影响）。
单条坏帧不影响连接：无法解析返回 `malformed_payload`，未知 action 返回
`unsupported_action`。

## 权限模型

- 凭据 = `client_id` + 不透明 `token`，双因素校验；“未提供”与“非法”可区分。
- 权限为若干条目 `Permission(filter, actions)`：
  - `filter` 支持与订阅相同的层级通配（`+`/`#`）。
  - `actions` 为 `pub` / `sub` 的子集；发布与订阅分别授权。
- 判定发生在**每次发布/订阅时**而非仅建连时，因此：
  - `POST /admin/credentials` 热更新规则后，既有连接的下一次操作立即按新规则判定；
  - 网关会 `reconcile` 既有订阅：不再被允许的订阅被立即撤销（之后不再收消息），
    撤销前已在途的消息仍按确认协议处理（不强杀）；
  - 越权操作只拒绝当前请求/帧，不影响该连接上的其他操作或任何其他连接。
- 超管定义：拥有过滤器 `#` 且同时含 `pub+sub` 的客户端可调用管理接口。

## 会话恢复规则（可靠性）

- 以 `client_id` 标识持久会话；**重连复用同一 client_id** 即恢复。
- 每条投递在会话内有单调递增的 `delivery_id`；`(client_id, delivery_id)` 唯一。
- 两级有界缓冲（默认 `max_inflight=100`，`max_queue=1000`）：
  - **inflight（在途）**：已投递但未 `ack`，容量即未确认窗口。
  - **queue（排队）**：窗口满（慢消费者）或连接断开时，消息保序排队。
- 连接恢复时：
  1. 旧 inflight 中未确认的消息**按原序重投**，并置 `duplicate=true`；
  2. 随后随 `ack` 腾出窗口，queue 中的消息按序首次下发（`duplicate=false`）。
  3. 因此同一条消息至多“首次 + 若干重复重投”，**不丢失、不重复（语义一次）、不乱序**。
- `ack` 三态可区分：`removed`（首次确认）、`duplicate`（重复确认）、`unknown`（未知 id）。
- **背压与丢弃**：总背压 = inflight + queue，严格有界；超出采用 **drop-newest**
  （丢弃最新消息，保护已入队消息的严格顺序）。传输层发送队列同样有界，溢出时
  通过 `abandon` 立即释放窗口并计数。慢消费者不会阻塞发布者或其他客户端。

## 可观测指标

`GET /metrics` 示例字段：

| 字段 | 含义 |
|---|---|
| `published_total` | 受理的发布总数 |
| `delivered_total` | 成功进入会话缓冲的投递次数（含扇出） |
| `retained_total` | 保存的保留消息条数 |
| `rejected_total` / `rejection_reasons` | 拒绝总数及按错误码分布 |
| `dropped_total` | 背压/发送队列丢弃总数 |
| `reconnect_recovered_total` | 重连重投的在途消息数 |
| `duplicate_acks_total` | 重复确认次数 |
| `backlog` | 各客户端当前积压（inflight+queue） |
| `sessions[]` | 每会话：连接态、订阅、inflight/queued/backlog、游标、丢弃数 |

丢弃原因在 `rejection_reasons` 中以 `drop:queue_full` / `drop:send_queue_full` 区分。

## 本地验证

```bash
# 单元 + 集成测试（45 个用例，含通配/保留/越权/乱序重连/慢消费者关键组合）
uv run pytest -q

# 手动联调脚本（需要先启动服务器）
uv run python scripts/smoke_ws.py
```

测试日志会为关键用例打印 `CASE / INPUT / BASIS / FINAL`（输入、判定依据、最终状态），
失败时可直接依据错误码区分原因。
