# Robotics Message Gateway (Python)

基于 FastAPI 的本地机器人消息网关：不依赖外部消息中间件，多个客户端可通过
HTTP 与 WebSocket 进行发布/订阅，并获得一致、可验证的结果。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run uvicorn main:app # 启动服务（默认 http://127.0.0.1:8000）
uv run pytest           # 运行全部测试
```

## 消息语义

### 主题与通配

- 主题以 `/` 分层，如 `robot/arm/joint1`；仅允许可见 ASCII（0x21–0x7E），最长 256。
- 订阅过滤器支持通配符：`+` 匹配单层，`#` 匹配剩余所有层（必须独占最后一层）。
- 发布主题不允许包含通配符。
- 非法主题/过滤器被拒绝并返回**可区分原因**：

| reason | 含义 |
| --- | --- |
| `empty` | 空主题 |
| `too_long` | 超过长度上限 |
| `empty_level` | 存在空层级（`a//b`、首尾 `/`） |
| `wildcard_in_publish` | 发布主题含通配符 |
| `hash_not_last` | `#` 未独占最后一层 |
| `invalid_char` | 非法字符或 `+` 未独占一层 |

### 保留消息（retained）

- 以 `retain=true` 发布的消息按主题保留最后一条。
- 新订阅建立时，所有匹配过滤器的保留消息按 `seq` 升序**先于实时消息**投递。
- `retain=true` 且 `payload` 为空字符串表示清除该主题的保留消息。
- 重复订阅同一过滤器是幂等的（返回 `duplicate=true`），不会重投保留消息。

### 投递与确认

- 每条消息有全局递增 `seq`；同一订阅内按 `seq` 顺序投递。
- 客户端收到 `message` 后应以 `{"op":"ack","seq":...}` 确认。
- 单连接未确认消息超过 `max_inflight`（默认 64）时对该连接施加背压。

## 权限模型

- 凭据：`token -> client_id` 映射（`ACLStore.set_tokens`）。
- 规则：`(client_id | "*", action, topic_filter)` 有序列表，**默认拒绝**，任一规则匹配即放行。
- 发布鉴权：规则过滤器用通配匹配覆盖发布的具体主题。
- 订阅鉴权：规则过滤器必须**完全覆盖**请求的过滤器
  （如规则 `robot/arm/#` 允许订阅 `robot/arm/+`，但拒绝 `robot/#`）。
- 鉴权在每次发布/订阅时实时查询，因此**规则更新对既有连接立即生效**。
- 拒绝结果可区分：

| reason | 场景 |
| --- | --- |
| `invalid_credentials` | 缺失/未知 token（HTTP 401；WS 发送错误帧后以 4401 关闭） |
| `not_allowed` | 凭据有效但无匹配规则（HTTP 403；WS 返回错误帧，连接保持） |

越权请求不会进入 broker，不影响其他连接。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 健康检查 |
| POST | `/publish` | 发布；头 `Authorization: Bearer <token>`，体 `{"topic","payload","retain"}` |
| GET | `/retained?filter=<f>` | 查询保留消息（仅返回有订阅权限的主题） |
| GET | `/stats` | 可观测指标 |

## WebSocket 协议

连接：`/ws?token=<t>&client_id=<id>&clean=<true|false>`

客户端 → 服务端：

```json
{"op":"subscribe","filter":"robot/#"}
{"op":"unsubscribe","filter":"robot/#"}
{"op":"publish","topic":"robot/arm","payload":"...","retain":false}
{"op":"ack","seq":123}
```

服务端 → 客户端：

```json
{"type":"connected","client_id":"..."}
{"type":"subscribed","filter":"...","duplicate":false}
{"type":"unsubscribed","filter":"..."}
{"type":"published","topic":"...","seq":1}
{"type":"message","message":{"topic":"...","payload":"...","retain":false,"seq":1},"redelivered":false}
{"type":"error","op":"...","reason":"..."}
```

## 会话恢复规则

- 会话按 `client_id` 持久保存：订阅关系、未确认消息（`pending`）、
  断线时已出队未发送消息（`ready`）。
- 断线期间订阅保持有效，消息积压在订阅的有界队列中。
- 重连（同 `client_id` 且 `clean=false`）后按以下顺序投递，保证不丢、不重、不乱序：
  1. 按原顺序重投未确认消息（`redelivered=true`）；
  2. 投递上次断线时已出队未发送的消息；
  3. 继续投递离线积压与实时消息。
- `clean=true` 连接会丢弃旧会话（订阅与未确认消息全部清除）。

## 慢消费者与内存有界

- 每个订阅对应有界队列（`max_queue`，默认 100）；每个连接有独立的
  转发队列与未确认上限（`max_inflight`，默认 64）。
- 慢消费者未确认达上限后背压；其订阅队列满时**仅丢弃该消费者的消息**
  并计数，其他订阅者投递不受任何影响。
- 内存守恒可验证：`pending_unacked + ready + backlog + dropped == 已发布数`。

## 可观测指标

`GET /stats` 返回：

- `broker.retained_topics`：保留消息主题数；
- `broker.subscriptions[]`：每个订阅的 `backlog`（积压）、`delivered`、`dropped`；
- `sessions[]`：每个会话的 `connected`、`pending_unacked`、`ready`、
  `redelivered`、各订阅积压与丢弃数。

## 本地验证

```bash
uv run pytest -v            # 全部用例；日志打印输入、判定依据、最终状态
uv run pytest tests/test_topics.py       # 通配匹配与非法主题
uv run pytest tests/test_retained.py     # 保留消息
uv run pytest tests/test_acl.py          # 越权访问与无效凭据
uv run pytest tests/test_reconnect.py    # 断线重连与顺序恢复
uv run pytest tests/test_slow_consumer.py # 慢消费者隔离
uv run pytest tests/test_concurrency.py  # 并发发布一致性
```

手工验证示例：

```bash
uv run uvicorn main:app &
TOKEN=...  # 需先在 gateway 中配置 token，见 tests/conftest.py 的示例规则
curl -X POST localhost:8000/publish -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' \
     -d '{"topic":"robot/arm","payload":"hello","retain":true}'
```

## 代码结构

```
gateway/
  topics.py   # 主题校验、通配匹配、ACL 覆盖判定
  messages.py # 消息模型（全局 seq）
  auth.py     # 凭据与 ACL 规则存储、发布/订阅鉴权
  broker.py   # 订阅、发布、保留消息、慢消费者隔离
  session.py  # 会话：订阅、未确认、待重投、统计
  app.py      # FastAPI 装配：HTTP 与 WebSocket 入口
main.py       # 应用入口（uvicorn main:app）
tests/        # 关键组合测试，日志含输入/判定依据/最终状态
```
