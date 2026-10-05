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
| `identity_mismatch` | WS 声明的 `client_id` 与凭据归属不一致（错误帧后以 4403 关闭） |
| `session_taken_over` | 同身份新连接接管，旧连接收到该错误帧后以 4409 关闭 |
| `account_revoked` | 账号被管理员注销，在线连接收到该错误帧后以 4401 关闭 |

越权请求不会进入 broker，不影响其他连接。

### 身份与凭据绑定

- 连接时声明的 `client_id` **必须与凭据（token）归属的身份一致**；
  缺省时自动使用凭据身份。
- 不一致即拒绝（`identity_mismatch`，关闭码 4403），一个凭据无法读取
  或接管另一个身份的会话数据；拒绝不影响其他连接。

### 同一身份的接管语义（takeover）

- 同一身份再次连接时，**后连上的顶掉先前的**：旧连接收到
  `session_taken_over` 错误帧后以 4409 关闭。
- 旧连接上未确认的消息留在会话 `pending` 中，由新连接**按原顺序重投**
  （`redelivered=true`），已出队未发送的消息按序交接；对该身份的消息
  不重、不乱序。
- 接管次数计入指标（会话级 `takeovers` 与全局 `gateway.takeovers`）。

### 运行期账号与权限治理（管理员接口）

管理接口由独立的管理员 token 保护（`create_app(admin_tokens=...)`，
默认 `t-admin`），无需重启即可生效：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/admin/clients` | 新增账号或更换 token，体 `{"client_id","token"}`；201 新建 / 200 更新 |
| DELETE | `/admin/clients/{id}` | 注销账号：在线连接收到 `account_revoked` 后以 4401 关闭，会话回收，token 立即失效 |
| PUT | `/admin/clients/{id}/acl` | 整体替换账号主题范围，体 `{"publish":[...],"subscribe":[...]}` |
| GET | `/admin/clients` | 列出账号与规则（不含 token） |

管理操作的结果可互相区分：

| 状态码 | reason | 场景 |
| --- | --- | --- |
| 401 | `invalid_credentials` | 缺失/未知 token |
| 403 | `not_allowed` | 合法客户端 token 但不是管理员 |
| 400 | `invalid_request` | 请求体缺字段或类型错误 |
| 400 | `invalid_filter` | ACL 过滤器非法（detail 含具体原因） |
| 404 | `not_found` | 目标账号不存在 |
| 409 | `token_in_use` | token 已绑定其他账号 |

**权限收紧的追溯生效**：PUT acl 收紧或收回权限后，该身份在线连接中
不再被允许的订阅**立即失效**（客户端收到
`{"type":"unsubscribed","filter":...,"reason":"acl_revoked"}`），
之后不再收到那些主题的消息；已出队未投递且仅匹配被撤销过滤器的消息
被丢弃并计数，已投递未确认（pending）的消息**原样保留**（重连后仍按
原顺序重投）。整个过程只触碰目标身份的会话与连接，不影响其他客户端。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 健康检查 |
| POST | `/publish` | 发布；头 `Authorization: Bearer <token>`，体 `{"topic","payload","retain"}` |
| GET | `/retained?filter=<f>` | 查询保留消息（仅返回有订阅权限的主题） |
| GET | `/stats` | 可观测指标 |
| * | `/admin/*` | 运行期治理接口，见「权限模型」一节 |

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
- 同身份新连接接管旧连接时，未确认消息按上述同一顺序由新连接重投，
  语义与断线重连一致。
- 离线超过 `session_expiry` 秒（默认 3600）的会话被回收：订阅取消、
  积压与未确认消息丢弃并计入 `gateway.expired_sessions` /
  `gateway.expired_messages`，内存真正释放；在线会话永不被回收。

## 慢消费者与内存有界

- 每个订阅对应有界队列（`max_queue`，默认 100）；每个连接有独立的
  转发队列与未确认上限（`max_inflight`，默认 64）。
- 慢消费者未确认达上限后背压；其订阅队列满时**仅丢弃该消费者的消息**
  并计数，其他订阅者投递不受任何影响。
- 内存守恒可验证：`pending_unacked + ready + backlog + dropped == 已发布数`。
- 单个客户端的**离线积压总量**有上限（`create_app(max_offline_backlog=...)`，
  默认不限）：超限即丢弃该订阅队列中最早的消息并计入
  `overflow_dropped`（会话级与 `gateway.overflow_dropped` 全局），
  不影响其他在线客户端；在线连接仍由背压与单队列上限约束。

## 可观测指标

`GET /stats` 返回：

- `broker.retained_topics`：保留消息主题数；
- `broker.subscriptions[]`：每个订阅的 `backlog`（积压）、`delivered`、`dropped`；
- `sessions[]`：每个会话的 `connected`、`pending_unacked`、`ready`、
  `redelivered`、`takeovers`、`overflow_dropped`、`disconnected_at`、
  各订阅积压与丢弃数。
- `gateway`：治理指标 `takeovers`（接管次数）、`expired_sessions` /
  `expired_messages`（过期回收）、`overflow_dropped`（离线积压超限丢弃）、
  `acl_revoked_subscriptions` / `acl_revoked_messages`（权限收紧追溯撤销）、
  `online_connections`。

## 兼容边界（相对旧行为的变化）

- **身份绑定**：旧行为允许 `client_id` 任意取值；现在必须与 token 归属
  身份一致（缺省自动等于凭据身份）。跨身份读取/接管会话数据不再可能。
- **同身份多开**：旧行为下两条连接会并存（语义未定义）；现在后连上的
  顶掉先前的，未确认消息按原顺序交接，不重、不乱序，接管次数可观测。
- **权限收紧**：既有连接的越权订阅会被追溯撤销（收到
  `reason=acl_revoked` 的 unsubscribed 帧），但已投递未确认的消息保留，
  重连后仍按原顺序重投；发布/订阅的逐次鉴权语义不变。
- **账号注销**：该身份的在线连接被关闭（4401）、会话回收、token 失效；
  其他客户端不受影响。
- 通配匹配、保留消息、断线重连补投、慢消费者隔离等原有语义均未改变。

## 本地验证

```bash
uv run pytest -v            # 全部用例；日志打印输入、判定依据、最终状态
uv run pytest tests/test_topics.py       # 通配匹配与非法主题
uv run pytest tests/test_retained.py     # 保留消息
uv run pytest tests/test_acl.py          # 越权访问与无效凭据
uv run pytest tests/test_reconnect.py    # 断线重连与顺序恢复
uv run pytest tests/test_slow_consumer.py # 慢消费者隔离
uv run pytest tests/test_concurrency.py  # 并发发布一致性
uv run pytest tests/test_identity.py     # 身份绑定与同身份接管
uv run pytest tests/test_admin.py        # 运行期账号/权限治理与追溯生效
uv run pytest tests/test_expiry.py       # 会话过期回收与离线积压上限
```

服务行为可通过 `create_app` 参数调整（`main.py` 使用默认值）：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `max_queue` | 100 | 单订阅积压队列容量 |
| `max_inflight` | 64 | 单连接未确认上限（背压） |
| `session_expiry` | 3600 | 离线会话过期秒数（None 不过期） |
| `reap_interval` | 60 | 过期会话扫描间隔秒数 |
| `max_offline_backlog` | None | 单客户端离线积压总量上限 |
| `admin_tokens` | `("t-admin",)` | 管理员 token 集合 |

手工验证示例：

```bash
uv run uvicorn main:app &
TOKEN=...  # 需先在 gateway 中配置 token，见 tests/conftest.py 的示例规则
curl -X POST localhost:8000/publish -H "Authorization: Bearer $TOKEN" \
     -H 'Content-Type: application/json' \
     -d '{"topic":"robot/arm","payload":"hello","retain":true}'

# 运行期新增账号并授予主题范围（管理员 token，默认 t-admin）
curl -X POST localhost:8000/admin/clients -H "Authorization: Bearer t-admin" \
     -H 'Content-Type: application/json' \
     -d '{"client_id":"robot-x","token":"t-x"}'
curl -X PUT localhost:8000/admin/clients/robot-x/acl \
     -H "Authorization: Bearer t-admin" -H 'Content-Type: application/json' \
     -d '{"subscribe":["robot/#"],"publish":[]}'
# 之后 robot-x 即可用 t-x 连接；收紧权限或注销账号会立即作用于在线连接
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
