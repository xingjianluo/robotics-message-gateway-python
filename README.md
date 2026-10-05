# Robotics Message Gateway (Python)

基于 FastAPI 的本地机器人消息网关：不依赖外部消息中间件，多个客户端可通过
HTTP 与 WebSocket 进行发布/订阅，并获得一致、可验证的结果。

## 快速开始

```bash
uv sync                 # 安装依赖
uv run uvicorn main:app # 启动服务（默认 http://127.0.0.1:8000）
uv run python -m pytest # 运行全部测试
```

运行参数可用环境变量覆盖（`main.py` 读取，默认值见 `gateway/app.py:create_app`）：

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `GATEWAY_MAX_QUEUE` | 100 | 单订阅积压队列上限 |
| `GATEWAY_MAX_INFLIGHT` | 64 | 单连接未确认消息上限（背压阈值） |
| `GATEWAY_SESSION_TTL` | 0（不过期） | 离线会话过期秒数 |
| `GATEWAY_MAX_OFFLINE_BACKLOG` | 0（不限） | 单客户端离线积压总量上限 |
| `GATEWAY_REAP_INTERVAL` | 1.0 | 过期回收扫描间隔秒数 |

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

- 凭据：`token -> client_id` 映射；管理凭据是独立的 admin token 集合。
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

### 连接身份与凭据绑定

- WS 连接的 `client_id` **必须与凭据归属的身份一致**；不一致时返回
  `identity_mismatch` 错误帧并以 **4403** 关闭，不影响其他连接。
  （省略 `client_id` 时默认使用凭据身份。）
- **同一身份多开连接 = 接管（takeover）**：后连上的顶掉先前的。旧连接收到
  `{"type":"displaced","reason":"takeover"}` 帧后以 **4000** 关闭；旧连接上
  未确认的消息按原顺序交接给新连接重投（`redelivered=true`），在途未投递的
  消息也按序回到会话，由新连接继续投递——对该身份的消息**不重、不乱序**。
  每次接管计入会话指标 `takeovers`。

## 运行期治理（/admin）

服务不重启即可增删账号、调整权限，改动**立即**作用于在线连接与后续请求。
所有 `/admin` 端点需要管理凭据 `Authorization: Bearer <admin-token>`
（admin token 与客户端 token 分离配置，见 `ACLStore.set_admin_tokens`）。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/admin/accounts` | 列出全部账号（client_id -> tokens） |
| POST | `/admin/accounts` | 新增/更新账号，体 `{"client_id","token"}` |
| DELETE | `/admin/accounts/{client_id}` | 注销账号：token 与规则移除，在线连接收到 `displaced(account_removed)` 后以 4401 关闭，其订阅全部收回 |
| PUT | `/admin/accounts/{client_id}/acl` | 整体替换该账号的主题范围，体 `{"publish":[...],"subscribe":[...]}` |

管理端点的失败结果可互相区分：

| 状态码 | reason | 场景 |
| --- | --- | --- |
| 401 | `invalid_credentials` | 缺失/未知凭据 |
| 403 | `not_admin` | 凭据有效但不是管理凭据 |
| 400 | `invalid_request` | 请求体不合法（缺字段、非法过滤器） |
| 404 | `unknown_client` | 目标账号不存在 |
| 409 | `token_in_use` | token 已被其他身份占用 |

### 权限收紧的追溯语义（兼容边界）

- 收紧某账号的订阅范围后，其**在线连接中不再被允许的订阅立即失效**：
  连接收到 `{"type":"unsubscribed","filter":...,"reason":"permission_revoked"}`，
  之后不再收到那些主题的消息（包括已出队未投递的在途消息，会被清除）。
- **未确认消息不受影响**：收紧前已投递未确认的消息仍保留在会话中，
  重连后按原顺序重投（不丢、不乱序）。
- 追溯收回只作用于被调整的账号，**同一时刻其他连接不受任何影响**。
- 权限放宽同理立即生效：既有连接无需重连即可订阅新放行的范围。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 健康检查 |
| POST | `/publish` | 发布；头 `Authorization: Bearer <token>`，体 `{"topic","payload","retain"}` |
| GET | `/retained?filter=<f>` | 查询保留消息（仅返回有订阅权限的主题） |
| GET | `/stats` | 可观测指标 |
| * | `/admin/*` | 运行期治理（见上节） |

## WebSocket 协议

连接：`/ws?token=<t>&client_id=<id>&clean=<true|false>`
（`client_id` 可省略，默认取凭据身份；若提供则必须与凭据身份一致）

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
{"type":"unsubscribed","filter":"...","reason":"permission_revoked"}
{"type":"displaced","reason":"takeover"}
{"type":"displaced","reason":"account_removed"}
{"type":"published","topic":"...","seq":1}
{"type":"message","message":{"topic":"...","payload":"...","retain":false,"seq":1},"redelivered":false}
{"type":"error","op":"...","reason":"..."}
```

关闭码：`4401` 凭据无效/账号已注销；`4403` 身份与凭据不一致；`4000` 被同身份新连接接管。

## 会话恢复规则

- 会话按 `client_id` 持久保存：订阅关系、未确认消息（`pending`）、
  断线时已出队未发送消息（`ready`）。
- 断线期间订阅保持有效，消息积压在订阅的有界队列中。
- 重连（同 `client_id` 且 `clean=false`）后按以下顺序投递，保证不丢、不重、不乱序：
  1. 按原顺序重投未确认消息（`redelivered=true`）；
  2. 投递上次断线时已出队未发送的消息；
  3. 继续投递离线积压与实时消息。
- 同身份新连接接管时走同一套交接逻辑：旧连接收尾完成后新连接才开始投递。
- `clean=true` 连接会丢弃旧会话（订阅与未确认消息全部清除）。

## 慢消费者与内存有界

- 每个订阅对应有界队列（`max_queue`，默认 100）；每个连接有独立的
  转发队列与未确认上限（`max_inflight`，默认 64）。
- 慢消费者未确认达上限后背压；其订阅队列满时**仅丢弃该消费者的消息**
  并计数，其他订阅者投递不受任何影响。
- **单客户端离线积压总量上限**（`max_offline_backlog`）：离线会话的所有订阅
  积压之和超限后，从积压最长的订阅队列头部丢弃最旧消息并计数
  （会话 `backlog_dropped` 与全局 `expiry.backlog_dropped`），
  重连后只能收到保留的最新部分。只作用于离线会话，不影响在线客户端。
- **会话过期回收**（`session_ttl`）：断线超过 TTL 的会话被后台任务整体回收
  （订阅摘除、会话对象释放，内存真正回收），回收会话数与被丢弃消息数
  计入 `expiry.sessions_expired` / `expiry.messages_expired`。
  在线会话无论空闲多久都不会被回收。
- 内存守恒可验证：`pending_unacked + ready + backlog + dropped == 已发布数`。

## 可观测指标

`GET /stats` 返回：

- `broker.retained_topics`：保留消息主题数；
- `broker.subscriptions[]`：每个订阅的 `backlog`（积压）、`delivered`、`dropped`；
- `sessions[]`：每个会话的 `connected`、`pending_unacked`、`ready`、
  `redelivered`、`takeovers`（被接管次数）、`revoked_subscriptions`
  （权限收紧被收回的订阅数）、`backlog_dropped`（积压超限丢弃数）、
  各订阅积压与丢弃数；
- `expiry`：`sessions_expired`（回收会话数）、`messages_expired`
  （随回收丢弃的消息数）、`backlog_dropped`（积压超限全局丢弃数）。

## 本地验证

```bash
uv run python -m pytest -v            # 全部用例；日志打印输入、判定依据、最终状态
uv run python -m pytest tests/test_topics.py        # 通配匹配与非法主题
uv run python -m pytest tests/test_retained.py      # 保留消息
uv run python -m pytest tests/test_acl.py           # 越权访问与无效凭据
uv run python -m pytest tests/test_reconnect.py     # 断线重连与顺序恢复
uv run python -m pytest tests/test_slow_consumer.py # 慢消费者隔离
uv run python -m pytest tests/test_concurrency.py   # 并发发布一致性
uv run python -m pytest tests/test_admin.py         # 运行期治理与追溯生效
uv run python -m pytest tests/test_identity.py      # 身份绑定与同身份接管
uv run python -m pytest tests/test_expiry.py        # 会话过期与积压上限
```

手工验证示例（先按 `tests/conftest.py` 的示例配置 token 与 admin token）：

```bash
uv run uvicorn main:app &
# 发布
curl -X POST localhost:8000/publish -H "Authorization: Bearer t-pub" \
     -H 'Content-Type: application/json' \
     -d '{"topic":"robot/arm","payload":"hello","retain":true}'
# 运行期新增账号并授权
curl -X POST localhost:8000/admin/accounts -H "Authorization: Bearer t-root" \
     -H 'Content-Type: application/json' \
     -d '{"client_id":"robot-7","token":"t-r7"}'
curl -X PUT localhost:8000/admin/accounts/robot-7/acl -H "Authorization: Bearer t-root" \
     -H 'Content-Type: application/json' \
     -d '{"publish":[],"subscribe":["robot/arm/#"]}'
# 收紧权限（既有连接的越范围订阅会被追溯收回）
curl -X PUT localhost:8000/admin/accounts/robot-7/acl -H "Authorization: Bearer t-root" \
     -H 'Content-Type: application/json' \
     -d '{"publish":[],"subscribe":["robot/arm/joint1"]}'
# 查看指标（含 takeovers / revoked_subscriptions / expiry）
curl localhost:8000/stats -H "Authorization: Bearer t-sub"
# 开启过期回收与积压上限启动
GATEWAY_SESSION_TTL=300 GATEWAY_MAX_OFFLINE_BACKLOG=1000 uv run uvicorn main:app
```

## 代码结构

```
gateway/
  topics.py   # 主题校验、通配匹配、ACL 覆盖判定
  messages.py # 消息模型（全局 seq）
  auth.py     # 凭据与 ACL 规则存储、运行期账号/规则治理、发布/订阅鉴权
  broker.py   # 订阅、发布、保留消息、慢消费者隔离
  session.py  # 会话：订阅、未确认、待重投、接管/收回计数、统计
  expiry.py   # 会话过期回收与离线积压上限
  admin.py    # /admin 管理接口（管理凭据保护、错误可区分）
  app.py      # FastAPI 装配：HTTP/WS 入口、身份绑定、接管、追溯收回
main.py       # 应用入口（uvicorn main:app，环境变量可配运行参数）
tests/        # 关键组合测试，日志含输入/判定依据/最终状态
```
