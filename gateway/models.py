"""核心数据模型。

主题语义：
- 层级以 ``/`` 分隔，例如 ``robots/r1/status``。
- 订阅过滤器允许通配符：``+`` 匹配单层，``#`` 只能位于末尾并匹配零到多层。
- 发布主题不允许出现通配符。
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass, field
from typing import Any

ClientId = str
"""逻辑客户端标识；凭据合法即可确定，重连时复用以恢复会话。"""

TopicName = str
"""具体发布主题（不含通配符）。"""

TopicFilter = str
"""订阅过滤器（可含 + / #）。"""

MAX_TOPIC_LENGTH = 1024
MAX_PAYLOAD_BYTES = 64 * 1024


@dataclass(frozen=True, slots=True)
class Message:
    """一条应用消息。

    seq 为网关上全局单调递增的发布序号，既用于保序，也用于
    判断重复（同 topic+seq 必为同一条消息）。
    """

    seq: int
    topic: TopicName
    payload: Any = None
    publisher: ClientId | None = None
    retained: bool = False
    published_at_ms: int = 0


@dataclass(frozen=True, slots=True)
class Delivery:
    """发给某客户端的一次投递。

    delivery_id 在单个会话内单调递增；ack 以它为键。
    """

    delivery_id: int
    seq: int
    topic: TopicName
    payload: Any
    publisher: ClientId | None
    retained: bool
    duplicate: bool = False


@dataclass(slots=True)
class Subscription:
    """一条订阅记录。"""

    client_id: ClientId
    filter: TopicFilter
    qos: int = 1


@dataclass(slots=True)
class SessionState:
    """持久会话状态（断线期间仍保留）。"""

    client_id: ClientId
    subscriptions: dict[TopicFilter, Subscription] = field(default_factory=dict)
    # 未确认投递：delivery_id -> Delivery
    inflight: dict[int, Delivery] = field(default_factory=dict)
    # 断线期间等待投递的消息（保序，有界，见 Session.max_queue）
    queued: deque[Delivery] = field(default_factory=deque)
    next_delivery_id: int = 1
    enqueued_total: int = 0
    dropped_total: int = 0
