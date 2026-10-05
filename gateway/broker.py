"""核心消息代理：订阅、发布、保留消息、慢消费者隔离。

设计要点：
- 每个订阅对应一个有界队列（max_queue）。发布使用 put_nowait，
  慢消费者队列满时仅丢弃该消费者的消息并计数，绝不阻塞其他订阅者。
- 保留消息按主题存储；新订阅建立时，匹配的保留消息按 seq 升序
  先行入队，保证新订阅者先收到保留消息再收到实时消息。
- retain=True 且 payload 为空表示清除该主题的保留消息。
"""
from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass, field

from . import topics
from .messages import Message


@dataclass(frozen=True)
class PublishResult:
    accepted: bool
    delivered: int = 0


@dataclass
class Subscription:
    topic_filter: str
    queue: asyncio.Queue[Message] = field(repr=False)
    delivered: int = 0
    dropped: int = 0
    # 入队成功后的回调（由会话层挂载，用于离线积压总量上限等治理逻辑）
    overflow_hook: Callable[[Subscription], None] | None = field(
        default=None, repr=False
    )

    def stats(self) -> dict:
        return {
            "filter": self.topic_filter,
            "backlog": self.queue.qsize(),
            "delivered": self.delivered,
            "dropped": self.dropped,
        }


class Broker:
    def __init__(self, max_queue: int = 100) -> None:
        self.max_queue = max_queue
        self._retained: dict[str, Message] = {}
        self._subscriptions: list[Subscription] = []

    async def publish(self, message: Message) -> PublishResult:
        if message.retain:
            if message.payload:
                self._retained[message.topic] = message
            else:
                self._retained.pop(message.topic, None)
        delivered = 0
        for sub in list(self._subscriptions):
            if not topics.match(sub.topic_filter, message.topic):
                continue
            try:
                sub.queue.put_nowait(message)
                sub.delivered += 1
                delivered += 1
            except asyncio.QueueFull:
                sub.dropped += 1
            else:
                if sub.overflow_hook is not None:
                    sub.overflow_hook(sub)
        return PublishResult(accepted=True, delivered=delivered)

    def subscribe(self, topic_filter: str) -> Subscription:
        """建立订阅；匹配的保留消息按 seq 升序先行入队。"""
        sub = Subscription(
            topic_filter=topic_filter,
            queue=asyncio.Queue(maxsize=self.max_queue),
        )
        retained = sorted(
            (m for m in self._retained.values() if topics.match(topic_filter, m.topic)),
            key=lambda m: m.seq,
        )
        for msg in retained:
            try:
                sub.queue.put_nowait(msg)
            except asyncio.QueueFull:  # 保留消息超过队列容量时丢弃最旧的之外的部分
                sub.dropped += 1
        self._subscriptions.append(sub)
        return sub

    def unsubscribe(self, sub: Subscription) -> None:
        try:
            self._subscriptions.remove(sub)
        except ValueError:
            pass

    def retained(self, topic_filter: str) -> list[Message]:
        return sorted(
            (m for m in self._retained.values() if topics.match(topic_filter, m.topic)),
            key=lambda m: m.seq,
        )

    def stats(self) -> dict:
        return {
            "retained_topics": len(self._retained),
            "subscriptions": [s.stats() for s in self._subscriptions],
        }
