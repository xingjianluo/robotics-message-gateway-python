"""客户端持久会话：在途窗口、断线排队、顺序恢复、有界丢弃。

语义（全部在事件循环单线程内调用，故内部不加锁）：

- ``inflight``：已投递到连接但尚未 ack 的消息，按 delivery_id 保序；
  其容量上限 ``max_inflight`` 即未确认窗口。
- ``queue``：窗口已满（慢消费者）或连接断开时的等待队列，容量上限
  ``max_queue``；满时丢弃“最新”消息（drop-newest），保证已入队
  消息的严格顺序，丢弃数与会话级结论可观测。
- ack 后按序把 queue 中的消息升入 inflight。
- 重连（attach）时，旧 inflight 以 duplicate 标记原序重投，随后再
  提升排队消息；排队消息的 seq 必更大，因此全局顺序不乱。
- delivery_id 仅在会话内单调；(client_id, delivery_id) 唯一标识一次投递。
"""

from __future__ import annotations

import logging
from collections import deque
from dataclasses import replace

from .errors import ErrorCode, GatewayError
from .models import Delivery, Message, SessionState, Subscription, TopicFilter

logger = logging.getLogger(__name__)


class Session:
    def __init__(
        self,
        client_id: str,
        max_inflight: int = 100,
        max_queue: int = 1000,
    ) -> None:
        self.client_id = client_id
        self.max_inflight = max_inflight
        self.max_queue = max_queue
        self.state = SessionState(client_id=client_id)
        self._connected = False
        self._ever_attached = False
        # 已可发送但尚未被连接适配器取走的 delivery_id
        self._pending_send: deque[int] = deque()
        # 最近已 ack 的 id，用于区分“重复 ack”与“未知 ack”，有界
        self._acked_ids: set[int] = set()
        self._acked_order: deque[int] = deque()
        # 连接适配器注册的唤醒回调（有新消息可发时触发）
        self.on_available: "object" = None

    # ---- 连接生命周期 ----
    @property
    def connected(self) -> bool:
        return self._connected

    def attach(self) -> list[Delivery]:
        """挂接连/重连。

        返回需要立即下发的消息：先按原序重投未确认的在途消息
        （重复连接标记 duplicate），再提升排队消息填满窗口。
        """
        if self._connected:
            raise GatewayError(
                ErrorCode.DUPLICATE_CONNECTION,
                f"client {self.client_id!r} already connected",
            )
        self._connected = True
        self._ever_attached = True
        # 适配器重建后，先前未取走的发送缓冲作废，以当前 inflight 全量重投为准
        self._pending_send.clear()

        # 1) 未确认的在途消息原序重投：重连时一律标记 duplicate
        #    （首次连接 inflight 必为空；queue 提升后未确认即断线的也算重投）
        for did, delivery in list(self.state.inflight.items()):
            if not delivery.duplicate:
                delivery = replace(delivery, duplicate=True)
                self.state.inflight[did] = delivery
            self._pending_send.append(did)
        # 2) 排队消息（从未下发过）按序填满窗口，duplicate 保持 False
        self._pending_send.extend(d.delivery_id for d in self._promote_locked())
        self._notify()
        return self.drain_pending()

    def detach(self) -> None:
        """异常/正常断开：在途与排队都保留，等待重连。"""
        self._connected = False
        self.on_available = None
        self._pending_send.clear()

    # ---- 订阅 ----
    def add_subscription(self, sub: Subscription) -> bool:
        """新增订阅；返回是否为新过滤器（False 表示重复订阅，不产生副作用）。"""
        if sub.filter in self.state.subscriptions:
            return False
        self.state.subscriptions[sub.filter] = sub
        return True

    def remove_subscription(self, topic_filter: TopicFilter) -> bool:
        return self.state.subscriptions.pop(topic_filter, None) is not None

    def has_subscription(self, topic_filter: TopicFilter) -> bool:
        return topic_filter in self.state.subscriptions

    def matching_filters(self, topic_levels: list[str]) -> list[str]:
        from .topics import matches

        return [
            f
            for f in self.state.subscriptions
            if matches(_levels(f), topic_levels)
        ]

    # ---- 消息接收/确认 ----
    def accept(self, message: Message) -> bool:
        """接收一条待投消息；被丢弃返回 False。"""
        delivery = self._build_delivery(message)
        if self._connected and len(self.state.inflight) < self.max_inflight:
            self.state.inflight[delivery.delivery_id] = delivery
            self._pending_send.append(delivery.delivery_id)
            self._notify()
            return True
        return self._enqueue(delivery)

    def _enqueue(self, delivery: Delivery) -> bool:
        if len(self.state.queued) >= self.max_queue:
            # drop-newest：保持已入队消息严格有序，记录可观测丢弃结论
            self.state.dropped_total += 1
            logger.warning(
                "drop-newest client=%s delivery_id=%d seq=%d topic=%s reason=queue_full "
                "inflight=%d queued=%d",
                self.client_id, delivery.delivery_id, delivery.seq, delivery.topic,
                len(self.state.inflight), len(self.state.queued),
            )
            return False
        self.state.queued.append(delivery)
        self.state.enqueued_total += 1
        return True

    def acknowledge(self, delivery_id: int) -> str:
        """确认投递。

        返回 ``"removed"``（正常确认）、``"duplicate"``（重复确认）
        或 ``"unknown"``（未知 id）；确认后按序提升排队消息。
        """
        removed = self.state.inflight.pop(delivery_id, None)
        promoted: list[Delivery] = []
        if removed is not None:
            self._remember_acked(delivery_id)
            promoted = self._promote_locked()
            if promoted:
                self._pending_send.extend(d.delivery_id for d in promoted)
                self._notify()
            return "removed"
        if delivery_id in self._acked_ids:
            return "duplicate"
        return "unknown"

    def _promote_locked(self) -> list[Delivery]:
        promoted: list[Delivery] = []
        while (
            self._connected
            and len(self.state.inflight) < self.max_inflight
            and self.state.queued
        ):
            delivery = self.state.queued.popleft()
            self.state.inflight[delivery.delivery_id] = delivery
            promoted.append(delivery)
        return promoted

    def drain_pending(self) -> list[Delivery]:
        """连接适配器取走当前全部可发送消息，按 delivery_id 升序。"""
        out: list[Delivery] = []
        while self._pending_send:
            did = self._pending_send.popleft()
            delivery = self.state.inflight.get(did)
            if delivery is not None:
                out.append(delivery)
        return out

    def abandon(self, delivery_id: int, reason: str = "send_queue_full") -> bool:
        """放弃一次已入在途但传输层无法承接的投递（慢消费者发送队列满）。

        与未确认不同：放弃即视为永久失败，立刻腾出窗口并按序提升排队
        消息，避免该投递永久卡住窗口；计入 dropped_total。
        """
        if self.state.inflight.pop(delivery_id, None) is None:
            return False
        self.state.dropped_total += 1
        promoted = self._promote_locked()
        if promoted:
            self._pending_send.extend(d.delivery_id for d in promoted)
            self._notify()
        logger.warning(
            "abandon delivery client=%s delivery_id=%d reason=%s",
            self.client_id, delivery_id, reason,
        )
        return True

    def backlog(self) -> int:
        return len(self.state.inflight) + len(self.state.queued)

    def snapshot(self) -> dict:
        return {
            "client_id": self.client_id,
            "connected": self._connected,
            "subscriptions": sorted(self.state.subscriptions),
            "inflight": len(self.state.inflight),
            "queued": len(self.state.queued),
            "backlog": self.backlog(),
            "delivered_cursor": self.state.next_delivery_id - 1,
            "enqueued_total": self.state.enqueued_total,
            "dropped_total": self.state.dropped_total,
        }

    # ---- 内部 ----
    def _build_delivery(self, message: Message) -> Delivery:
        did = self.state.next_delivery_id
        self.state.next_delivery_id += 1
        return Delivery(
            delivery_id=did,
            seq=message.seq,
            topic=message.topic,
            payload=message.payload,
            publisher=message.publisher,
            retained=message.retained,
        )

    def _remember_acked(self, delivery_id: int) -> None:
        self._acked_ids.add(delivery_id)
        self._acked_order.append(delivery_id)
        cap = max(self.max_inflight * 2, 16)
        while len(self._acked_order) > cap:
            self._acked_ids.discard(self._acked_order.popleft())

    def _notify(self) -> None:
        callback = self.on_available
        if callback is not None:
            callback()


def _levels(topic_filter: str) -> list[str]:
    # 订阅存入时已校验，直接切分
    return topic_filter.split("/")


class SessionRegistry:
    def __init__(self, max_inflight: int = 100, max_queue: int = 1000) -> None:
        self.max_inflight = max_inflight
        self.max_queue = max_queue
        self._sessions: dict[str, Session] = {}

    def get_or_create(self, client_id: str) -> Session:
        session = self._sessions.get(client_id)
        if session is None:
            session = Session(client_id, self.max_inflight, self.max_queue)
            self._sessions[client_id] = session
        return session

    def get(self, client_id: str) -> Session | None:
        return self._sessions.get(client_id)

    def all_sessions(self) -> list[Session]:
        return list(self._sessions.values())
