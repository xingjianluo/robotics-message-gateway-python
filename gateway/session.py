"""客户端会话：断线重连后按原顺序恢复未确认消息。

会话模型：
- 会话按 client_id 持久持有 broker 订阅，断线不取消订阅，
  离线期间消息积压在订阅的有界队列中（容量由 Broker.max_queue 决定），
  溢出丢弃计数可观测。
- 已投递未确认的消息保存在 ``pending`` 中；重连后先按原顺序重投
  pending，再继续消费订阅队列，保证不丢、不重、不乱序。
- ``clean=True`` 的连接会清除旧会话（语义同 MQTT clean session）。

运维治理（新增）：
- 离线会话按 ``session_expiry`` 过期回收：订阅取消、积压与未确认消息
  丢弃并计入指标，内存真正释放；在线会话永不被回收。
- 单个会话的离线积压总量有上限（``backlog_cap``）：超限即丢弃该订阅
  队列中最早的消息并计数，不影响其他客户端。
- 同一身份被新连接接管（takeover）的次数按会话与全局指标计数。
"""
from __future__ import annotations

import asyncio
import threading
from collections import deque
from dataclasses import dataclass, field

from .broker import Subscription
from .messages import Message

#: 会话治理相关的全局指标键（SessionManager.metrics）
METRIC_TAKEOVERS = "takeovers"
METRIC_EXPIRED_SESSIONS = "expired_sessions"
METRIC_EXPIRED_MESSAGES = "expired_messages"
METRIC_OVERFLOW_DROPPED = "overflow_dropped"
METRIC_ACL_REVOKED_SUBSCRIPTIONS = "acl_revoked_subscriptions"
METRIC_ACL_REVOKED_MESSAGES = "acl_revoked_messages"

ALL_METRICS = (
    METRIC_TAKEOVERS,
    METRIC_EXPIRED_SESSIONS,
    METRIC_EXPIRED_MESSAGES,
    METRIC_OVERFLOW_DROPPED,
    METRIC_ACL_REVOKED_SUBSCRIPTIONS,
    METRIC_ACL_REVOKED_MESSAGES,
)


@dataclass
class Session:
    client_id: str
    subscriptions: dict[str, Subscription] = field(default_factory=dict)
    pending: deque[Message] = field(default_factory=deque)  # 已投递未确认，按投递顺序
    ready: deque[Message] = field(default_factory=deque)  # 断线时已出队未投递，重连后先于新消息投递
    connected: bool = False
    redelivered_count: int = 0
    # ---- 运维治理 ----
    disconnected_at: float | None = None  # 最近一次断开时刻（time.time()），在线为 None
    backlog_cap: int | None = None  # 离线积压总量上限（跨订阅合计），None 表示不限
    takeovers: int = 0  # 被新连接接管的次数
    overflow_dropped: int = 0  # 离线积压超限丢弃的消息数
    metrics: dict[str, int] = field(default_factory=dict)  # 共享全局指标（SessionManager 注入）

    def total_backlog(self) -> int:
        return sum(s.queue.qsize() for s in self.subscriptions.values())

    def note_enqueued_offline(self, sub: Subscription) -> None:
        """订阅队列入队回调（broker 在投递成功后调用）。

        仅对离线会话生效：积压总量超过 backlog_cap 时，丢弃该订阅队列中
        最早的消息直到回到上限以内；丢弃计数可观测。在线会话由背压与
        单队列上限约束，不在此处干预。
        """
        if self.connected or self.backlog_cap is None:
            return
        total = self.total_backlog()
        while total > self.backlog_cap:
            try:
                sub.queue.get_nowait()
            except asyncio.QueueEmpty:  # pragma: no cover - 防御
                break
            sub.dropped += 1
            self.overflow_dropped += 1
            self.metrics[METRIC_OVERFLOW_DROPPED] += 1
            total -= 1

    def stats(self) -> dict:
        return {
            "client_id": self.client_id,
            "connected": self.connected,
            "pending_unacked": len(self.pending),
            "ready": len(self.ready),
            "redelivered": self.redelivered_count,
            "takeovers": self.takeovers,
            "overflow_dropped": self.overflow_dropped,
            "disconnected_at": self.disconnected_at,
            "subscriptions": [s.stats() for s in self.subscriptions.values()],
            "backlog": self.total_backlog(),
            "dropped": sum(s.dropped for s in self.subscriptions.values()),
        }


class SessionManager:
    def __init__(self, backlog_cap: int | None = None) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, Session] = {}
        self._backlog_cap = backlog_cap
        self._metrics: dict[str, int] = {k: 0 for k in ALL_METRICS}

    @property
    def metrics(self) -> dict[str, int]:
        """全局治理指标（接管数、过期回收数、超限丢弃数等）。"""
        return self._metrics

    def _new_session(self, client_id: str) -> Session:
        return Session(
            client_id=client_id,
            backlog_cap=self._backlog_cap,
            metrics=self._metrics,
        )

    def get(self, client_id: str) -> Session | None:
        with self._lock:
            return self._sessions.get(client_id)

    def get_or_create(self, client_id: str, clean: bool = False) -> Session:
        with self._lock:
            if clean and client_id in self._sessions:
                del self._sessions[client_id]
            session = self._sessions.get(client_id)
            if session is None:
                session = self._new_session(client_id)
                self._sessions[client_id] = session
            return session

    def remove(self, client_id: str) -> Session | None:
        """移除会话并返回之（调用方负责取消其 broker 订阅）。"""
        with self._lock:
            return self._sessions.pop(client_id, None)

    def note_takeover(self, session: Session) -> None:
        session.takeovers += 1
        self._metrics[METRIC_TAKEOVERS] += 1

    def reap_expired(self, now: float, expiry_seconds: float, broker) -> list[str]:
        """回收离线时长超过 expiry_seconds 的会话。

        取消订阅、丢弃积压与未确认消息并计数；返回被回收的 client_id。
        在线会话与未超时的会话不受影响；回收不改变其他会话的任何状态。
        """
        with self._lock:
            candidates = [
                s
                for s in self._sessions.values()
                if not s.connected
                and s.disconnected_at is not None
                and now - s.disconnected_at >= expiry_seconds
            ]
        reaped: list[str] = []
        for session in candidates:
            with self._lock:
                # 复查：候选期间可能已重连或被替换
                current = self._sessions.get(session.client_id)
                if current is not session or current.connected:
                    continue
                del self._sessions[session.client_id]
            for sub in session.subscriptions.values():
                broker.unsubscribe(sub)
            discarded = (
                len(session.pending) + len(session.ready) + session.total_backlog()
            )
            self._metrics[METRIC_EXPIRED_SESSIONS] += 1
            self._metrics[METRIC_EXPIRED_MESSAGES] += discarded
            reaped.append(session.client_id)
        return reaped

    def stats(self, client_id: str) -> dict | None:
        with self._lock:
            session = self._sessions.get(client_id)
            return session.stats() if session else None

    def all_stats(self) -> list[dict]:
        with self._lock:
            return [s.stats() for s in self._sessions.values()]
