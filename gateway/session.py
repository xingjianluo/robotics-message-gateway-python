"""客户端会话：断线重连后按原顺序恢复未确认消息。

会话模型：
- 会话按 client_id 持久持有 broker 订阅，断线不取消订阅，
  离线期间消息积压在订阅的有界队列中（容量由 Broker.max_queue 决定），
  溢出丢弃计数可观测。
- 已投递未确认的消息保存在 ``pending`` 中；重连后先按原顺序重投
  pending，再继续消费订阅队列，保证不丢、不重、不乱序。
- ``clean=True`` 的连接会清除旧会话（语义同 MQTT clean session）。
- ``active`` 指向当前在线连接（同一身份同一时刻至多一条，
  新连接接管时旧连接被顶掉，见 app.py 的接管语义）。
"""
from __future__ import annotations

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .broker import Subscription
from .messages import Message


@dataclass
class Session:
    client_id: str
    subscriptions: dict[str, Subscription] = field(default_factory=dict)
    pending: deque[Message] = field(default_factory=deque)  # 已投递未确认，按投递顺序
    ready: deque[Message] = field(default_factory=deque)  # 断线时已出队未投递，重连后先于新消息投递
    connected: bool = False
    redelivered_count: int = 0
    active: Any = None  # 当前在线连接（app.Connection），离线为 None
    connect_lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)
    takeovers: int = 0  # 本身份被新连接接管的次数
    revoked_subscriptions: int = 0  # 权限收紧后被追溯收回的订阅数
    backlog_dropped: int = 0  # 离线积压超限被丢弃的消息数
    last_disconnect: float = 0.0  # 最近一次断开的时间戳（过期回收依据）

    def note_disconnected(self) -> None:
        self.connected = False
        self.active = None
        self.last_disconnect = time.monotonic()

    def stats(self) -> dict:
        return {
            "client_id": self.client_id,
            "connected": self.connected,
            "pending_unacked": len(self.pending),
            "ready": len(self.ready),
            "redelivered": self.redelivered_count,
            "takeovers": self.takeovers,
            "revoked_subscriptions": self.revoked_subscriptions,
            "backlog_dropped": self.backlog_dropped,
            "subscriptions": [s.stats() for s in self.subscriptions.values()],
            "backlog": sum(s.queue.qsize() for s in self.subscriptions.values()),
            "dropped": sum(s.dropped for s in self.subscriptions.values()),
        }


class SessionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._sessions: dict[str, Session] = {}

    def get(self, client_id: str) -> Session | None:
        with self._lock:
            return self._sessions.get(client_id)

    def get_or_create(self, client_id: str, clean: bool = False) -> Session:
        with self._lock:
            if clean and client_id in self._sessions:
                del self._sessions[client_id]
            session = self._sessions.get(client_id)
            if session is None:
                session = Session(client_id=client_id)
                self._sessions[client_id] = session
            return session

    def remove(self, client_id: str) -> Session | None:
        with self._lock:
            return self._sessions.pop(client_id, None)

    def all_sessions(self) -> list[Session]:
        with self._lock:
            return list(self._sessions.values())

    def stats(self, client_id: str) -> dict | None:
        with self._lock:
            session = self._sessions.get(client_id)
            return session.stats() if session else None

    def all_stats(self) -> list[dict]:
        with self._lock:
            return [s.stats() for s in self._sessions.values()]
