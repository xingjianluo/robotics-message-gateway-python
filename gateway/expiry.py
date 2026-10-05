"""会话过期回收与单客户端离线积压上限。

- 过期回收：断线超过 ``session_ttl`` 秒的会话被整体回收（订阅从 broker
  摘除、会话对象释放，内存随之回收），回收会话数与被丢弃的消息数
  计入指标。``session_ttl <= 0`` 表示不过期。
- 积压上限：离线会话的所有订阅积压总量超过 ``max_offline_backlog`` 时，
  从积压最长的订阅队列头部丢弃最旧消息直到回落到上限以内，丢弃数
  计入会话与全局指标。``max_offline_backlog <= 0`` 表示不限。
- 只处理离线会话：在线会话由背压与单订阅有界队列约束，不受本模块影响；
  回收与丢弃均按会话独立进行，不影响其他客户端。
"""
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from .broker import Broker
from .session import Session, SessionManager


@dataclass
class ExpiryStats:
    """回收与丢弃的全局计数。"""

    sessions_expired: int = 0
    messages_expired: int = 0
    backlog_dropped: int = 0

    def to_dict(self) -> dict:
        return {
            "sessions_expired": self.sessions_expired,
            "messages_expired": self.messages_expired,
            "backlog_dropped": self.backlog_dropped,
        }


class SessionReaper:
    """后台回收任务：周期扫描，过期回收 + 离线积压上限。"""

    def __init__(
        self,
        sessions: SessionManager,
        broker: Broker,
        session_ttl: float = 0.0,
        max_offline_backlog: int = 0,
        interval: float = 1.0,
    ) -> None:
        self.sessions = sessions
        self.broker = broker
        self.session_ttl = session_ttl
        self.max_offline_backlog = max_offline_backlog
        self.interval = interval
        self.stats = ExpiryStats()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self.session_ttl <= 0 and self.max_offline_backlog <= 0:
            return  # 未启用任何回收策略
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            self.sweep()

    def sweep(self) -> None:
        """执行一轮回收与积压修剪（同步、可独立调用以便测试）。"""
        now = time.monotonic()
        for session in self.sessions.all_sessions():
            if session.connected:
                continue  # 在线会话由背压约束，不在此处处理
            if (
                self.session_ttl > 0
                and session.last_disconnect > 0
                and now - session.last_disconnect > self.session_ttl
            ):
                self._expire(session)
            else:
                self._enforce_backlog_cap(session)

    def _expire(self, session: Session) -> None:
        """回收过期会话：摘除订阅、释放会话，统计被丢弃的消息数。"""
        if self.sessions.remove(session.client_id) is None:
            return  # 已被并发回收
        discarded = len(session.pending) + len(session.ready)
        for sub in session.subscriptions.values():
            discarded += sub.queue.qsize()  # 队列中未投递消息
            self.broker.unsubscribe(sub)
        session.subscriptions.clear()
        session.pending.clear()
        session.ready.clear()
        self.stats.sessions_expired += 1
        self.stats.messages_expired += discarded

    def _enforce_backlog_cap(self, session: Session) -> None:
        """离线积压总量超限：从最长的订阅队列头部丢弃最旧消息。"""
        cap = self.max_offline_backlog
        if cap <= 0:
            return
        subs = list(session.subscriptions.values())
        total = sum(s.queue.qsize() for s in subs)
        while total > cap:
            longest = max(subs, key=lambda s: s.queue.qsize())
            try:
                longest.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            longest.dropped += 1
            session.backlog_dropped += 1
            self.stats.backlog_dropped += 1
            total -= 1
