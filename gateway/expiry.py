"""会话过期回收与单客户端离线积压上限。

能力（骨架，逐步实现）：
- 长时间掉线的会话按 TTL 回收，内存真正释放，回收数与丢弃消息数可观测；
- 单客户端离线积压总量有上限，超限丢弃最旧消息并计数；
- 回收与丢弃不影响其他在线客户端。
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass


@dataclass
class ExpiryStats:
    """回收与丢弃的全局计数（骨架）。"""

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
    """后台回收任务（骨架）。"""

    def __init__(self) -> None:
        self.stats = ExpiryStats()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:  # TODO: 周期扫描回收
        return None

    async def stop(self) -> None:  # TODO: 停止后台任务
        return None
