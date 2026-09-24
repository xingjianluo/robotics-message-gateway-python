"""可观测指标：计数器、按客户端积压、拒绝/丢弃原因分布。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field


@dataclass(slots=True)
class Metrics:
    published_total: int = 0
    delivered_total: int = 0
    retained_total: int = 0
    rejected_total: int = 0
    dropped_total: int = 0
    reconnect_recovered_total: int = 0
    duplicate_acks_total: int = 0
    by_reason: dict[str, int] = field(default_factory=dict)
    # client_id -> 当前积压（排队 + 在途）
    backlog: dict[str, int] = field(default_factory=dict)
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def inc(self, name: str, amount: int = 1) -> None:
        with self._lock:
            setattr(self, name, getattr(self, name) + amount)

    def reject(self, reason: str) -> None:
        with self._lock:
            self.rejected_total += 1
            self.by_reason[reason] = self.by_reason.get(reason, 0) + 1

    def drop(self, client_id: str, reason: str) -> None:
        with self._lock:
            self.dropped_total += 1
            key = f"drop:{reason}"
            self.by_reason[key] = self.by_reason.get(key, 0) + 1

    def set_backlog(self, client_id: str, value: int) -> None:
        with self._lock:
            if value <= 0:
                self.backlog.pop(client_id, None)
            else:
                self.backlog[client_id] = value

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "published_total": self.published_total,
                "delivered_total": self.delivered_total,
                "retained_total": self.retained_total,
                "rejected_total": self.rejected_total,
                "dropped_total": self.dropped_total,
                "reconnect_recovered_total": self.reconnect_recovered_total,
                "duplicate_acks_total": self.duplicate_acks_total,
                "rejection_reasons": dict(self.by_reason),
                "backlog": dict(self.backlog),
            }
