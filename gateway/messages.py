"""消息模型。payload 对外为 UTF-8 字符串。"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field

_seq_counter = itertools.count(1)


@dataclass(frozen=True)
class Message:
    topic: str
    payload: str
    retain: bool = False
    seq: int = field(default_factory=lambda: next(_seq_counter))

    def to_dict(self) -> dict:
        return {
            "topic": self.topic,
            "payload": self.payload,
            "retain": self.retain,
            "seq": self.seq,
        }

    @staticmethod
    def from_dict(data: dict) -> "Message":
        return Message(
            topic=data["topic"],
            payload=str(data["payload"]),
            retain=bool(data.get("retain", False)),
            seq=int(data["seq"]),
        )
