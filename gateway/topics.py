"""主题层级、校验与通配匹配。

规则（类 MQTT）：
- 主题以 '/' 分层，如 ``robot/arm/joint1``。
- 发布主题不允许包含通配符 ``+`` / ``#``，不允许空层级。
- 订阅过滤器允许 ``+``（单层通配）与 ``#``（多层通配，必须独占最后一层）。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

_MAX_LEN = 256


class TopicRejectReason(str, Enum):
    """非法主题的可区分原因。"""

    EMPTY = "empty"
    TOO_LONG = "too_long"
    EMPTY_LEVEL = "empty_level"
    WILDCARD_IN_PUBLISH = "wildcard_in_publish"
    HASH_NOT_LAST = "hash_not_last"
    INVALID_CHAR = "invalid_char"


@dataclass(frozen=True)
class TopicVerdict:
    ok: bool
    reason: TopicRejectReason | None = None


def _check_common(name: str) -> TopicVerdict | None:
    if not name:
        return TopicVerdict(False, TopicRejectReason.EMPTY)
    if len(name) > _MAX_LEN:
        return TopicVerdict(False, TopicRejectReason.TOO_LONG)
    for ch in name:
        if ord(ch) < 0x21 or ord(ch) > 0x7E:
            return TopicVerdict(False, TopicRejectReason.INVALID_CHAR)
    if "//" in name or name.startswith("/") or name.endswith("/"):
        return TopicVerdict(False, TopicRejectReason.EMPTY_LEVEL)
    return None


def validate_topic(topic: str) -> TopicVerdict:
    """校验发布主题（不允许通配符）。"""
    if (bad := _check_common(topic)) is not None:
        return bad
    if "+" in topic or "#" in topic:
        return TopicVerdict(False, TopicRejectReason.WILDCARD_IN_PUBLISH)
    return TopicVerdict(True)


def validate_filter(topic_filter: str) -> TopicVerdict:
    """校验订阅过滤器（允许通配符）。"""
    if (bad := _check_common(topic_filter)) is not None:
        return bad
    levels = topic_filter.split("/")
    for i, level in enumerate(levels):
        if "#" in level:
            if level != "#" or i != len(levels) - 1:
                return TopicVerdict(False, TopicRejectReason.HASH_NOT_LAST)
        if "+" in level and level != "+":
            return TopicVerdict(False, TopicRejectReason.INVALID_CHAR)
    return TopicVerdict(True)


def match(topic_filter: str, topic: str) -> bool:
    """判断订阅过滤器是否匹配发布主题。假定两者均已通过校验。"""
    f_levels = topic_filter.split("/")
    t_levels = topic.split("/")
    for i, f in enumerate(f_levels):
        if f == "#":
            return True
        if i >= len(t_levels):
            return False
        if f != "+" and f != t_levels[i]:
            return False
    return len(f_levels) == len(t_levels)


def covers(rule_filter: str, requested_filter: str) -> bool:
    """判断权限规则的过滤器是否完全覆盖订阅请求的过滤器。

    即：任何匹配 requested_filter 的主题必然匹配 rule_filter。
    """
    r_levels = rule_filter.split("/")
    q_levels = requested_filter.split("/")
    for i, rl in enumerate(r_levels):
        if rl == "#":
            return True
        if i >= len(q_levels):
            return False
        ql = q_levels[i]
        if rl == "+":
            if ql == "#":  # 单层通配无法覆盖多层通配
                return False
            continue
        if ql in ("+", "#"):  # 字面层级无法覆盖通配
            return False
        if rl != ql:
            return False
    return len(r_levels) == len(q_levels)
