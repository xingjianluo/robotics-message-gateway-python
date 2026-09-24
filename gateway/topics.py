"""主题校验与通配匹配。

判定依据（与 MQTT 主题语义对齐）：

- 层级以 ``/`` 分隔；不允许空层级（前导/尾随 ``/``、连续 ``//`` 均非法）。
- 发布主题不允许通配符；``+`` 匹配且仅匹配一个层级。
- ``#`` 只能独占最后一个层级，匹配其父层级及其下任意多层
  （即 ``a/#`` 同时匹配 ``a``、``a/x``、``a/x/y``）。
- 字符层面拒绝 C0 控制字符（含 NUL），``+``/``#`` 只能独占整层。
"""

from __future__ import annotations

from .errors import ErrorCode, GatewayError
from .models import MAX_TOPIC_LENGTH

_WILDCARDS = ("+", "#")


def split_topic(topic: str) -> list[str]:
    """按 '/' 切分主题层级。"""
    return topic.split("/")


def validate_publish_topic(topic: str) -> list[str]:
    """校验发布主题，返回层级；非法时抛出带可区分错误码的异常。"""
    if not isinstance(topic, str):
        raise GatewayError(ErrorCode.EMPTY_TOPIC, "topic must be a string")
    if len(topic) == 0:
        raise GatewayError(ErrorCode.EMPTY_TOPIC, "topic must not be empty")
    if len(topic) > MAX_TOPIC_LENGTH:
        raise GatewayError(ErrorCode.TOPIC_TOO_LONG, f"topic length {len(topic)} > {MAX_TOPIC_LENGTH}")
    levels = split_topic(topic)
    for level in levels:
        if level == "":
            raise GatewayError(ErrorCode.EMPTY_LEVEL, f"empty level in topic {topic!r}")
        if level in _WILDCARDS:
            # 发布目标不允许是通配层
            raise GatewayError(ErrorCode.BAD_WILDCARD_POSITION, f"wildcard level {level!r} not allowed in publish topic")
        if "+" in level or "#" in level:
            raise GatewayError(ErrorCode.ILLEGAL_CHARACTER, "wildcard characters must occupy a whole level")
        if _has_control_char(level):
            raise GatewayError(ErrorCode.ILLEGAL_CHARACTER, "control characters are not allowed in topic")
    return levels


def validate_subscribe_filter(topic_filter: str) -> list[str]:
    """校验订阅过滤器（允许 + / #），返回层级。"""
    if not isinstance(topic_filter, str):
        raise GatewayError(ErrorCode.EMPTY_TOPIC, "filter must be a string")
    if len(topic_filter) == 0:
        raise GatewayError(ErrorCode.EMPTY_TOPIC, "filter must not be empty")
    if len(topic_filter) > MAX_TOPIC_LENGTH:
        raise GatewayError(ErrorCode.TOPIC_TOO_LONG, f"filter length {len(topic_filter)} > {MAX_TOPIC_LENGTH}")
    levels = split_topic(topic_filter)
    for index, level in enumerate(levels):
        if level == "":
            raise GatewayError(ErrorCode.EMPTY_LEVEL, f"empty level in filter {topic_filter!r}")
        if _has_control_char(level):
            raise GatewayError(ErrorCode.ILLEGAL_CHARACTER, "control characters are not allowed in filter")
        if "#" in level:
            if level != "#" or index != len(levels) - 1:
                raise GatewayError(
                    ErrorCode.BAD_WILDCARD_POSITION,
                    "'#' must be the last level and occupy the whole level",
                )
        if "+" in level and level != "+":
            raise GatewayError(ErrorCode.ILLEGAL_CHARACTER, "'+' must occupy a whole level")
    return levels


def _has_control_char(level: str) -> bool:
    return any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in level)


def matches(filter_levels: list[str], topic_levels: list[str]) -> bool:
    """判断已校验的过滤器层级是否匹配已校验的主题层级。"""
    for i, fl in enumerate(filter_levels):
        if fl == "#":
            # '#' 匹配父层及以下任意多层；走到这里说明前缀已全部相等
            return True
        if i >= len(topic_levels):
            return False
        if fl == "+":
            continue
        if fl != topic_levels[i]:
            return False
    return len(filter_levels) == len(topic_levels)
