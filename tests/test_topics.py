"""主题层级、通配匹配与非法主题错误码区分。"""

from __future__ import annotations

import logging

import pytest

from gateway.errors import ErrorCode, GatewayError
from gateway.topics import matches, validate_publish_topic, validate_subscribe_filter

from .conftest import log_case

log = logging.getLogger("case")


@pytest.mark.parametrize(
    "topic_filter,topic,expected",
    [
        ("robots/r1/status", "robots/r1/status", True),
        ("robots/r1/status", "robots/r2/status", False),
        ("robots/+/status", "robots/r1/status", True),
        ("robots/+/status", "robots/r1/telemetry", False),
        ("robots/+", "robots/r1/status", False),       # + 只匹配单层
        ("robots/#", "robots", True),                  # # 匹配父层自身
        ("robots/#", "robots/r1/status", True),
        ("+/status/+", "r1/status/up", True),
        ("+/status/+", "r1/status", False),
        ("#", "anything/here/now", True),
        ("a/b/#", "a/b", True),
        ("a/b/#", "a/b/c/d", True),
        ("a/b/#", "a/c", False),
    ],
)
def test_wildcard_matching(topic_filter, topic, expected):
    basis = matches(validate_subscribe_filter(topic_filter),
                    validate_publish_topic(topic))
    log_case(
        f"match {topic_filter} ~ {topic}",
        {"filter": topic_filter, "topic": topic},
        ["层级切分", "+匹配单层", "#匹配父层及以下"],
        {"match": basis},
    )
    assert basis is expected


@pytest.mark.parametrize(
    "raw,kind,code",
    [
        ("", "publish", ErrorCode.EMPTY_TOPIC),
        ("", "subscribe", ErrorCode.EMPTY_TOPIC),
        ("a//b", "publish", ErrorCode.EMPTY_LEVEL),
        ("a/", "publish", ErrorCode.EMPTY_LEVEL),
        ("/a", "subscribe", ErrorCode.EMPTY_LEVEL),
        ("x" * 2000, "publish", ErrorCode.TOPIC_TOO_LONG),
        ("x" * 2000, "subscribe", ErrorCode.TOPIC_TOO_LONG),
        ("a\u0000b", "publish", ErrorCode.ILLEGAL_CHARACTER),
        ("a+b/c", "subscribe", ErrorCode.ILLEGAL_CHARACTER),
    ],
)
def test_invalid_topics_distinguishable(raw, kind, code):
    validator = validate_publish_topic if kind == "publish" else validate_subscribe_filter
    with pytest.raises(GatewayError) as exc:
        validator(raw)
    log_case(
        f"invalid {kind}",
        {"raw": raw},
        [f"期望错误码 {code.value}"],
        {"got": exc.value.code.value},
    )
    assert exc.value.code is code


@pytest.mark.parametrize(
    "raw,kind,code",
    [
        ("a/+", "publish", ErrorCode.BAD_WILDCARD_POSITION),  # 发布禁用通配
        ("a/#", "publish", ErrorCode.BAD_WILDCARD_POSITION),
        ("a/#/b", "subscribe", ErrorCode.BAD_WILDCARD_POSITION),  # # 不在末尾
        ("a/#/b", "publish", ErrorCode.BAD_WILDCARD_POSITION),
    ],
)
def test_wildcard_position_rules(raw, kind, code):
    validator = validate_publish_topic if kind == "publish" else validate_subscribe_filter
    with pytest.raises(GatewayError) as exc:
        validator(raw)
    log_case(
        f"wildcard-position {kind}",
        {"raw": raw},
        ["#必须独占末层", "发布主题不允许通配"],
        {"got": exc.value.code.value},
    )
    assert exc.value.code is code
