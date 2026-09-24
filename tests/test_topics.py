"""通配匹配与主题校验。"""
from __future__ import annotations

import pytest

from gateway.topics import (
    TopicRejectReason,
    covers,
    match,
    validate_filter,
    validate_topic,
)
from tests.conftest import log_case


@pytest.mark.parametrize(
    "filt,topic,expected",
    [
        ("robot/#", "robot/arm", True),
        ("robot/#", "robot/arm/joint1", True),
        ("#", "anything/at/all", True),
        ("robot/+", "robot/arm", True),
        ("robot/+", "robot/arm/joint1", False),
        ("robot/+/joint1", "robot/arm/joint1", True),
        ("robot/arm", "robot/arm", True),
        ("robot/arm", "robot/leg", False),
        ("robot/arm/#", "robot/arm", True),
        ("robot/arm", "robot/arm/joint1", False),
    ],
)
def test_wildcard_match(filt, topic, expected):
    result = match(filt, topic)
    log_case(
        "通配匹配",
        {"filter": filt, "topic": topic},
        "按层比对：'+'匹配单层，'#'匹配剩余所有层",
        {"match": result, "expected": expected},
    )
    assert result is expected


@pytest.mark.parametrize(
    "topic,reason",
    [
        ("", TopicRejectReason.EMPTY),
        ("robot//arm", TopicRejectReason.EMPTY_LEVEL),
        ("/robot", TopicRejectReason.EMPTY_LEVEL),
        ("robot/+", TopicRejectReason.WILDCARD_IN_PUBLISH),
        ("robot/#", TopicRejectReason.WILDCARD_IN_PUBLISH),
        ("ro bot", TopicRejectReason.INVALID_CHAR),
    ],
)
def test_invalid_publish_topic_rejected_with_reason(topic, reason):
    verdict = validate_topic(topic)
    log_case(
        "非法发布主题",
        {"topic": topic},
        "发布主题禁止通配符/空层级/非法字符",
        {"ok": verdict.ok, "reason": verdict.reason},
    )
    assert not verdict.ok
    assert verdict.reason is reason  # 失败原因可区分


@pytest.mark.parametrize(
    "filt,reason",
    [
        ("robot/#/arm", TopicRejectReason.HASH_NOT_LAST),
        ("robot/ar#", TopicRejectReason.HASH_NOT_LAST),
        ("robot/a+", TopicRejectReason.INVALID_CHAR),
    ],
)
def test_invalid_filter_rejected_with_reason(filt, reason):
    verdict = validate_filter(filt)
    log_case(
        "非法订阅过滤器",
        {"filter": filt},
        "'#'必须独占最后一层；'+'必须独占一层",
        {"ok": verdict.ok, "reason": verdict.reason},
    )
    assert not verdict.ok
    assert verdict.reason is reason


@pytest.mark.parametrize(
    "rule,req,expected",
    [
        ("robot/#", "robot/arm", True),
        ("robot/#", "robot/#", True),
        ("robot/+", "robot/arm", True),
        ("robot/+", "robot/#", False),
        ("robot/arm", "robot/+", False),
        ("robot/arm", "robot/arm", True),
    ],
)
def test_acl_covers(rule, req, expected):
    result = covers(rule, req)
    log_case(
        "ACL 覆盖判定",
        {"rule_filter": rule, "requested_filter": req},
        "规则过滤器必须覆盖请求过滤器的全部主题空间",
        {"covers": result, "expected": expected},
    )
    assert result is expected
