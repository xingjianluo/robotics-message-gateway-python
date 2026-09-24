"""保留消息与新订阅者语义（直接驱动 Broker）。"""

from __future__ import annotations

import logging

import pytest

from .conftest import log_case, make_credentials
from gateway.auth import Authenticator
from gateway.broker import Broker
from gateway.errors import ErrorCode, GatewayError

log = logging.getLogger("case")


@pytest.fixture()
def broker():
    return Broker(Authenticator(make_credentials()))


def _drain_seqs(broker, cid):
    return [d.seq for d in broker.registry.get(cid).drain_pending()]


def test_new_subscriber_gets_last_matching_retained(broker):
    broker.publish("robots/r1/status", {"v": 1}, "pub-r1", retained=True)
    broker.publish("robots/r1/status", {"v": 2}, "pub-r1", retained=True)
    broker.publish("robots/r1/status", {"v": 3}, "pub-r1", retained=True)
    broker.connect("sub-status")
    result = broker.subscribe("sub-status", "robots/+/status")
    got = broker.registry.get("sub-status").drain_pending()
    log_case(
        "retained-last-of-topic",
        {"published": [1, 2, 3], "subscribe": "robots/+/status"},
        ["每个具体主题仅保留最后一条", "+ 匹配 r1"],
        {"retained_matched": result["retained_matched"],
         "payloads": [d.payload for d in got]},
    )
    assert result["retained_matched"] == 1
    assert len(got) == 1 and got[0].payload == {"v": 3} and got[0].retained is True


def test_wildcard_subscription_gets_one_per_topic_sorted(broker):
    broker.publish("robots/r2/status", {"r": 2}, "ops", retained=True)
    broker.publish("robots/r1/status", {"r": 1}, "ops", retained=True)
    broker.publish("robots/r3/status", {"r": 3}, "ops", retained=True)
    broker.connect("sub-all")
    broker.subscribe("sub-all", "robots/#")
    got = broker.registry.get("sub-all").drain_pending()
    order = [(d.topic, d.payload) for d in got]
    log_case(
        "retained-one-per-topic",
        {"topics": ["r2", "r1", "r3"], "filter": "robots/#"},
        ["每主题一条", "按主题名字典序保证确定性"],
        {"order": order},
    )
    assert [d.topic for d in got] == [
        "robots/r1/status", "robots/r2/status", "robots/r3/status"
    ]


def test_retained_tombstone(broker):
    broker.publish("robots/r1/status", {"v": 1}, "pub-r1", retained=True)
    assert broker.retained_snapshot()  # 有保留
    # payload=None 的保留发布即清除墓碑
    broker.publish("robots/r1/status", None, "ops", retained=True)
    broker.connect("sub-status")
    broker.subscribe("sub-status", "robots/+/status")
    got = broker.registry.get("sub-status").drain_pending()
    log_case(
        "retained-tombstone",
        {"retain-publish": [{"v": 1}, None]},
        ["payload=null 的保留消息删除保留"],
        {"retained_store": broker.retained_snapshot(), "delivered": len(got)},
    )
    assert broker.retained_snapshot() == {}
    assert got == []


def test_duplicate_subscription_no_duplicate_retained(broker):
    broker.publish("robots/r1/status", {"v": 1}, "pub-r1", retained=True)
    broker.connect("sub-status")
    first = broker.subscribe("sub-status", "robots/+/status")
    assert [d.payload for d in broker.registry.get("sub-status").drain_pending()] == [{"v": 1}]
    second = broker.subscribe("sub-status", "robots/+/status")
    extra = broker.registry.get("sub-status").drain_pending()
    log_case(
        "duplicate-subscription",
        {"filter": "robots/+/status", "times": 2},
        ["重复订阅不新增订阅、不重复投保留消息"],
        {"first": first, "second": second, "extra_deliveries": len(extra)},
    )
    assert first["is_new"] is True
    assert second["is_new"] is False and second["retained_matched"] == 0
    assert extra == []


def test_realtime_delivery_is_not_flagged_retained(broker):
    broker.publish("robots/r1/status", {"v": 1}, "pub-r1", retained=True)
    broker.connect("sub-all")
    broker.subscribe("sub-all", "robots/#")
    retained = broker.registry.get("sub-all").drain_pending()
    broker.publish("robots/r1/status", {"v": 2}, "pub-r1")
    live = broker.registry.get("sub-all").drain_pending()
    log_case(
        "retained-vs-live-flag",
        {"retained_pub": 1, "live_pub": 2},
        ["实时扇出的消息 retained 标记为 False"],
        {"retained_flags": [d.retained for d in retained + live]},
    )
    assert retained[0].retained is True
    assert live[0].retained is False and live[0].seq > retained[0].seq
