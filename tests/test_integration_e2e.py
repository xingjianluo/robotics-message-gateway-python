"""端到端关键组合：HTTP 保留发布 + WS 通配订阅 + 重连恢复 + 越权隔离。

全程真实 HTTP/WebSocket 传输（经 TestClient ASGI 栈），发布用 HTTP，
订阅/确认用 WS；为避免 TestClient 阻塞读占用 portal，HTTP 发布只安排在
“WS 不处于阻塞读”的窗口之间。
"""

from __future__ import annotations

import base64
import json
import logging

import pytest

from .conftest import log_case

log = logging.getLogger("case")


def _basic(cid, tok):
    raw = base64.b64encode(f"{cid}:{tok}".encode()).decode()
    return {"Authorization": f"Basic {raw}"}


def _recv(ws, kind, **matcher):
    for _ in range(200):
        f = json.loads(ws.receive_text())
        if f.get("type") == kind and all(f.get(k) == v for k, v in matcher.items()):
            return f
    raise AssertionError(f"frame type={kind} matcher={matcher} not received")


def test_retained_wildcard_reconnect_and_forbidden_combination(client):
    # 1) HTTP 越权发布被明确拒绝（403 publish_forbidden）
    forbidden = client.post(
        "/publish", json={"topic": "robots/r2/status", "payload": {"x": 1}},
        headers=_basic("pub-r1", "t-pub"),
    )
    assert forbidden.status_code == 403 and forbidden.json()["code"] == "publish_forbidden"

    # 2) HTTP 合法发布两条不同主题的保留消息
    for robot, val in [("r1", 10), ("r2", 20)]:
        r = client.post(
            "/publish",
            json={"topic": f"robots/{robot}/status", "payload": {"v": val}, "retain": True},
            headers=_basic("ops", "t-ops"),
        )
        assert r.status_code == 200

    # 3) WS 以 + 通配订阅 -> 立即收到 r1 保留（sub-status 仅匹配 robots/+/status）
    with client.websocket_connect("/ws?client_id=sub-status&token=t-sub") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "s1",
                                 "filter": "robots/+/status"}))
        assert _recv(ws, "ack", id="s1")["status"] == "ok"
        retained_r1 = _recv(ws, "message", topic="robots/r1/status")
        assert retained_r1["retained"] is True and retained_r1["payload"] == {"v": 10}

        # 确认保留消息
        ws.send_text(json.dumps({"action": "ack", "id": "a1",
                                 "delivery_id": retained_r1["delivery_id"]}))
        assert _recv(ws, "ack", id="a1")["result"] == "removed"

    # 4) 断线后，HTTP 发一条实时消息（此时 WS 已断开 -> 离线排队）
    r = client.post(
        "/publish", json={"topic": "robots/r1/status", "payload": {"v": 30}},
        headers=_basic("pub-r1", "t-pub"),
    )
    assert r.status_code == 200

    # 5) 重连（会话恢复）：未确认消息集合为空（保留已 ack），但离线实时消息须补投
    with client.websocket_connect("/ws?client_id=sub-status&token=t-sub") as ws:
        recovered = _recv(ws, "message", topic="robots/r1/status")
        log_case(
            "e2e-retained-wildcard-reconnect",
            {"http_forbidden": "robots/r2/status by pub-r1",
             "http_retained": ["robots/r1/status=10", "robots/r2/status=20"],
             "offline_live": "robots/r1/status=30"},
            ["越权 403 publish_forbidden 可区分", "+ 通配新订阅收到匹配保留",
             "断线期间的实时消息重连后补投"],
            {"forbidden": forbidden.json()["code"],
             "retained_payload": retained_r1["payload"],
             "recovered": {k: recovered[k] for k in ("seq", "payload", "retained", "duplicate")}},
        )
        assert recovered["payload"] == {"v": 30}
        assert recovered["retained"] is False  # 实时消息不带保留标记
        assert recovered["duplicate"] is False  # 从未下发过，不是重投

        # 6) 越权订阅仍被独立拒绝，连接继续可用
        ws.send_text(json.dumps({"action": "subscribe", "id": "s2",
                                 "filter": "robots/#"}))  # sub-status 只有 + 范围
        err = _recv(ws, "error", id="s2")
        assert err["code"] == "subscribe_forbidden"

    # 7) 指标里拒绝原因与投递计数可观测
    metrics = client.get("/metrics").json()
    assert metrics["rejection_reasons"]["publish_forbidden"] >= 1
    assert metrics["rejection_reasons"]["subscribe_forbidden"] >= 1
    assert metrics["published_total"] >= 3
    log_case(
        "e2e-metrics",
        {"endpoint": "GET /metrics"},
        ["拒绝按原因计数", "发布/投递计数存在"],
        {"rejection_reasons": metrics["rejection_reasons"],
         "published_total": metrics["published_total"],
         "delivered_total": metrics["delivered_total"]},
    )


def test_retained_query_endpoint_reflects_store(client):
    client.post(
        "/publish", json={"topic": "robots/r1/status", "payload": {"v": 1}, "retain": True},
        headers=_basic("ops", "t-ops"),
    )
    r = client.get("/retained", params={"filter": "robots/+/status"})
    assert r.status_code == 200
    topics = [m["topic"] for m in r.json()["messages"]]
    assert topics == ["robots/r1/status"]
    log_case("retained-http-query", {"filter": "robots/+/status"},
             ["通配查询返回匹配保留消息"], {"topics": topics})
