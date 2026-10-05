"""保留消息：新订阅者收到最后一条匹配保留消息；重复订阅幂等。"""
from __future__ import annotations

from tests.conftest import auth, log_case


def _publish(client, token, topic, payload, retain=True):
    return client.post(
        "/publish",
        json={"topic": topic, "payload": payload, "retain": retain},
        headers=auth(token),
    )


def test_new_subscriber_receives_last_retained(client):
    _publish(client, "t-pub", "robot/arm", "v1")
    _publish(client, "t-pub", "robot/arm", "v2")  # 覆盖同主题保留
    _publish(client, "t-pub", "robot/leg", "l1")

    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        assert ws.receive_json()["type"] == "connected"
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        assert ws.receive_json()["type"] == "subscribed"
        m1 = ws.receive_json()["message"]
        m2 = ws.receive_json()["message"]
        log_case(
            "新订阅者收到保留消息",
            {"filter": "robot/#", "published": ["v1", "v2", "l1"]},
            "每个主题仅保留最后一条；按 seq 升序先投递",
            {"received": [m1["payload"], m2["payload"]]},
        )
        assert (m1["topic"], m1["payload"]) == ("robot/arm", "v2")
        assert (m2["topic"], m2["payload"]) == ("robot/leg", "l1")
        assert m1["seq"] < m2["seq"]


def test_retained_cleared_by_empty_payload(client):
    _publish(client, "t-pub", "robot/arm", "temp")
    _publish(client, "t-pub", "robot/arm", "")  # 空载荷清除保留
    r = client.get("/retained", params={"filter": "robot/#"}, headers=auth("t-sub"))
    log_case(
        "空载荷清除保留消息",
        {"topic": "robot/arm", "payload": ""},
        "retain=True 且 payload 为空 -> 删除该主题保留",
        {"retained": r.json()["messages"]},
    )
    assert r.status_code == 200
    assert r.json()["messages"] == []


def test_retained_visible_via_http_consistently(client):
    _publish(client, "t-pub", "robot/arm", "http-retained")
    r = client.get("/retained", params={"filter": "robot/+"}, headers=auth("t-sub"))
    messages = r.json()["messages"]
    log_case(
        "HTTP 查询保留消息",
        {"filter": "robot/+"},
        "HTTP 与 WS 共享同一 broker，结果一致",
        {"retained": messages},
    )
    assert [m["payload"] for m in messages] == ["http-retained"]


def test_duplicate_subscribe_idempotent(client):
    _publish(client, "t-pub", "robot/arm", "dup-check")
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        first = ws.receive_json()
        retained = ws.receive_json()  # 保留消息只应投递一次
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        second = ws.receive_json()
        log_case(
            "重复订阅幂等",
            {"filter": "robot/#", "subscribe_count": 2},
            "已存在的过滤器返回 duplicate=True，不重建订阅、不重投保留消息",
            {"first": first, "second": second},
        )
        assert first["duplicate"] is False
        assert second["duplicate"] is True
        assert retained["message"]["payload"] == "dup-check"
        # 重复订阅后下一条应是实时消息而非重投的保留消息
        _publish(client, "t-pub", "robot/arm", "after-dup", retain=False)
        nxt = ws.receive_json()
        assert nxt["message"]["payload"] == "after-dup"
