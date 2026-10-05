"""断线重连：未确认消息按原顺序重投，离线积压按序续投，不丢不重不乱序。"""
from __future__ import annotations

from tests.conftest import auth, log_case


def _pub(client, topic, payload):
    r = client.post(
        "/publish",
        json={"topic": topic, "payload": payload},
        headers=auth("t-pub"),
    )
    assert r.status_code == 200


def test_unacked_redelivered_in_order_then_backlog(client):
    cid = "subscriber"
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
        for p in ["m1", "m2", "m3"]:
            _pub(client, "robot/arm", p)
        got = [ws.receive_json()["message"] for _ in range(3)]
        # 只确认 m1，m2/m3 成为未确认消息
        ws.send_json({"op": "ack", "seq": got[0]["seq"]})
    # 离线期间继续发布
    _pub(client, "robot/arm", "m4")
    _pub(client, "robot/leg", "m5")

    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws:
        ws.receive_json()
        r1, r2 = ws.receive_json(), ws.receive_json()  # 重投 m2, m3
        n1, n2 = ws.receive_json(), ws.receive_json()  # 积压 m4, m5
        log_case(
            "重连按序恢复",
            {"acked": ["m1"], "unacked": ["m2", "m3"], "offline_published": ["m4", "m5"]},
            "先按原顺序重投未确认消息，再按发布顺序投递离线积压",
            {"redelivered": [r1, r2], "new": [n1, n2]},
        )
        assert [r1["message"]["payload"], r2["message"]["payload"]] == ["m2", "m3"]
        assert r1["redelivered"] and r2["redelivered"]
        assert r1["message"]["seq"] < r2["message"]["seq"]  # 原顺序
        assert [n1["message"]["payload"], n2["message"]["payload"]] == ["m4", "m5"]
        assert not n1["redelivered"] and not n2["redelivered"]
        # 无重复：重投后不会出现 m2/m3 的第二次投递
        seqs = [r1["message"]["seq"], r2["message"]["seq"],
                n1["message"]["seq"], n2["message"]["seq"]]
        assert len(set(seqs)) == 4


def test_clean_session_discards_state(client):
    cid = "subscriber"
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
        _pub(client, "robot/arm", "unacked")
        ws.receive_json()  # 不确认
    _pub(client, "robot/arm", "offline")
    # clean=true：旧会话（含订阅与未确认消息）被清除
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}&clean=true") as ws:
        hello = ws.receive_json()
        stats = client.get("/stats", headers=auth("t-sub")).json()
        session = [s for s in stats["sessions"] if s["client_id"] == cid][0]
        log_case(
            "clean 会话清除状态",
            {"client_id": cid, "clean": True},
            "clean=true 丢弃订阅与未确认消息，重连后不重投",
            {"hello": hello, "session": session},
        )
        assert session["pending_unacked"] == 0
        assert session["subscriptions"] == []


def test_reconnect_without_resubscribe_keeps_subscription(client):
    cid = "subscriber"
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
    _pub(client, "robot/arm", "while-offline")
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws:
        ws.receive_json()
        msg = ws.receive_json()  # 无需重新订阅即收到离线消息
        log_case(
            "重连无需重新订阅",
            {"client_id": cid},
            "订阅随会话持久，离线消息积压在订阅队列中",
            {"received": msg},
        )
        assert msg["message"]["payload"] == "while-offline"
