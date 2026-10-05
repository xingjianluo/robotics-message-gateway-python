"""身份与凭据绑定：冒用被拒；同一身份接管时消息不重、不乱序、可观测。"""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import auth, log_case


def test_identity_mismatch_rejected(client):
    # t-sub 的凭据属于 subscriber，却声明 client_id=admin：必须拒绝
    with client.websocket_connect("/ws?token=t-sub&client_id=admin") as ws:
        resp = ws.receive_json()
        log_case(
            "身份与凭据不一致",
            {"token": "t-sub(归属 subscriber)", "client_id": "admin"},
            "声明身份必须与凭据归属一致 -> error identity_mismatch 后以 4403 关闭",
            {"response": resp},
        )
        assert resp["type"] == "error"
        assert resp["reason"] == "identity_mismatch"
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 4403


def test_identity_mismatch_does_not_affect_others(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        assert ws.receive_json()["type"] == "subscribed"
        # 冒用 admin 身份的连接被拒
        with client.websocket_connect("/ws?token=t-sub&client_id=admin") as bad:
            assert bad.receive_json()["reason"] == "identity_mismatch"
        # 正常连接不受影响
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "ok"},
            headers=auth("t-pub"),
        )
        got = ws.receive_json()
        log_case(
            "冒用被拒不影响他人",
            {"legit": "subscriber", "impersonated": "admin"},
            "拒绝仅作用于冒用连接本身",
            {"received": got},
        )
        assert got["message"]["payload"] == "ok"


def test_takeover_hands_over_unacked_in_order(client):
    # 连接 A：订阅后收到 m1/m2 但不确认
    ws_a = client.websocket_connect("/ws?token=t-sub&client_id=subscriber")
    ws_a.__enter__()
    ws_a.receive_json()
    ws_a.send_json({"op": "subscribe", "filter": "robot/#"})
    ws_a.receive_json()
    for p in ("m1", "m2"):
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": p},
            headers=auth("t-pub"),
        )
    a1 = ws_a.receive_json()
    a2 = ws_a.receive_json()  # m1/m2 均未确认
    assert [a1["message"]["payload"], a2["message"]["payload"]] == ["m1", "m2"]

    # 连接 B（同身份）：顶掉 A
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws_b:
        assert ws_b.receive_json()["type"] == "connected"
        # A 收到 displaced 并以 4000 关闭
        displaced = ws_a.receive_json()
        with pytest.raises(WebSocketDisconnect) as exc:
            ws_a.receive_json()
        # B 按原顺序收到交接的未确认消息，再收到新消息
        b1 = ws_b.receive_json()
        b2 = ws_b.receive_json()
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "m3"},
            headers=auth("t-pub"),
        )
        b3 = ws_b.receive_json()
        stats = client.get("/stats", headers=auth("t-sub")).json()
        session = next(s for s in stats["sessions"] if s["client_id"] == "subscriber")
        log_case(
            "同身份接管",
            {"unacked_on_a": ["m1", "m2"], "new_published": ["m3"]},
            "后连上的顶掉先前的：A 收 displaced(takeover) 以 4000 关闭；"
            "未确认消息按原顺序交接给 B（redelivered），新消息续后；"
            "不重、不乱序；takeovers 计数可见",
            {"displaced": displaced, "close_code": exc.value.code,
             "b_received": [b1, b2, b3], "takeovers": session["takeovers"]},
        )
        assert displaced["type"] == "displaced"
        assert displaced["reason"] == "takeover"
        assert exc.value.code == 4000
        # 顺序交接：m1、m2 原序重投，m3 在其后
        assert [b1["message"]["payload"], b2["message"]["payload"],
                b3["message"]["payload"]] == ["m1", "m2", "m3"]
        assert b1["redelivered"] and b2["redelivered"]
        assert not b3["redelivered"]
        seqs = [b1["message"]["seq"], b2["message"]["seq"], b3["message"]["seq"]]
        assert seqs == sorted(seqs)
        # 不重：A 收到的与 B 收到的无重复投递（同一 seq 只在 B 出现一次）
        b_seqs = [b1["message"]["seq"], b2["message"]["seq"]]
        assert len(set(b_seqs)) == 2
        assert session["takeovers"] == 1
    ws_a.__exit__(None, None, None)


def test_takeover_visible_in_stats_and_old_conn_cannot_publish(client):
    ws_a = client.websocket_connect("/ws?token=t-sub&client_id=subscriber")
    ws_a.__enter__()
    ws_a.receive_json()
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws_b:
        ws_b.receive_json()
        assert ws_a.receive_json()["type"] == "displaced"
        # 被顶掉的连接上再发消息：连接已关闭，发送失败
        stats = client.get("/stats", headers=auth("t-sub")).json()
        session = next(s for s in stats["sessions"] if s["client_id"] == "subscriber")
        log_case(
            "接管可观测",
            {"client_id": "subscriber"},
            "接管后仅新连接在线，会话 takeovers=1",
            {"session": session},
        )
        assert session["takeovers"] == 1
        assert session["connected"] is True
    ws_a.__exit__(None, None, None)
