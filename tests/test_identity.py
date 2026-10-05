"""身份与凭据绑定：冒用身份被拒；同身份接管时不丢、不重、不乱序。"""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import auth, log_case


def _pub(client, payload, topic="robot/arm"):
    r = client.post(
        "/publish", json={"topic": topic, "payload": payload}, headers=auth("t-pub")
    )
    assert r.status_code == 200


def test_declared_identity_must_match_credentials(client):
    # subscriber 的凭据试图声明 admin 的身份 -> 拒绝
    with client.websocket_connect("/ws?token=t-sub&client_id=admin") as ws:
        resp = ws.receive_json()
        code = None
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        code = exc.value.code
        log_case(
            "身份与凭据不一致被拒",
            {"token": "t-sub (subscriber)", "client_id": "admin"},
            "声明的 client_id 与凭据归属不一致 -> identity_mismatch + 4403",
            {"response": resp, "close_code": code},
        )
        assert resp["type"] == "error" and resp["reason"] == "identity_mismatch"
        assert code == 4403


def test_identity_mismatch_does_not_affect_other_connections(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as good:
        assert good.receive_json()["type"] == "connected"
        good.send_json({"op": "subscribe", "filter": "robot/#"})
        assert good.receive_json()["type"] == "subscribed"
        # 另一条连接冒用身份被拒
        with client.websocket_connect("/ws?token=t-pub&client_id=subscriber") as bad:
            resp = bad.receive_json()
            assert resp["reason"] == "identity_mismatch"
            with pytest.raises(WebSocketDisconnect):
                bad.receive_json()
        # 正常连接不受影响，消息照常投递
        _pub(client, "still-ok")
        msg = good.receive_json()
        log_case(
            "冒用不影响其他连接",
            {"legit": "subscriber", "forged": "publisher 声明 subscriber"},
            "冒用连接被拒并关闭；既有连接的订阅与投递不受影响",
            {"forged_response": resp, "received": msg},
        )
        assert msg["message"]["payload"] == "still-ok"


def test_takeover_hands_over_unacked_in_order(client):
    cid = "subscriber"
    # 第一条连接：订阅并留下两条未确认消息
    ws_a = client.websocket_connect(f"/ws?token=t-sub&client_id={cid}")
    ws_a.__enter__()
    assert ws_a.receive_json()["type"] == "connected"
    ws_a.send_json({"op": "subscribe", "filter": "robot/#"})
    assert ws_a.receive_json()["type"] == "subscribed"
    for p in ["m1", "m2", "m3"]:
        _pub(client, p)
    got = [ws_a.receive_json()["message"] for _ in range(3)]
    ws_a.send_json({"op": "ack", "seq": got[0]["seq"]})  # 只确认 m1

    # 同一身份再开一条连接：接管
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws_b:
        assert ws_b.receive_json()["type"] == "connected"
        # 旧连接收到接管通知并以 4409 关闭
        notice = ws_a.receive_json()
        with pytest.raises(WebSocketDisconnect) as exc:
            ws_a.receive_json()
        # 接管后发布新消息
        _pub(client, "m4")
        r1 = ws_b.receive_json()
        r2 = ws_b.receive_json()
        n1 = ws_b.receive_json()
        stats = client.get("/stats", headers=auth("t-sub")).json()
        session = [s for s in stats["sessions"] if s["client_id"] == cid][0]
        log_case(
            "同身份接管",
            {"unacked": ["m2", "m3"], "after_takeover": ["m4"]},
            "后连上的顶掉先前的；未确认消息按原顺序由新连接重投，"
            "再投新消息；不重、不乱序；接管次数计入指标",
            {
                "old_conn_notice": notice,
                "old_conn_close_code": exc.value.code,
                "redelivered": [r1, r2],
                "new": n1,
                "takeovers": session["takeovers"],
                "gateway_takeovers": stats["gateway"]["takeovers"],
            },
        )
        assert notice["reason"] == "session_taken_over"
        assert exc.value.code == 4409
        # 未确认消息按原顺序交接（redelivered），随后才是新消息
        assert [r1["message"]["payload"], r2["message"]["payload"]] == ["m2", "m3"]
        assert r1["redelivered"] and r2["redelivered"]
        assert r1["message"]["seq"] < r2["message"]["seq"]
        assert n1["message"]["payload"] == "m4" and not n1["redelivered"]
        # 不重：四条消息 seq 唯一
        seqs = [r1["message"]["seq"], r2["message"]["seq"], n1["message"]["seq"]]
        assert len(set(seqs)) == 3
        # 接管可观测
        assert session["takeovers"] == 1
        assert stats["gateway"]["takeovers"] == 1
    ws_a.__exit__(None, None, None)


def test_takeover_offline_backlog_continues_in_order(client):
    cid = "subscriber"
    with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws_a:
        ws_a.receive_json()
        ws_a.send_json({"op": "subscribe", "filter": "robot/#"})
        ws_a.receive_json()
        _pub(client, "before-takeover")
        first = ws_a.receive_json()["message"]  # 不确认，留在 pending
        # 接管瞬间再发布一条，应在新连接上按序续投
        with client.websocket_connect(f"/ws?token=t-sub&client_id={cid}") as ws_b:
            ws_b.receive_json()
            _pub(client, "after-takeover")
            r = ws_b.receive_json()
            n = ws_b.receive_json()
            log_case(
                "接管后按序续投",
                {"pending": ["before-takeover"], "new": ["after-takeover"]},
                "新连接先重投未确认消息，再投新消息",
                {"redelivered": r, "new": n},
            )
            assert r["message"]["payload"] == "before-takeover" and r["redelivered"]
            assert r["message"]["seq"] == first["seq"]
            assert n["message"]["payload"] == "after-takeover"
