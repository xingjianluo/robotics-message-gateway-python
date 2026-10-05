"""运行期账号与权限治理：不重启新增/注销账号、调整主题范围并追溯生效。"""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import auth, log_case

ADMIN = auth("t-admin")


def _make_robot_client(client, cid="robot-x", token="t-x", publish=None, subscribe=None):
    r = client.post(
        "/admin/clients", json={"client_id": cid, "token": token}, headers=ADMIN
    )
    assert r.status_code == 201
    r = client.put(
        f"/admin/clients/{cid}/acl",
        json={"publish": publish or [], "subscribe": subscribe or []},
        headers=ADMIN,
    )
    assert r.status_code == 200


def test_admin_auth_results_distinguishable(client):
    body = {"client_id": "c1", "token": "t-c1"}
    r_no_token = client.post("/admin/clients", json=body)
    r_bad_token = client.post("/admin/clients", json=body, headers=auth("no-such"))
    r_client_token = client.post("/admin/clients", json=body, headers=auth("t-sub"))
    r_bad_body = client.post(
        "/admin/clients", json={"client_id": "", "token": "t"}, headers=ADMIN
    )
    log_case(
        "管理接口鉴权可区分",
        {"cases": ["无 token", "未知 token", "普通客户端 token", "非法请求体"]},
        "401 invalid_credentials / 403 not_allowed / 400 invalid_request 互相区分",
        {
            "no_token": r_no_token.status_code,
            "bad_token": r_bad_token.status_code,
            "client_token": (r_client_token.status_code, r_client_token.json()),
            "bad_body": (r_bad_body.status_code, r_bad_body.json()),
        },
    )
    assert r_no_token.status_code == 401
    assert r_bad_token.status_code == 401
    assert r_client_token.status_code == 403
    assert r_client_token.json()["reason"] == "not_allowed"
    assert r_bad_body.status_code == 400
    assert r_bad_body.json()["reason"] == "invalid_request"


def test_create_client_and_use_immediately(client):
    _make_robot_client(client, subscribe=["robot/#"])
    # 新账号立即可连接、可订阅；新 token 立即生效
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws:
        hello = ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        resp = ws.receive_json()
        clients = client.get("/admin/clients", headers=ADMIN).json()["clients"]
        log_case(
            "运行期新增账号",
            {"client_id": "robot-x", "subscribe": ["robot/#"]},
            "不重启即可连接并使用新权限",
            {"hello": hello, "subscribe": resp, "clients": clients},
        )
        assert hello["type"] == "connected" and hello["client_id"] == "robot-x"
        assert resp["type"] == "subscribed"
        robot_x = [c for c in clients if c["client_id"] == "robot-x"][0]
        assert robot_x["subscribe"] == ["robot/#"]


def test_admin_request_validation(client):
    _make_robot_client(client)
    r_unknown = client.put(
        "/admin/clients/ghost/acl", json={"subscribe": []}, headers=ADMIN
    )
    r_bad_filter = client.put(
        "/admin/clients/robot-x/acl",
        json={"subscribe": ["robot/#/bad"]},
        headers=ADMIN,
    )
    r_bad_shape = client.put(
        "/admin/clients/robot-x/acl", json={"subscribe": "robot/#"}, headers=ADMIN
    )
    r_dup_token = client.post(
        "/admin/clients",
        json={"client_id": "robot-y", "token": "t-x"},
        headers=ADMIN,
    )
    r_delete_unknown = client.delete("/admin/clients/ghost", headers=ADMIN)
    log_case(
        "管理请求内容校验",
        {"cases": ["未知账号", "非法过滤器", "字段类型错误", "token 冲突", "删未知账号"]},
        "404 not_found / 400 invalid_filter / 400 invalid_request / 409 token_in_use",
        {
            "unknown": r_unknown.status_code,
            "bad_filter": (r_bad_filter.status_code, r_bad_filter.json()),
            "bad_shape": r_bad_shape.status_code,
            "dup_token": r_dup_token.status_code,
            "delete_unknown": r_delete_unknown.status_code,
        },
    )
    assert r_unknown.status_code == 404
    assert r_bad_filter.status_code == 400
    assert r_bad_filter.json()["reason"] == "invalid_filter"
    assert r_bad_shape.status_code == 400
    assert r_dup_token.status_code == 409
    assert r_delete_unknown.status_code == 404


def test_acl_tightening_revokes_live_subscription(client):
    _make_robot_client(client, subscribe=["robot/#"])
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        assert ws.receive_json()["type"] == "subscribed"
        # 收紧前能收到 robot/leg
        client.post(
            "/publish", json={"topic": "robot/leg", "payload": "before"},
            headers=auth("t-pub"),
        )
        assert ws.receive_json()["message"]["payload"] == "before"
        # 运行期收紧：只允许 robot/arm/#
        r = client.put(
            "/admin/clients/robot-x/acl",
            json={"subscribe": ["robot/arm/#"]},
            headers=ADMIN,
        )
        assert r.status_code == 200
        # 在线连接收到撤销通知
        notice = ws.receive_json()
        # 收紧后 robot/leg 的消息不再送达
        client.post(
            "/publish", json={"topic": "robot/leg", "payload": "after"},
            headers=auth("t-pub"),
        )
        # 重新订阅允许的范围，验证连接本身仍可用
        ws.send_json({"op": "subscribe", "filter": "robot/arm/#"})
        granted = ws.receive_json()
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "arm-ok"},
            headers=auth("t-pub"),
        )
        msg = ws.receive_json()
        stats = client.get("/stats", headers=ADMIN).json()
        session = [s for s in stats["sessions"] if s["client_id"] == "robot-x"][0]
        log_case(
            "权限收紧追溯生效",
            {"before": "subscribe robot/#", "after": "subscribe robot/arm/#"},
            "不再被允许的订阅立即失效并通知客户端；robot/leg 不再送达；"
            "连接保持可用，可订阅新范围",
            {
                "notice": notice,
                "re_subscribe": granted,
                "received_after": msg,
                "session_subs": session["subscriptions"],
                "revoked_metric": stats["gateway"]["acl_revoked_subscriptions"],
            },
        )
        assert notice["type"] == "unsubscribed"
        assert notice["filter"] == "robot/#"
        assert notice["reason"] == "acl_revoked"
        assert granted["type"] == "subscribed"
        # 收到的是 robot/arm 而非被撤销范围的 robot/leg
        assert msg["message"]["topic"] == "robot/arm"
        assert [s["filter"] for s in session["subscriptions"]] == ["robot/arm/#"]
        assert stats["gateway"]["acl_revoked_subscriptions"] == 1


def test_acl_tightening_preserves_unacked_and_order(client):
    _make_robot_client(client, subscribe=["robot/#"])
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
        for p in ["k1", "k2"]:
            client.post(
                "/publish", json={"topic": "robot/arm", "payload": p},
                headers=auth("t-pub"),
            )
        got = [ws.receive_json()["message"] for _ in range(2)]  # 不确认
        # 收紧权限（robot/arm/# 仍在允许范围内，但 robot/# 订阅被撤销）
        client.put(
            "/admin/clients/robot-x/acl",
            json={"subscribe": ["robot/arm/#"]},
            headers=ADMIN,
        )
        assert ws.receive_json()["reason"] == "acl_revoked"
    # 重连：未确认消息必须按原顺序重投，不因权限收紧而丢失
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws:
        ws.receive_json()
        r1, r2 = ws.receive_json(), ws.receive_json()
        log_case(
            "收紧不丢未确认消息",
            {"unacked": ["k1", "k2"], "tightened_to": "robot/arm/#"},
            "pending 中的未确认消息原样保留，重连后按原顺序重投",
            {"redelivered": [r1, r2]},
        )
        assert [r1["message"]["payload"], r2["message"]["payload"]] == ["k1", "k2"]
        assert r1["redelivered"] and r2["redelivered"]
        assert r1["message"]["seq"] == got[0]["seq"]
        assert r2["message"]["seq"] == got[1]["seq"]


def test_acl_tightening_does_not_affect_other_clients(client):
    _make_robot_client(client, subscribe=["robot/#"])
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws_x, \
         client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws_s:
        ws_x.receive_json()
        ws_s.receive_json()
        ws_x.send_json({"op": "subscribe", "filter": "robot/#"})
        ws_x.receive_json()
        ws_s.send_json({"op": "subscribe", "filter": "robot/#"})
        ws_s.receive_json()
        # 收紧 robot-x
        client.put(
            "/admin/clients/robot-x/acl", json={"subscribe": []}, headers=ADMIN
        )
        assert ws_x.receive_json()["reason"] == "acl_revoked"
        # subscriber 不受影响，照常收到消息
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "for-sub"},
            headers=auth("t-pub"),
        )
        msg = ws_s.receive_json()
        log_case(
            "收紧不影响其他连接",
            {"tightened": "robot-x", "observer": "subscriber"},
            "只撤销目标身份的订阅，其他连接的订阅与投递不变",
            {"observer_received": msg},
        )
        assert msg["message"]["payload"] == "for-sub"


def test_delete_client_disconnects_and_revokes(client):
    _make_robot_client(client, subscribe=["robot/#"])
    with client.websocket_connect("/ws?token=t-x&client_id=robot-x") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
        r = client.delete("/admin/clients/robot-x", headers=ADMIN)
        assert r.json()["deleted"] is True
        notice = ws.receive_json()
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        # token 立即失效
        r_pub = client.post(
            "/publish", json={"topic": "robot/arm", "payload": "x"},
            headers=auth("t-x"),
        )
        stats = client.get("/stats", headers=ADMIN).json()
        log_case(
            "注销账号即时生效",
            {"client_id": "robot-x"},
            "在线连接收到 account_revoked 后以 4401 关闭；token 立即失效；"
            "会话被回收",
            {
                "notice": notice,
                "close_code": exc.value.code,
                "publish_after": r_pub.status_code,
                "sessions": [s["client_id"] for s in stats["sessions"]],
            },
        )
        assert notice["reason"] == "account_revoked"
        assert exc.value.code == 4401
        assert r_pub.status_code == 401
        assert "robot-x" not in [s["client_id"] for s in stats["sessions"]]
