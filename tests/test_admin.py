"""运行期账号与权限治理：管理凭据保护、错误可区分、改动追溯生效。"""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from tests.conftest import ADMIN_TOKEN, auth, log_case


def _admin() -> dict[str, str]:
    return auth(ADMIN_TOKEN)


def test_admin_endpoints_require_admin_credential(client):
    r_no_token = client.get("/admin/accounts")
    r_client_token = client.get("/admin/accounts", headers=auth("t-sub"))
    r_admin = client.get("/admin/accounts", headers=_admin())
    log_case(
        "管理接口凭据保护",
        {"no_token": True, "client_token": "t-sub", "admin_token": ADMIN_TOKEN},
        "无凭据->401 invalid_credentials；客户端凭据->403 not_admin；管理凭据->200",
        {"no_token": r_no_token.status_code, "client_token": r_client_token.status_code,
         "admin": r_admin.status_code},
    )
    assert r_no_token.status_code == 401
    assert r_no_token.json()["reason"] == "invalid_credentials"
    assert r_client_token.status_code == 403
    assert r_client_token.json()["reason"] == "not_admin"
    assert r_admin.status_code == 200
    assert "subscriber" in r_admin.json()["accounts"]


def test_admin_request_validation_errors_distinguishable(client):
    r_bad_body = client.post("/admin/accounts", json={"client_id": ""}, headers=_admin())
    r_conflict1 = client.post(
        "/admin/accounts", json={"client_id": "robot-x", "token": "t-x"},
        headers=_admin(),
    )
    r_conflict2 = client.post(
        "/admin/accounts", json={"client_id": "robot-y", "token": "t-x"},
        headers=_admin(),
    )
    r_unknown = client.delete("/admin/accounts/ghost", headers=_admin())
    r_bad_acl = client.put(
        "/admin/accounts/robot-x/acl",
        json={"publish": [], "subscribe": ["robot//bad"]},
        headers=_admin(),
    )
    r_acl_unknown = client.put(
        "/admin/accounts/ghost/acl",
        json={"publish": [], "subscribe": ["a/#"]},
        headers=_admin(),
    )
    log_case(
        "管理请求错误可区分",
        {"bad_body": "空 client_id", "conflict": "token 复用", "unknown": "ghost",
         "bad_acl": "robot//bad"},
        "400 invalid_request / 409 token_in_use / 404 unknown_client 互相区分",
        {"bad_body": r_bad_body.json(), "conflict": r_conflict2.json(),
         "unknown": r_unknown.json(), "bad_acl": r_bad_acl.json()},
    )
    assert r_conflict1.status_code == 200
    assert r_bad_body.status_code == 400
    assert r_bad_body.json()["reason"] == "invalid_request"
    assert r_conflict2.status_code == 409
    assert r_conflict2.json()["reason"] == "token_in_use"
    assert r_unknown.status_code == 404
    assert r_unknown.json()["reason"] == "unknown_client"
    assert r_bad_acl.status_code == 400
    assert r_bad_acl.json()["reason"] == "invalid_request"
    assert r_acl_unknown.status_code == 404


def test_runtime_account_lifecycle(client):
    # 新增账号立即生效：新 token 可以连接与订阅
    r = client.post(
        "/admin/accounts", json={"client_id": "robot-7", "token": "t-r7"},
        headers=_admin(),
    )
    assert r.status_code == 200
    r = client.put(
        "/admin/accounts/robot-7/acl",
        json={"publish": [], "subscribe": ["robot/arm/#"]},
        headers=_admin(),
    )
    assert r.status_code == 200
    with client.websocket_connect("/ws?token=t-r7&client_id=robot-7") as ws:
        assert ws.receive_json()["type"] == "connected"
        ws.send_json({"op": "subscribe", "filter": "robot/arm/#"})
        assert ws.receive_json()["type"] == "subscribed"
        # 注销账号：在线连接被断开，凭据立即失效
        r = client.delete("/admin/accounts/robot-7", headers=_admin())
        assert r.status_code == 200
        displaced = ws.receive_json()
        log_case(
            "账号注销即时生效",
            {"client_id": "robot-7"},
            "注销后在线连接收到 displaced(account_removed) 并以 4401 关闭",
            {"displaced": displaced},
        )
        assert displaced["type"] == "displaced"
        assert displaced["reason"] == "account_removed"
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 4401
    # 旧 token 不再可用
    r = client.post(
        "/publish", json={"topic": "robot/arm", "payload": "x"}, headers=auth("t-r7")
    )
    assert r.status_code == 401


def test_permission_tightening_revokes_existing_subscription(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        assert ws.receive_json()["type"] == "connected"
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        assert ws.receive_json()["type"] == "subscribed"
        client.post(
            "/publish", json={"topic": "robot/leg", "payload": "before"},
            headers=auth("t-pub"),
        )
        assert ws.receive_json()["message"]["payload"] == "before"

        # 运行期收紧：subscriber 只允许 robot/arm/#
        r = client.put(
            "/admin/accounts/subscriber/acl",
            json={"publish": [], "subscribe": ["robot/arm/#"]},
            headers=_admin(),
        )
        assert r.status_code == 200
        # 既有订阅被追溯收回
        revoked = ws.receive_json()
        # 收紧后：robot/leg 不再投递；越范围的新订阅被拒绝；范围内可重新订阅
        client.post(
            "/publish", json={"topic": "robot/leg", "payload": "after"},
            headers=auth("t-pub"),
        )
        ws.send_json({"op": "subscribe", "filter": "robot/leg/#"})
        denied = ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/arm/#"})
        granted = ws.receive_json()
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "in-scope"},
            headers=auth("t-pub"),
        )
        got = ws.receive_json()
        log_case(
            "权限收紧追溯生效",
            {"old_rule": "robot/#", "new_rule": "robot/arm/#"},
            "既有 robot/# 订阅被收回(permission_revoked)；robot/leg 不再投递；"
            "越范围订阅 not_allowed；范围内订阅与投递正常",
            {"revoked": revoked, "denied": denied, "granted": granted, "got": got},
        )
        assert revoked["type"] == "unsubscribed"
        assert revoked["filter"] == "robot/#"
        assert revoked["reason"] == "permission_revoked"
        assert denied["type"] == "error" and denied["reason"] == "not_allowed"
        assert granted["type"] == "subscribed"
        assert got["message"]["payload"] == "in-scope"  # 不是 robot/leg 的 after


def test_tightening_does_not_affect_other_connections(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws1, \
         client.websocket_connect("/ws?token=t-sub2&client_id=subscriber-2") as ws2:
        ws1.receive_json()
        ws2.receive_json()
        for ws in (ws1, ws2):
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            assert ws.receive_json()["type"] == "subscribed"
        # 只收紧 subscriber，subscriber-2 不受影响
        r = client.put(
            "/admin/accounts/subscriber/acl",
            json={"publish": [], "subscribe": ["robot/arm/#"]},
            headers=_admin(),
        )
        assert r.status_code == 200
        assert ws1.receive_json()["reason"] == "permission_revoked"
        client.post(
            "/publish", json={"topic": "robot/leg", "payload": "for-ws2"},
            headers=auth("t-pub"),
        )
        got2 = ws2.receive_json()
        log_case(
            "收紧不影响其他连接",
            {"tightened": "subscriber", "untouched": "subscriber-2"},
            "仅目标身份的订阅被收回，其他连接照常收到 robot/leg 消息",
            {"ws2_got": got2},
        )
        assert got2["message"]["payload"] == "for-ws2"


def test_tightening_preserves_unacked_messages(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        ws.receive_json()
        client.post(
            "/publish", json={"topic": "robot/arm", "payload": "unacked"},
            headers=auth("t-pub"),
        )
        first = ws.receive_json()  # 收到但不确认
        # 收紧权限（订阅被收回）后断线
        client.put(
            "/admin/accounts/subscriber/acl",
            json={"publish": [], "subscribe": ["robot/arm/#"]},
            headers=_admin(),
        )
        ws.receive_json()  # permission_revoked
    # 重连：收紧前已投递的未确认消息不丢，按原顺序重投
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
        ws.receive_json()
        again = ws.receive_json()
        log_case(
            "收紧不丢未确认消息",
            {"unacked": first["message"]["payload"]},
            "未确认消息保存在会话中，权限收紧不清除，重连后按原序重投",
            {"redelivered": again},
        )
        assert again["message"]["payload"] == "unacked"
        assert again["message"]["seq"] == first["message"]["seq"]
        assert again["redelivered"] is True
