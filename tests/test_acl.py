"""访问控制：越权发布、越权订阅、无效凭据均可区分，且不影响其他连接。"""
from __future__ import annotations

import pytest
from starlette.websockets import WebSocketDisconnect

from gateway.auth import ACLRule, Action
from tests.conftest import RULES, auth, log_case


def test_invalid_credentials_http(client):
    r = client.post("/publish", json={"topic": "robot/arm", "payload": "x"})
    log_case(
        "HTTP 无效凭据",
        {"authorization": None},
        "缺少/未知 token -> 401 invalid_credentials",
        {"status": r.status_code, "body": r.json()},
    )
    assert r.status_code == 401
    assert r.json()["reason"] == "invalid_credentials"


def test_invalid_credentials_ws(client):
    with client.websocket_connect("/ws?token=bad-token") as ws:
        resp = ws.receive_json()
        log_case(
            "WS 无效凭据",
            {"token": "bad-token"},
            "认证失败 -> error invalid_credentials 后以 4401 关闭，不影响他人",
            {"response": resp},
        )
        assert resp["reason"] == "invalid_credentials"
        with pytest.raises(WebSocketDisconnect) as exc:
            ws.receive_json()
        assert exc.value.code == 4401


def test_unauthorized_publish_http(client):
    # subscriber 只有订阅权限
    r = client.post(
        "/publish",
        json={"topic": "robot/arm", "payload": "x"},
        headers=auth("t-sub"),
    )
    log_case(
        "越权发布",
        {"token": "t-sub", "topic": "robot/arm"},
        "subscriber 无 PUBLISH 规则 -> 403 not_allowed",
        {"status": r.status_code, "body": r.json()},
    )
    assert r.status_code == 403
    assert r.json()["reason"] == "not_allowed"


def test_unauthorized_publish_ws_does_not_break_connection(client):
    with client.websocket_connect("/ws?token=t-arm&client_id=arm-1") as ws:
        ws.receive_json()
        ws.send_json({"op": "publish", "topic": "robot/leg", "payload": "x"})
        denied = ws.receive_json()
        # 连接仍然可用：发布到自己允许的范围应成功
        ws.send_json({"op": "publish", "topic": "robot/arm/joint1", "payload": "ok"})
        allowed = ws.receive_json()
        log_case(
            "WS 越权发布不影响连接",
            {"denied_topic": "robot/leg", "allowed_topic": "robot/arm/joint1"},
            "arm-only 仅允许 robot/arm/#；越权返回 error 但连接保持",
            {"denied": denied, "allowed": allowed},
        )
        assert denied["type"] == "error" and denied["reason"] == "not_allowed"
        assert allowed["type"] == "published"


def test_unauthorized_subscribe_ws(client):
    with client.websocket_connect("/ws?token=t-arm&client_id=arm-2") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        resp = ws.receive_json()
        log_case(
            "越权订阅",
            {"token": "t-arm", "filter": "robot/#"},
            "规则 robot/arm/# 不能覆盖 robot/# -> not_allowed",
            {"response": resp},
        )
        assert resp["type"] == "error" and resp["reason"] == "not_allowed"


def test_invalid_topic_reasons_distinguishable(client):
    r1 = client.post(
        "/publish", json={"topic": "robot/#", "payload": "x"}, headers=auth("t-pub")
    )
    r2 = client.post(
        "/publish", json={"topic": "robot//arm", "payload": "x"}, headers=auth("t-pub")
    )
    log_case(
        "非法主题原因可区分",
        {"topics": ["robot/#", "robot//arm"]},
        "通配符发布与空层级是不同拒绝原因",
        {"r1": r1.json(), "r2": r2.json()},
    )
    assert r1.status_code == 400 and r1.json()["reason"] == "wildcard_in_publish"
    assert r2.status_code == 400 and r2.json()["reason"] == "empty_level"


def test_rule_update_applies_to_existing_connection(client, app):
    with client.websocket_connect("/ws?token=t-arm&client_id=arm-3") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/leg/#"})
        denied = ws.receive_json()
        # 更新规则：放行 arm-only 对 robot/leg/# 的订阅
        app.state.acl.set_rules(
            RULES + [ACLRule("arm-only", Action.SUBSCRIBE, "robot/leg/#")]
        )
        ws.send_json({"op": "subscribe", "filter": "robot/leg/#"})
        granted = ws.receive_json()
        log_case(
            "规则更新即时生效",
            {"filter": "robot/leg/#", "update": "新增 SUBSCRIBE robot/leg/#"},
            "鉴权实时查询 ACLStore，既有连接无需重连",
            {"before": denied, "after": granted},
        )
        assert denied["reason"] == "not_allowed"
        assert granted["type"] == "subscribed"


def test_unauthorized_publish_does_not_affect_others(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=sub-observer") as ws:
        ws.receive_json()
        ws.send_json({"op": "subscribe", "filter": "robot/#"})
        assert ws.receive_json()["type"] == "subscribed"
        # 越权发布被拒绝
        r = client.post(
            "/publish",
            json={"topic": "robot/arm", "payload": "bad"},
            headers=auth("t-sub"),
        )
        assert r.status_code == 403
        # 合法发布仍正常送达其他连接
        client.post(
            "/publish",
            json={"topic": "robot/arm", "payload": "good"},
            headers=auth("t-pub"),
        )
        msg = ws.receive_json()
        log_case(
            "越权不影响其他连接",
            {"rejected": "bad", "published": "good"},
            "越权请求被拒绝且不进入 broker",
            {"received": msg},
        )
        assert msg["message"]["payload"] == "good"
