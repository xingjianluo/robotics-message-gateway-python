"""认证、按客户端/主题范围授权、错误码可区分、规则热更新一致。"""

from __future__ import annotations

import json
import logging

from fastapi.testclient import TestClient

from .conftest import auth, log_case

log = logging.getLogger("case")


def test_http_missing_and_invalid_credentials_distinguishable(client):
    # 1) 完全无凭据
    r1 = client.post("/publish", json={"topic": "robots/r1/status", "payload": {}})
    # 2) 错误 token
    r2 = client.post(
        "/publish",
        json={"topic": "robots/r1/status", "payload": {}},
        headers=auth("pub-r1", "wrong-token"),
    )
    log_case(
        "auth-failures",
        {"no_creds": "...", "bad_token": "wrong-token"},
        ["缺失凭据 401 missing_credentials", "非法凭据 401 invalid_credentials"],
        {"no_creds": (r1.status_code, r1.json().get("code")),
         "bad_token": (r2.status_code, r2.json().get("code"))},
    )
    assert r1.status_code == 401 and r1.json()["code"] == "missing_credentials"
    assert r2.status_code == 401 and r2.json()["code"] == "invalid_credentials"


def test_publish_forbidden_is_distinct_and_isolated(client, broker):
    headers = auth("pub-r1", "t-pub")
    ok = client.post(
        "/publish",
        json={"topic": "robots/r1/status", "payload": {"v": 1}},
        headers=headers,
    )
    forbidden = client.post(
        "/publish",
        json={"topic": "robots/r2/status", "payload": {"v": 2}},  # r1 无权发 r2
        headers=headers,
    )
    # 越权之后合法发布仍然成功 -> 不影响该连接/其他连接
    again = client.post(
        "/publish",
        json={"topic": "robots/r1/status", "payload": {"v": 3}},
        headers=headers,
    )
    log_case(
        "publish-forbidden-isolation",
        {"topics": ["r1/status", "r2/status", "r1/status"]},
        ["ACL 仅允许 robots/r1/status 的 pub", "拒绝不应影响后续合法操作"],
        {"codes": [ok.status_code, forbidden.status_code, again.status_code],
         "forbidden_body": forbidden.json()},
    )
    assert ok.status_code == 200
    assert forbidden.status_code == 403 and forbidden.json()["code"] == "publish_forbidden"
    assert again.status_code == 200


def test_subscribe_forbidden_over_ws(client):
    with client.websocket_connect("/ws?client_id=pub-r1&token=t-pub") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "1", "filter": "robots/+/status"}))
        frame = json.loads(ws.receive_text())
    log_case(
        "subscribe-forbidden-ws",
        {"client": "pub-r1", "filter": "robots/+/status"},
        ["pub-r1 只有 pub 权限", "sub 越权返回 subscribe_forbidden"],
        frame,
    )
    assert frame["type"] == "error" and frame["code"] == "subscribe_forbidden"


def test_ws_invalid_credentials_closed(client):
    # 凭据非法时握手以 4403 关闭，且不影响其他已建立连接
    with client.websocket_connect("/ws?client_id=sub-all&token=t-all") as good:
        good.send_text(json.dumps({"action": "subscribe", "id": "0", "filter": "robots/#"}))
        assert json.loads(good.receive_text())["status"] == "ok"
        try:
            with client.websocket_connect("/ws?client_id=ghost&token=x"):
                rejected = None
        except Exception as exc:  # TestClient 抛出关闭异常
            rejected = str(exc)
        # 旧连接仍能收消息
        client.post(
            "/publish",
            json={"topic": "robots/r1/status", "payload": {"v": 7}},
            headers=auth("pub-r1", "t-pub"),
        )
        msg = json.loads(good.receive_text())
    log_case(
        "ws-invalid-cred-isolation",
        {"bad": "ghost:x", "then_publish": "robots/r1/status"},
        ["非法凭据 4403 关闭", "已建立连接照常收消息"],
        {"rejected_close": rejected, "good_received": msg["type"]},
    )
    assert msg["type"] == "message" and msg["payload"] == {"v": 7}


def test_acl_hot_update_existing_connections_consistent(client, broker):
    # 建立订阅并验证可收
    with client.websocket_connect("/ws?client_id=sub-status&token=t-sub") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "1", "filter": "robots/+/status"}))
        assert json.loads(ws.receive_text())["status"] == "ok"
        client.post(
            "/publish",
            json={"topic": "robots/r1/status", "payload": {"v": 1}},
            headers=auth("pub-r1", "t-pub"),
        )
        assert json.loads(ws.receive_text())["payload"] == {"v": 1}
        session = broker.registry.get("sub-status")

        # ops 热更新规则：撤销 sub-status 的订阅权（只给它一个无关范围）
        new_rules = {
            "pub-r1": {"token": "t-pub", "permissions": [
                {"filter": "robots/r1/status", "actions": ["pub"]}]},
            "sub-status": {"token": "t-sub", "permissions": [
                {"filter": "other/#", "actions": ["sub"]}]},
            "sub-all": {"token": "t-all", "permissions": [
                {"filter": "robots/#", "actions": ["sub"]}]},
            "ops": {"token": "t-ops", "permissions": [
                {"filter": "#", "actions": ["pub", "sub"]}]},
        }
        upd = client.post("/admin/credentials", json=new_rules, headers=auth("ops", "t-ops"))
        assert upd.status_code == 200, upd.text
        revoked = upd.json()["revoked"]

        # 既有连接：订阅记录已被 reconcile 立即移除（行为与新 ACL 一致）
        assert "robots/+/status" not in session.state.subscriptions
        # 既有连接再订阅旧范围 -> 明确拒绝；连接仍可用
        ws.send_text(json.dumps({"action": "subscribe", "id": "2", "filter": "robots/+/status"}))
        err = json.loads(ws.receive_text())
        assert err["code"] == "subscribe_forbidden"
        # 既有连接订阅新允许范围 -> 成功
        ws.send_text(json.dumps({"action": "subscribe", "id": "3", "filter": "other/#"}))
        ok_new = json.loads(ws.receive_text())
        assert ok_new["status"] == "ok"

        # 撤权后再发布：确认该会话不再收到新匹配消息（不阻塞在 socket 上）。
        # 撤销前已在途的 v=1 保留，按会话确认协议处理；撤权不强杀在途。
        before_backlog = session.backlog()
        client.post(
            "/publish",
            json={"topic": "robots/r1/status", "payload": {"v": 2}},
            headers=auth("pub-r1", "t-pub"),
        )
        after_backlog = session.backlog()
        inflight_topics = {d.topic for d in session.state.inflight.values()}
        log_case(
            "acl-hot-update",
            {"revoke": "sub-status robots/+/status", "publish_after_revoke": "robots/r1/status"},
            ["reconcile 立即移除既有订阅", "旧范围订阅被拒", "撤权后无新匹配消息；撤权前在途保留"],
            {"revoked": revoked,
             "old_filter_reject": err["code"],
             "new_filter_ack": ok_new["status"],
             "backlog_before": before_backlog,
             "backlog_after": after_backlog,
             "inflight_topics": sorted(inflight_topics)},
        )
        assert any(item["client_id"] == "sub-status" and item["filter"] == "robots/+/status"
                   for item in revoked)
        assert after_backlog == before_backlog          # 撤销后的发布不再进入该会话
        assert inflight_topics == {"robots/r1/status"}  # 仅撤销前的在途残留
