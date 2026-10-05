"""会话过期回收与单客户端离线积压上限：内存有界、回收可观测、互不影响。"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from gateway.app import create_app
from tests.conftest import RULES, TOKENS, auth, log_case

TTL = 0.2
CAP = 5


def _make_client(**kw) -> TestClient:
    app = create_app(**kw)
    app.state.acl.set_tokens(dict(TOKENS))
    app.state.acl.set_rules(list(RULES))
    return TestClient(app)


def _wait_offline(client, cid):
    for _ in range(50):
        stats = client.get("/stats", headers=auth("t-sub")).json()
        s = {x["client_id"]: x for x in stats["sessions"]}.get(cid)
        if s is not None and not s["connected"]:
            return
        time.sleep(0.02)
    raise AssertionError(f"{cid} 未完成断线收尾")


def test_session_expires_after_ttl_and_memory_reclaimed():
    with _make_client(session_ttl=TTL, reap_interval=0.05) as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
        _wait_offline(client, "subscriber")
        # 离线期间积压 3 条
        for i in range(3):
            client.post(
                "/publish", json={"topic": "robot/arm", "payload": f"m{i}"},
                headers=auth("t-pub"),
            )
        stats = client.get("/stats", headers=auth("t-sub")).json()
        assert stats["sessions"], "过期前会话应存在"
        # 等待超过 TTL，后台回收任务自动清理
        expired = None
        for _ in range(100):
            stats = client.get("/stats", headers=auth("t-sub")).json()
            if stats["expiry"]["sessions_expired"] >= 1:
                expired = stats
                break
            time.sleep(0.05)
        log_case(
            "会话过期回收",
            {"session_ttl": TTL, "offline_messages": 3},
            "断线超过 TTL 的会话被后台任务回收：会话与订阅释放，"
            "回收会话数与丢弃消息数计入 expiry 指标",
            {"expiry": expired["expiry"] if expired else None,
             "sessions_left": expired["sessions"] if expired else None},
        )
        assert expired is not None, "会话未被回收"
        assert expired["expiry"]["sessions_expired"] == 1
        assert expired["expiry"]["messages_expired"] == 3
        # 内存真正释放：会话列表与 broker 订阅都不再持有
        assert expired["sessions"] == []
        assert expired["broker"]["subscriptions"] == []


def test_expiry_does_not_touch_online_sessions():
    with _make_client(session_ttl=TTL, reap_interval=0.05) as client, \
         client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
            time.sleep(TTL * 3)  # 在线会话即使空闲也不被回收
            stats = client.get("/stats", headers=auth("t-sub")).json()
            session = next(s for s in stats["sessions"]
                           if s["client_id"] == "subscriber")
            log_case(
                "在线会话不被回收",
                {"session_ttl": TTL, "idle": TTL * 3},
                "回收只针对断线会话",
                {"connected": session["connected"],
                 "expired": stats["expiry"]["sessions_expired"]},
            )
            assert session["connected"] is True
            assert stats["expiry"]["sessions_expired"] == 0


def test_offline_backlog_cap_drops_oldest():
    with _make_client(max_offline_backlog=CAP, reap_interval=0.05) as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
        _wait_offline(client, "subscriber")
        total = CAP + 4
        for i in range(total):
            client.post(
                "/publish", json={"topic": "robot/arm", "payload": f"m{i}"},
                headers=auth("t-pub"),
            )
        stats = client.get("/stats", headers=auth("t-sub")).json()
        session = next(s for s in stats["sessions"] if s["client_id"] == "subscriber")
        log_case(
            "离线积压上限",
            {"max_offline_backlog": CAP, "published": total},
            "离线积压总量超上限即从队头丢弃最旧消息并计数，"
            "重连后收到的是最新的 CAP 条",
            {"backlog": session["backlog"],
             "backlog_dropped": session["backlog_dropped"],
             "global_dropped": stats["expiry"]["backlog_dropped"]},
        )
        assert session["backlog"] <= CAP
        assert session["backlog_dropped"] == total - CAP
        assert stats["expiry"]["backlog_dropped"] == total - CAP
        # 重连：只收到最新的 CAP 条（最旧的已被丢弃），顺序保持
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            got = [ws.receive_json()["message"]["payload"] for _ in range(CAP)]
            assert got == [f"m{i}" for i in range(total - CAP, total)]


def test_backlog_cap_does_not_affect_online_clients():
    with _make_client(max_offline_backlog=CAP, reap_interval=0.05) as client:
        # subscriber 建立订阅后掉线，形成离线积压；subscriber-2 在线正常接收
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
        _wait_offline(client, "subscriber")
        with client.websocket_connect("/ws?token=t-sub2&client_id=subscriber-2") as ws2:
            ws2.receive_json()
            ws2.send_json({"op": "subscribe", "filter": "robot/#"})
            ws2.receive_json()
            for i in range(CAP + 4):
                client.post(
                    "/publish", json={"topic": "robot/arm", "payload": f"m{i}"},
                    headers=auth("t-pub"),
                )
                got = ws2.receive_json()  # 在线客户端逐条收到，不受修剪影响
                assert got["message"]["payload"] == f"m{i}"
                ws2.send_json({"op": "ack", "seq": got["message"]["seq"]})
            stats = client.get("/stats", headers=auth("t-sub")).json()
            s2 = next(s for s in stats["sessions"]
                      if s["client_id"] == "subscriber-2")
            log_case(
                "修剪不影响在线客户端",
                {"max_offline_backlog": CAP},
                "积压修剪只作用于离线会话，在线客户端完整按序接收",
                {"subscriber-2": {"dropped": s2["dropped"]}},
            )
            assert s2["dropped"] == 0
