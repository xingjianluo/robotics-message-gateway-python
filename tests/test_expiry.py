"""会话过期回收与离线积压上限：内存有界、可观测、不影响其他客户端。"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from gateway.app import create_app
from tests.conftest import RULES, TOKENS, auth, log_case


def _make_client(**kwargs) -> TestClient:
    app = create_app(**kwargs)
    app.state.acl.set_tokens(dict(TOKENS))
    app.state.acl.set_rules(list(RULES))
    return TestClient(app)


def _pub(client, payload, topic="robot/arm"):
    r = client.post(
        "/publish", json={"topic": topic, "payload": payload}, headers=auth("t-pub")
    )
    assert r.status_code == 200


def test_offline_session_expires_and_memory_reclaimed():
    with _make_client(session_expiry=10.0, reap_interval=60.0) as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
        # 离线期间积压 3 条
        for p in ["o1", "o2", "o3"]:
            _pub(client, p)
        before = client.get("/stats", headers=auth("t-sub")).json()
        assert before["sessions"][0]["backlog"] == 3

        # 模拟时间前进到过期之后，手动触发回收
        app = client.app
        reaped = app.state.sessions.reap_expired(
            time.time() + 11.0, 10.0, app.state.broker
        )
        after = client.get("/stats", headers=auth("t-sub")).json()
        log_case(
            "离线会话过期回收",
            {"expiry": 10.0, "offline_backlog": 3},
            "离线超时的会话被回收：订阅取消、积压丢弃计数、会话从内存移除",
            {
                "reaped": reaped,
                "sessions_after": after["sessions"],
                "broker_subs_after": after["broker"]["subscriptions"],
                "gateway": after["gateway"],
            },
        )
        assert reaped == ["subscriber"]
        assert after["sessions"] == []  # 内存真正释放
        assert after["broker"]["subscriptions"] == []  # broker 订阅也取消
        assert after["gateway"]["expired_sessions"] == 1
        assert after["gateway"]["expired_messages"] == 3


def test_expiry_spares_online_and_fresh_sessions():
    with _make_client(session_expiry=10.0, reap_interval=60.0) as client:
        # subscriber2 保持在线；subscriber 离线但刚断开（未超时）
        with client.websocket_connect("/ws?token=t-sub2&client_id=subscriber2") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
            with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as old:
                old.receive_json()
                old.send_json({"op": "subscribe", "filter": "robot/#"})
                old.receive_json()
            app = client.app
            # 只推进 5 秒（未达 10 秒过期线）
            reaped = app.state.sessions.reap_expired(
                time.time() + 5.0, 10.0, app.state.broker
            )
            stats = client.get("/stats", headers=auth("t-sub")).json()
            log_case(
                "回收不伤及在线与未超时会话",
                {"expiry": 10.0, "elapsed": 5.0},
                "在线会话与未超时的离线会话都不被回收",
                {"reaped": reaped, "sessions": [s["client_id"] for s in stats["sessions"]]},
            )
            assert reaped == []
            assert {s["client_id"] for s in stats["sessions"]} == {
                "subscriber",
                "subscriber2",
            }


def test_background_reaper_collects_expired_session():
    with _make_client(session_expiry=0.2, reap_interval=0.1) as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            ws.send_json({"op": "subscribe", "filter": "robot/#"})
            ws.receive_json()
        _pub(client, "offline")
        # 后台回收任务应在过期后自动清理（轮询等待，避免依赖精确时序）
        deadline = time.time() + 5.0
        gateway = {}
        while time.time() < deadline:
            stats = client.get("/stats", headers=auth("t-sub")).json()
            gateway = stats["gateway"]
            if gateway["expired_sessions"] >= 1:
                break
            time.sleep(0.05)
        log_case(
            "后台任务自动回收",
            {"expiry": 0.2, "reap_interval": 0.1},
            "后台回收循环在会话过期后自动清理并计数",
            {"gateway": gateway},
        )
        assert gateway["expired_sessions"] == 1
        assert gateway["expired_messages"] == 1


def test_offline_backlog_cap_drops_oldest():
    with _make_client(max_offline_backlog=3, max_queue=100) as client:
        # subscriber2 在线，作为对照
        with client.websocket_connect("/ws?token=t-sub2&client_id=subscriber2") as ws2:
            ws2.receive_json()
            ws2.send_json({"op": "subscribe", "filter": "robot/#"})
            ws2.receive_json()
            with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
                ws.receive_json()
                ws.send_json({"op": "subscribe", "filter": "robot/#"})
                ws.receive_json()
            # subscriber 离线后发布 5 条
            for i in range(5):
                _pub(client, f"m{i}")
            got_online = [ws2.receive_json()["message"]["payload"] for _ in range(5)]
            stats = client.get("/stats", headers=auth("t-sub")).json()
            offline = [s for s in stats["sessions"] if s["client_id"] == "subscriber"][0]
            log_case(
                "离线积压上限",
                {"cap": 3, "published_after_offline": 5},
                "离线积压超过上限即丢弃最早的消息并计数；在线客户端不受影响",
                {
                    "offline_backlog": offline["backlog"],
                    "overflow_dropped": offline["overflow_dropped"],
                    "gateway_overflow": stats["gateway"]["overflow_dropped"],
                    "online_received": got_online,
                },
            )
            assert offline["backlog"] == 3
            assert offline["overflow_dropped"] == 2
            assert stats["gateway"]["overflow_dropped"] == 2
            assert got_online == [f"m{i}" for i in range(5)]  # 在线客户端完整收到

        # 重连后只收到积压中保留的最新 3 条（最早 2 条已被丢弃）
        with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as ws:
            ws.receive_json()
            kept = [ws.receive_json()["message"]["payload"] for _ in range(3)]
            log_case(
                "超限丢弃结论",
                {"cap": 3},
                "保留最新 3 条积压，重连后按序投递",
                {"kept": kept},
            )
            assert kept == ["m2", "m3", "m4"]
