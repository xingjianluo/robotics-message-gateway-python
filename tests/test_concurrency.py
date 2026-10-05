"""并发发布：多发布者交错发布，订阅者不重不漏，单发布者内有序。"""
from __future__ import annotations

from contextlib import ExitStack

from tests.conftest import auth, log_case

PUBLISHERS = 3
PER_PUBLISHER = 10


def test_concurrent_publishers_no_loss_no_dup(client):
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as sub:
        sub.receive_json()
        sub.send_json({"op": "subscribe", "filter": "robot/#"})
        sub.receive_json()
        stack = ExitStack()
        pubs = [
            stack.enter_context(
                client.websocket_connect(f"/ws?token=t-pub{i + 1}&client_id=publisher-{i + 1}")
            )
            for i in range(PUBLISHERS)
        ]
        for ws in pubs:
            ws.receive_json()
        # 交错发布，模拟并发
        for n in range(PER_PUBLISHER):
            for i, ws in enumerate(pubs):
                ws.send_json({"op": "publish", "topic": f"robot/p{i}", "payload": f"p{i}-{n}"})
        for ws in pubs:
            for _ in range(PER_PUBLISHER):
                resp = ws.receive_json()
                assert resp["type"] == "published"

        received = [sub.receive_json()["message"] for _ in range(PUBLISHERS * PER_PUBLISHER)]
        by_pub: dict[str, list[str]] = {}
        for m in received:
            by_pub.setdefault(m["topic"], []).append(m["payload"])
        log_case(
            "并发发布",
            {"publishers": PUBLISHERS, "per_publisher": PER_PUBLISHER},
            "broker 在事件循环内串行化投递：全局不重不漏，单主题内按发布顺序",
            {"received_total": len(received), "by_publisher": by_pub},
        )
        # 不重不漏
        payloads = [m["payload"] for m in received]
        assert len(payloads) == len(set(payloads)) == PUBLISHERS * PER_PUBLISHER
        # 单发布者（单主题）内严格有序
        for i in range(PUBLISHERS):
            assert by_pub[f"robot/p{i}"] == [f"p{i}-{n}" for n in range(PER_PUBLISHER)]
        stack.close()


def test_http_and_ws_publish_consistent(client):
    """HTTP 与 WS 发布的消息经同一 broker 投递，订阅者无差别接收。"""
    with client.websocket_connect("/ws?token=t-sub&client_id=subscriber") as sub:
        sub.receive_json()
        sub.send_json({"op": "subscribe", "filter": "robot/#"})
        sub.receive_json()
        client.post(
            "/publish",
            json={"topic": "robot/arm", "payload": "via-http"},
            headers=auth("t-pub"),
        )
        with client.websocket_connect("/ws?token=t-pub&client_id=publisher") as pub:
            pub.receive_json()
            pub.send_json({"op": "publish", "topic": "robot/arm", "payload": "via-ws"})
            assert pub.receive_json()["type"] == "published"
        m1 = sub.receive_json()["message"]
        m2 = sub.receive_json()["message"]
        log_case(
            "HTTP/WS 发布一致",
            {"http": "via-http", "ws": "via-ws"},
            "两个入口共享 broker 与全局 seq，订阅者按序接收",
            {"received": [m1["payload"], m2["payload"]]},
        )
        assert [m1["payload"], m2["payload"]] == ["via-http", "via-ws"]
        assert m1["seq"] < m2["seq"]


def test_app_starts_and_root_ok(client):
    r = client.get("/")
    log_case("应用启动", {"GET": "/"}, "服务可用", {"status": r.status_code, "body": r.json()})
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
