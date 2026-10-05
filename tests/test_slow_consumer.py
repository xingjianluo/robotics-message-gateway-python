"""慢消费者：不阻塞其他客户端、内存有界、积压与丢弃可观测。"""
from __future__ import annotations

import time

from fastapi.testclient import TestClient

from gateway.app import create_app
from tests.conftest import RULES, TOKENS, auth, log_case

MAX_QUEUE = 4
MAX_INFLIGHT = 4
TOTAL = 40


def _make_client() -> TestClient:
    app = create_app(max_queue=MAX_QUEUE, max_inflight=MAX_INFLIGHT)
    app.state.acl.set_tokens(dict(TOKENS))
    app.state.acl.set_rules(list(RULES))
    return TestClient(app)


def test_slow_consumer_does_not_block_others():
    with _make_client() as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=slow-1") as slow, \
             client.websocket_connect("/ws?token=t-sub&client_id=fast-1") as fast:
            slow.receive_json()
            fast.receive_json()
            for ws in (slow, fast):
                ws.send_json({"op": "subscribe", "filter": "robot/#"})
                assert ws.receive_json()["type"] == "subscribed"

            received = []
            for i in range(TOTAL):
                r = client.post(
                    "/publish",
                    json={"topic": "robot/arm", "payload": f"m{i}"},
                    headers=auth("t-pub"),
                )
                assert r.status_code == 200
                msg = fast.receive_json()  # 快消费者逐条接收并确认
                received.append(msg["message"])
                fast.send_json({"op": "ack", "seq": msg["message"]["seq"]})
            # slow 从不读取、从不确认

        # 断开 slow：传输中的消息回收进会话，统计达到守恒稳态
        # （服务端断线收尾异步完成，轮询等待其落定）
        stats = None
        for _ in range(50):
            stats = client.get("/stats", headers=auth("t-sub")).json()
            slow_now = {s["client_id"]: s for s in stats["sessions"]}["slow-1"]
            if (
                slow_now["pending_unacked"] + slow_now["ready"]
                + slow_now["backlog"] + slow_now["dropped"] == TOTAL
            ):
                break
            time.sleep(0.02)
        sessions = {s["client_id"]: s for s in stats["sessions"]}
        slow_stats, fast_stats = sessions["slow-1"], sessions["fast-1"]
        log_case(
            "慢消费者隔离",
            {"total": TOTAL, "max_queue": MAX_QUEUE, "max_inflight": MAX_INFLIGHT,
             "slow_behavior": "不读取不确认"},
            "慢消费者未确认达上限后背压，订阅队列有界溢出即丢弃并计数；"
            "快消费者投递不受任何影响",
            {"fast_received": len(received), "slow": slow_stats, "fast": fast_stats},
        )
        # 快消费者完整、按序收到全部消息
        assert len(received) == TOTAL
        assert [m["payload"] for m in received] == [f"m{i}" for i in range(TOTAL)]
        seqs = [m["seq"] for m in received]
        assert seqs == sorted(seqs)
        assert fast_stats["dropped"] == 0
        # 慢消费者：丢弃可观测，内存有界
        assert slow_stats["dropped"] > 0
        assert slow_stats["backlog"] <= MAX_QUEUE
        assert slow_stats["pending_unacked"] <= MAX_INFLIGHT
        # 守恒：未确认 + 待重投 + 积压 + 丢弃 == 发布总数（不丢不重）
        conserved = (
            slow_stats["pending_unacked"]
            + slow_stats["ready"]
            + slow_stats["backlog"]
            + slow_stats["dropped"]
        )
        assert conserved == TOTAL, f"conserved={conserved} != {TOTAL}"


def test_slow_consumer_recovers_after_draining():
    with _make_client() as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=slow-2") as slow:
            slow.receive_json()
            slow.send_json({"op": "subscribe", "filter": "robot/#"})
            slow.receive_json()
            for i in range(TOTAL):
                client.post(
                    "/publish",
                    json={"topic": "robot/arm", "payload": f"m{i}"},
                    headers=auth("t-pub"),
                )
            # 慢消费者开始消费：应收到积压在队列中的最早消息（丢弃的是溢出部分）
            first = slow.receive_json()["message"]
            log_case(
                "慢消费者恢复消费",
                {"total": TOTAL, "max_queue": MAX_QUEUE},
                "队列保留最早未投递消息，溢出丢弃发生在队满之后",
                {"first_after_drain": first},
            )
            assert first["payload"] == "m0"
