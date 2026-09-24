"""慢消费者背压、有界内存、丢弃结论与对其他客户端零阻塞。

- 背压/隔离的核心组合直接驱动 Broker（与网关同一单线程事件循环模型），
  避免 TestClient 阻塞读对测试线程的限制；
- 协议层的慢消费丢弃与重连补投走真实 WebSocket 验证。
"""

from __future__ import annotations

import asyncio
import json
import logging

from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.auth import Authenticator

from .conftest import log_case, make_credentials

log = logging.getLogger("case")


def test_slow_consumer_does_not_block_others_and_is_bounded():
    """慢者不确认 -> 背压有界并丢弃；快者及时确认 -> 收齐有序；发布恒定成功。"""
    from gateway.broker import Broker

    broker = Broker(Authenticator(make_credentials()), max_inflight=2, max_queue=2)

    async def scenario():
        broker.connect("sub-all")     # 慢者：robots/#
        broker.connect("sub-status")  # 快者：robots/+/status
        broker.subscribe("sub-all", "robots/#")
        broker.subscribe("sub-status", "robots/+/status")
        slow = broker.registry.get("sub-all")
        fast = broker.registry.get("sub-status")
        slow.drain_pending(); fast.drain_pending()

        accepted_publish = 0
        for i in range(1, 21):
            broker.publish("robots/r1/status", {"v": i}, "pub-r1")
            accepted_publish += 1          # 发布在单线程内永不被慢者阻塞
            # 快者立即取走并确认；慢者只取前两条、之后永不确认
            for d in fast.drain_pending():
                assert fast.acknowledge(d.delivery_id) == "removed"
            if i <= 2:
                slow.drain_pending()       # 取走 1,2 但不 ack -> 占满窗口

        fast_got = []
        return accepted_publish, slow, fast, fast_got

    async def run():
        accepted, slow, fast, _ = await scenario()
        # 快者所有消息都已取走（在循环内 drain），用会话状态核对计数
        return accepted, slow.snapshot(), fast.snapshot(), broker.metrics_snapshot()

    accepted, slow_snap, fast_snap, metrics = asyncio.run(run())
    log_case(
        "slow-consumer-bounded",
        {"published": 20, "slow": "sub-all/no-ack", "fast": "sub-status/ack"},
        ["慢者总背压<=inflight2+queue2=4", "超出 drop-newest 计数",
         "快者及时 ack、无积压", "20 次发布全部受理"],
        {"accepted": accepted, "slow": slow_snap, "fast_backlog": fast_snap["backlog"],
         "fast_cursor": fast_snap["delivered_cursor"],
         "dropped_total": metrics["dropped_total"],
         "drop_reasons": {k: v for k, v in metrics["rejection_reasons"].items()
                          if k.startswith("drop:")}},
    )
    assert accepted == 20                       # 发布不被阻塞
    assert fast_snap["delivered_cursor"] == 20  # 快者收到全部 20
    assert fast_snap["backlog"] == 0            # 且都已确认
    assert slow_snap["inflight"] <= 2 and slow_snap["queued"] <= 2
    assert slow_snap["backlog"] <= 4
    accepted_by_slow = slow_snap["inflight"] + slow_snap["queued"]
    assert slow_snap["dropped_total"] == 20 - accepted_by_slow == 16
    assert metrics["dropped_total"] >= 16


def test_slow_consumer_ws_send_queue_abandon_releases_window():
    """真实 WS：发送队列长期堆积导致放弃投递时，窗口被释放且可继续确认恢复。"""
    app = create_app(make_credentials(), max_inflight=2, max_queue=2)
    with TestClient(app) as client:
        with client.websocket_connect("/ws?client_id=ops&token=t-ops") as ops:
            ops.send_text(json.dumps({"action": "subscribe", "id": "s0", "filter": "robots/#"}))
            assert json.loads(ops.receive_text())["status"] == "ok"
            # ops 自己发布、自己订阅；发少量消息验证协议自洽（发送队列在 TestClient
            # 本地回环下不会真正溢出，这里验证 session.abandon 直接腾窗的不变量）
            for i in range(1, 4):
                ops.send_text(json.dumps(
                    {"action": "publish", "id": f"p{i}", "topic": "robots/r1/status",
                     "payload": {"v": i}}))
            # 收到发布 ack（可能夹杂 message）
            msg_delivery_ids = []
            for _ in range(6):
                f = json.loads(ops.receive_text())
                if f.get("type") == "message":
                    msg_delivery_ids.append(f["delivery_id"])
                if f.get("type") == "ack" and f.get("id") == "p3":
                    break
            broker = app.state.broker
            session = broker.registry.get("ops")
            before = session.backlog()
            # 模拟发送队列满：放弃最早一条在途 -> 背压下降、窗口释放
            if msg_delivery_ids:
                assert broker.abandon("ops", msg_delivery_ids[0]) is True
            after = session.backlog()
            log_case(
                "send-queue-abandon",
                {"published": 3, "abandon_delivery_id": msg_delivery_ids[0] if msg_delivery_ids else None},
                ["abandon 立即从 inflight 删除并提升排队", "backlog 严格下降"],
                {"backlog_before": before, "backlog_after": after,
                 "dropped": session.state.dropped_total},
            )
            assert after < before


def test_slow_consumer_reconnect_bounded_and_ordered():
    """慢者断线重连：未确认原序重投，ack 后 queued 按序补投，丢弃数稳定。"""
    app = create_app(make_credentials(), max_inflight=2, max_queue=2)
    with TestClient(app) as client:
        with client.websocket_connect("/ws?client_id=ops&token=t-ops") as ops:
            ops.send_text(json.dumps({"action": "subscribe", "id": "s0", "filter": "robots/#"}))
            assert json.loads(ops.receive_text())["status"] == "ok"
            for i in range(1, 6):  # 在途2 + 排队2 + 丢弃1
                ops.send_text(json.dumps(
                    {"action": "publish", "id": f"p{i}", "topic": "robots/r1/status",
                     "payload": {"v": i}}))
            # 读取直到拿到 5 个发布 ack 与 4 条 message（seq5 被丢弃）
            acks, msgs = 0, []
            while acks < 5:
                f = json.loads(ops.receive_text())
                if f.get("type") == "ack" and str(f.get("id", "")).startswith("p"):
                    acks += 1
                elif f.get("type") == "message":
                    msgs.append(f)
            assert [f["seq"] for f in msgs] == [1, 2]  # 只有窗口内的下发

        session = app.state.broker.registry.get("ops")
        dropped_before = session.state.dropped_total
        assert dropped_before == 1

        with client.websocket_connect("/ws?client_id=ops&token=t-ops") as ws:
            red = []
            while len(red) < 2:
                f = json.loads(ws.receive_text())
                if f.get("type") == "message":
                    red.append(f)
            assert [(f["seq"], f["duplicate"]) for f in red] == [(1, True), (2, True)]

            recovered = []
            for redelivered in red:
                ws.send_text(json.dumps(
                    {"action": "ack", "id": "a", "delivery_id": redelivered["delivery_id"]}))
                # 先收 ack 结果，再收被提升的 queued 消息
                while True:
                    f = json.loads(ws.receive_text())
                    if f.get("type") == "ack" and f.get("action") == "ack":
                        assert f["result"] == "removed"
                        break
                while True:
                    f = json.loads(ws.receive_text())
                    if f.get("type") == "message":
                        recovered.append((f["seq"], f["duplicate"]))
                        break
            log_case(
                "slow-consumer-reconnect",
                {"published": 5, "limits": {"inflight": 2, "queue": 2}},
                ["超背压丢弃 seq5", "重连 1,2 标注重投", "ack 后 3,4 按序补投"],
                {"dropped_before": dropped_before, "recovered": recovered,
                 "dropped_after": session.state.dropped_total},
            )
            assert recovered == [(3, False), (4, False)]
            assert session.state.dropped_total == 1
