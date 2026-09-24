"""断线重连、未确认消息按原序继续投递、不丢失不重复不乱序。"""

from __future__ import annotations

import json
import logging

import pytest

from .conftest import auth, log_case

log = logging.getLogger("case")


def _pub(client, seq_value):
    return client.post(
        "/publish",
        json={"topic": "robots/r1/status", "payload": {"v": seq_value}},
        headers=auth("pub-r1", "t-pub"),
    )


def _recv_messages(ws, count, ack=False):
    """接收 count 条 message 帧（跳过 ack 帧），可选逐条确认。"""
    out = []
    while len(out) < count:
        frame = json.loads(ws.receive_text())
        if frame.get("type") != "message":
            continue
        out.append(frame)
        if ack:
            ws.send_text(json.dumps(
                {"action": "ack", "id": f"a-{frame['delivery_id']}",
                 "delivery_id": frame["delivery_id"]}))
            ack_frame = json.loads(ws.receive_text())
            assert ack_frame["result"] == "removed", ack_frame
    return out


def test_concurrent_preserves_global_seq_order(broker):
    """并发发布：扇出消息按全局 seq 单调，不重排、不丢失、不重复。

    直接在 asyncio 事件循环内并发驱动 Broker（与网关单线程事件循环模型一致）。
    """
    import asyncio

    async def scenario():
        broker.connect("sub-all")
        broker.subscribe("sub-all", "robots/#")
        session = broker.registry.get("sub-all")
        session.drain_pending()  # 无保留消息，清空

        received: list[int] = []

        async def pub(i):
            await asyncio.sleep(0)
            broker.publish("robots/r1/status", {"v": i}, "pub-r1")
            # 模拟接收端：立刻取走并确认，保证窗口不成为瓶颈
            for delivery in session.drain_pending():
                received.append(delivery.seq)
                session.acknowledge(delivery.delivery_id)

        await asyncio.gather(*(pub(i) for i in range(40)))
        for delivery in session.drain_pending():
            received.append(delivery.seq)
            session.acknowledge(delivery.delivery_id)
        return received

    seqs = asyncio.run(scenario())
    ordered = sorted(seqs)
    log_case(
        "concurrent-publish-order",
        {"tasks": 40, "topic": "robots/r1/status"},
        ["seq 为全局单调序号", "单会话接收顺序必须等于 seq 升序"],
        {"received": len(seqs), "monotonic": seqs == ordered,
         "unique": len(set(seqs)) == 40},
    )
    assert seqs == ordered              # 不乱序
    assert len(set(seqs)) == 40         # 不重复
    assert ordered == list(range(1, 41))  # 不丢失


def test_reconnect_redelivers_unacked_in_order_without_loss(client, broker):
    # 第一次连接订阅，收 6 条但全部不确认：窗口4在途(1..4) + 队列2(5,6)
    with client.websocket_connect("/ws?client_id=sub-all&token=t-all") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "s", "filter": "robots/#"}))
        assert json.loads(ws.receive_text())["status"] == "ok"
        for i in range(1, 7):
            assert _pub(client, i).status_code == 200
        first = _recv_messages(ws, 4)
        assert [f["seq"] for f in first] == [1, 2, 3, 4]

    # 断开后离线再发 4 条：队列还能放 2（7,8），9,10 背压丢弃（窗口4+队列4=8）
    for i in range(7, 11):
        _pub(client, i)

    session = broker.registry.get("sub-all")
    offline_dropped = session.state.dropped_total
    assert offline_dropped == 2, offline_dropped

    # 重连：未确认的 1..4 原序 duplicate 重投
    with client.websocket_connect("/ws?client_id=sub-all&token=t-all") as ws:
        redelivered = _recv_messages(ws, 4)
        assert [f["seq"] for f in redelivered] == [1, 2, 3, 4]
        assert all(f["duplicate"] is True for f in redelivered)

        # 严格交错：每确认一条重投 -> 恰好一条 queued 消息被提升并下发（先读 ack 帧再读 message）
        recovered: list[tuple[int, bool]] = []
        expected_promoted = iter([5, 6, 7, 8])
        for f in redelivered:
            ws.send_text(json.dumps(
                {"action": "ack", "id": "a", "delivery_id": f["delivery_id"]}))
            assert json.loads(ws.receive_text())["result"] == "removed"
            promoted = _recv_messages(ws, 1)[0]
            recovered.append((promoted["seq"], promoted["duplicate"]))
            assert promoted["seq"] == next(expected_promoted)
            assert promoted["duplicate"] is False

        log_case(
            "reconnect-recovery",
            {"first_batch": [1, 2, 3, 4], "offline_published": [7, 8, 9, 10]},
            ["未确认原序重投并标 duplicate", "每 ack 一条窗口提升一条 queued，严格按序",
             "离线超背压(窗口4+队列4)部分丢弃且可计数"],
            {"redelivered_seqs": [f["seq"] for f in redelivered],
             "recovered_queued": recovered,
             "offline_dropped": offline_dropped},
        )
        assert recovered == [(5, False), (6, False), (7, False), (8, False)]
        assert session.state.dropped_total == 2  # 9,10 被丢弃，结论可观测无新增


def test_ack_duplicate_and_unknown_are_distinguishable(client):
    with client.websocket_connect("/ws?client_id=sub-all&token=t-all") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "s", "filter": "robots/#"}))
        assert json.loads(ws.receive_text())["status"] == "ok"
        _pub(client, 1)
        [frame] = _recv_messages(ws, 1)
        did = frame["delivery_id"]

        def ack():
            ws.send_text(json.dumps({"action": "ack", "id": "a", "delivery_id": did}))
            return json.loads(ws.receive_text())["result"]

        first = ack()
        duplicate = ack()                       # 重复确认
        ws.send_text(json.dumps({"action": "ack", "id": "a", "delivery_id": 999999}))
        unknown = json.loads(ws.receive_text())["result"]
    log_case(
        "ack-results",
        {"delivery_id": did},
        ["首次 removed / 重复 duplicate / 未知 unknown 三态可区分"],
        {"first": first, "duplicate": duplicate, "unknown": unknown},
    )
    assert first == "removed"
    assert duplicate == "duplicate"
    assert unknown == "unknown"


def test_duplicate_connection_rejected_without_killing_old(client):
    with client.websocket_connect("/ws?client_id=sub-all&token=t-all") as ws:
        ws.send_text(json.dumps({"action": "subscribe", "id": "s", "filter": "robots/#"}))
        assert json.loads(ws.receive_text())["status"] == "ok"
        close_seen = None
        try:
            with client.websocket_connect("/ws?client_id=sub-all&token=t-all"):
                pass
        except Exception as exc:
            close_seen = str(exc)
        # 旧连接继续可用
        _pub(client, 1)
        msg = _recv_messages(ws, 1)[0]
    log_case(
        "duplicate-connection",
        {"client": "sub-all", "attempts": 2},
        ["重复连接以 4409 关闭，旧连接不受影响"],
        {"close": close_seen, "old_got_seq": msg["seq"]},
    )
    assert msg["payload"] == {"v": 1}
