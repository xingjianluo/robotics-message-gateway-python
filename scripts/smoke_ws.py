"""本地端到端联调脚本（需先启动：uvicorn main:app --port 8000）。

演示：HTTP 保留发布 -> WS 通配订阅收到保留 -> 实时消息 -> 断线 ->
重连后未确认消息标 duplicate 原序重投 -> ack -> 指标查看。

用法：uv run python scripts/smoke_ws.py [ws_url]
"""

from __future__ import annotations

import asyncio
import json
import sys
import urllib.request

import websockets

BASE_HTTP = "http://127.0.0.1:8000"
WS_URL = sys.argv[1] if len(sys.argv) > 1 else "ws://127.0.0.1:8000/ws"


def http_publish(topic: str, payload, retain: bool = False) -> None:
    import base64

    token = base64.b64encode(b"robot-r1:token-r1").decode()
    req = urllib.request.Request(
        f"{BASE_HTTP}/publish",
        data=json.dumps({"topic": topic, "payload": payload, "retain": retain}).encode(),
        headers={"Content-Type": "application/json", "Authorization": f"Basic {token}"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        print("HTTP publish:", resp.read().decode())


async def recv_until(ws, predicate):
    while True:
        frame = json.loads(await ws.recv())
        if predicate(frame):
            return frame


async def main() -> None:
    # 1) 先发一条保留消息
    http_publish("robots/r1/status", {"v": "retained-1"}, retain=True)

    url = f"{WS_URL}?client_id=dashboard&token=token-dash"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"action": "subscribe", "id": "s1",
                                  "filter": "robots/+/status"}))
        ack = await recv_until(ws, lambda f: f.get("type") == "ack" and f.get("id") == "s1")
        print("subscribe:", ack["status"], "retained_matched=", ack.get("retained_matched"))
        retained = await recv_until(ws, lambda f: f.get("type") == "message")
        print("retained message:", retained["payload"], "retained=", retained["retained"])
        # 故意不确认 -> 断线后应重投

    print("== disconnected (message unacknowledged) ==")
    async with websockets.connect(url) as ws:
        red = await recv_until(ws, lambda f: f.get("type") == "message")
        print("redelivered:", red["payload"], "duplicate=", red["duplicate"])
        assert red["duplicate"] is True
        await ws.send(json.dumps({"action": "ack", "id": "a1",
                                  "delivery_id": red["delivery_id"]}))
        result = await recv_until(ws, lambda f: f.get("type") == "ack")
        print("ack:", result["result"])
    print("SMOKE OK")


if __name__ == "__main__":
    asyncio.run(main())
