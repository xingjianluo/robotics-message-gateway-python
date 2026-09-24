"""FastAPI 应用装配：HTTP 与 WebSocket 入口。

HTTP：
- POST /publish   发布消息（Authorization: Bearer <token>）
- GET  /retained  按过滤器查询保留消息（仅返回有订阅权限的主题）
- GET  /stats     可观测指标（积压、丢弃、会话状态）

WebSocket /ws?token=&client_id=&clean=
- 客户端 -> 服务端：subscribe/unsubscribe/publish/ack
- 服务端 -> 客户端：subscribed/unsubscribed/published/message/error
- 断线重连（同 client_id 且 clean=false）：先按原顺序重投未确认消息，
  再继续投递离线期间积压的消息。
"""
from __future__ import annotations

import asyncio
import logging

from fastapi import FastAPI, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from . import topics
from .auth import ACLStore, Action, DenyReason
from .broker import Broker
from .messages import Message
from .session import Session, SessionManager

logger = logging.getLogger("gateway")

DEFAULT_MAX_INFLIGHT = 64  # 单连接已投递未确认上限，超出则施加背压


def _error(op: str, reason: str) -> dict:
    return {"type": "error", "op": op, "reason": reason}


def create_app(max_queue: int = 100, max_inflight: int = DEFAULT_MAX_INFLIGHT) -> FastAPI:
    app = FastAPI(title="robotics-message-gateway")
    app.state.max_inflight = max_inflight
    app.state.broker = Broker(max_queue=max_queue)
    app.state.acl = ACLStore()
    app.state.sessions = SessionManager()

    def _client_id_from(authorization: str | None) -> str | None:
        if not authorization or not authorization.startswith("Bearer "):
            return None
        return app.state.acl.authenticate(authorization.removeprefix("Bearer ").strip())

    # ---------------- HTTP ----------------

    @app.get("/")
    def root() -> dict:
        return {"service": "robotics-message-gateway", "status": "ok"}

    @app.post("/publish")
    async def http_publish(
        body: dict, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        client_id = _client_id_from(authorization)
        if client_id is None:
            return JSONResponse(
                {"accepted": False, "reason": DenyReason.INVALID_CREDENTIALS.value},
                status_code=401,
            )
        topic = str(body.get("topic", ""))
        verdict = topics.validate_topic(topic)
        if not verdict.ok:
            return JSONResponse(
                {"accepted": False, "reason": verdict.reason.value}, status_code=400
            )
        decision = app.state.acl.authorize(client_id, Action.PUBLISH, topic)
        if not decision.allowed:
            return JSONResponse(
                {"accepted": False, "reason": decision.reason.value}, status_code=403
            )
        msg = Message(
            topic=topic,
            payload=str(body.get("payload", "")),
            retain=bool(body.get("retain", False)),
        )
        result = await app.state.broker.publish(msg)
        return JSONResponse(
            {"accepted": True, "delivered": result.delivered, "seq": msg.seq}
        )

    @app.get("/retained")
    def http_retained(
        filter: str, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        client_id = _client_id_from(authorization)
        if client_id is None:
            return JSONResponse(
                {"reason": DenyReason.INVALID_CREDENTIALS.value}, status_code=401
            )
        verdict = topics.validate_filter(filter)
        if not verdict.ok:
            return JSONResponse({"reason": verdict.reason.value}, status_code=400)
        acl = app.state.acl
        messages = [
            m.to_dict()
            for m in app.state.broker.retained(filter)
            if acl.authorize(client_id, Action.SUBSCRIBE, m.topic).allowed
        ]
        return JSONResponse({"messages": messages})

    @app.get("/stats")
    def http_stats(authorization: str | None = Header(default=None)) -> JSONResponse:
        if _client_id_from(authorization) is None:
            return JSONResponse(
                {"reason": DenyReason.INVALID_CREDENTIALS.value}, status_code=401
            )
        return JSONResponse(
            {
                "broker": app.state.broker.stats(),
                "sessions": app.state.sessions.all_stats(),
            }
        )

    # ---------------- WebSocket ----------------

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket) -> None:
        await ws.accept()
        acl: ACLStore = app.state.acl
        broker: Broker = app.state.broker
        sessions: SessionManager = app.state.sessions

        token = ws.query_params.get("token")
        authed_id = acl.authenticate(token)
        if authed_id is None:
            await ws.send_json(_error("connect", DenyReason.INVALID_CREDENTIALS.value))
            await ws.close(code=4401)
            return

        client_id = ws.query_params.get("client_id") or authed_id
        clean = ws.query_params.get("clean", "false").lower() == "true"
        if clean:
            old = sessions.get(client_id)
            if old is not None:
                for sub in old.subscriptions.values():
                    broker.unsubscribe(sub)
        session = sessions.get_or_create(client_id, clean=clean)
        session.connected = True
        await ws.send_json({"type": "connected", "client_id": client_id})

        incoming: asyncio.Queue[Message] = asyncio.Queue(maxsize=broker.max_queue)
        pumps: dict[str, asyncio.Task] = {}
        inflight: list[Message] = []  # 泵已取出但未入 incoming 的消息（取消时回收）
        ack_event = asyncio.Event()

        async def pump(filt: str) -> None:
            sub = session.subscriptions[filt]
            try:
                while True:
                    msg = await sub.queue.get()
                    inflight.append(msg)
                    try:
                        await incoming.put(msg)
                    except asyncio.CancelledError:
                        raise  # 保留在 inflight 中，由连接收尾按序回收
                    else:
                        inflight.remove(msg)
            except (asyncio.CancelledError, KeyError):
                pass

        # 重连时恢复既有订阅的转发泵（离线积压由此继续投递）
        for filt in session.subscriptions:
            pumps[filt] = asyncio.create_task(pump(filt))

        async def sender() -> None:
            # 1) 重投未确认消息（保持原顺序）
            for msg in list(session.pending):
                await ws.send_json(
                    {"type": "message", "message": msg.to_dict(), "redelivered": True}
                )
                session.redelivered_count += 1
            # 2) 投递上次断线时已出队未发送的消息（仍在 pending 之后、新消息之前）
            while session.ready:
                msg = session.ready.popleft()
                session.pending.append(msg)
                await ws.send_json(
                    {"type": "message", "message": msg.to_dict(), "redelivered": False}
                )
            # 3) 持续投递新消息；未确认数达上限时施加背压
            while True:
                while len(session.pending) >= app.state.max_inflight:
                    ack_event.clear()
                    await ack_event.wait()
                msg = await incoming.get()
                session.pending.append(msg)
                await ws.send_json(
                    {"type": "message", "message": msg.to_dict(), "redelivered": False}
                )

        def handle_subscribe(filt: str) -> dict:
            verdict = topics.validate_filter(filt)
            if not verdict.ok:
                return _error("subscribe", verdict.reason.value)
            decision = acl.authorize_subscribe(authed_id, filt)
            if not decision.allowed:
                return _error("subscribe", decision.reason.value)
            if filt in session.subscriptions:  # 重复订阅：幂等
                return {"type": "subscribed", "filter": filt, "duplicate": True}
            sub = broker.subscribe(filt)  # 保留消息在此先行入队
            session.subscriptions[filt] = sub
            pumps[filt] = asyncio.create_task(pump(filt))
            return {"type": "subscribed", "filter": filt, "duplicate": False}

        def handle_unsubscribe(filt: str) -> dict:
            sub = session.subscriptions.pop(filt, None)
            if sub is None:
                return _error("unsubscribe", "not_subscribed")
            broker.unsubscribe(sub)
            task = pumps.pop(filt, None)
            if task:
                task.cancel()
            return {"type": "unsubscribed", "filter": filt}

        async def handle_publish(data: dict) -> dict:
            topic = str(data.get("topic", ""))
            verdict = topics.validate_topic(topic)
            if not verdict.ok:
                return _error("publish", verdict.reason.value)
            decision = acl.authorize(authed_id, Action.PUBLISH, topic)
            if not decision.allowed:
                return _error("publish", decision.reason.value)
            msg = Message(
                topic=topic,
                payload=str(data.get("payload", "")),
                retain=bool(data.get("retain", False)),
            )
            await broker.publish(msg)
            return {"type": "published", "topic": topic, "seq": msg.seq}

        def handle_ack(data: dict) -> None:
            seq = data.get("seq")
            for msg in list(session.pending):
                if msg.seq == seq:
                    session.pending.remove(msg)
                    ack_event.set()
                    return

        sender_task = asyncio.create_task(sender())
        try:
            while True:
                data = await ws.receive_json()
                op = data.get("op")
                if op == "subscribe":
                    await ws.send_json(handle_subscribe(str(data.get("filter", ""))))
                elif op == "unsubscribe":
                    await ws.send_json(handle_unsubscribe(str(data.get("filter", ""))))
                elif op == "publish":
                    await ws.send_json(await handle_publish(data))
                elif op == "ack":
                    handle_ack(data)
                else:
                    await ws.send_json(_error(str(op), "unknown_op"))
        except WebSocketDisconnect:
            logger.info("client %s disconnected", client_id)
        finally:
            sender_task.cancel()
            for task in pumps.values():
                task.cancel()
            if pumps:
                await asyncio.gather(*pumps.values(), return_exceptions=True)
            # 已出队未发送的消息按序保存到会话，重连后先于新消息投递：
            # 先是 incoming 中较早的消息，再是泵取消瞬间在途的消息
            while True:
                try:
                    session.ready.append(incoming.get_nowait())
                except asyncio.QueueEmpty:
                    break
            session.ready.extend(inflight)
            inflight.clear()
            session.connected = False
            # 订阅保留在会话中：离线期间消息在 broker 有界队列中积压，
            # 重连后依次重投 pending、ready，再消费积压，保证不丢、不重、不乱序。

    return app
