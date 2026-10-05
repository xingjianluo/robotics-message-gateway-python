"""FastAPI 应用装配：HTTP 与 WebSocket 入口。

HTTP：
- POST /publish   发布消息（Authorization: Bearer <token>）
- GET  /retained  按过滤器查询保留消息（仅返回有订阅权限的主题）
- GET  /stats     可观测指标（积压、丢弃、会话状态、接管/收回计数）
- /admin/*        运行期账号与权限治理（管理凭据保护，见 admin.py）

WebSocket /ws?token=&client_id=&clean=
- 客户端 -> 服务端：subscribe/unsubscribe/publish/ack
- 服务端 -> 客户端：subscribed/unsubscribed/published/message/error/displaced
- client_id 必须与凭据身份一致，否则以 4403 拒绝（identity_mismatch）。
- 同一身份新连接会顶掉旧连接（takeover）：旧连接收到 displaced 帧后以
  4000 关闭，其未确认消息按原顺序由会话交接给新连接，不重、不乱序，
  接管次数计入会话指标。
- 断线重连（同 client_id 且 clean=false）：先按原顺序重投未确认消息，
  再继续投递离线期间积压的消息。
- 权限被运行期收紧后，不再被允许的订阅立即失效（permission_revoked），
  在途的该过滤器消息被清除，未确认消息不受影响。
"""
from __future__ import annotations

import asyncio
import logging
from collections import deque
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from . import topics
from .admin import AdminEvent, create_admin_router
from .auth import ACLStore, Action, DenyReason
from .broker import Broker
from .expiry import SessionReaper
from .messages import Message
from .session import Session, SessionManager

logger = logging.getLogger("gateway")

DEFAULT_MAX_INFLIGHT = 64  # 单连接已投递未确认上限，超出则施加背压

# WebSocket 关闭码
CLOSE_INVALID_CREDENTIALS = 4401
CLOSE_IDENTITY_MISMATCH = 4403
CLOSE_TAKEOVER = 4000


def _error(op: str, reason: str) -> dict:
    return {"type": "error", "op": op, "reason": reason}


class Connection:
    """一条在线连接的运行态：转发队列、泵、在途消息、关闭/交接协调。"""

    def __init__(self, ws: WebSocket, session: Session, max_queue: int) -> None:
        self.ws = ws
        self.session = session
        self.incoming: asyncio.Queue[Message] = asyncio.Queue(maxsize=max_queue)
        self.pumps: dict[str, asyncio.Task] = {}
        self.inflight: list[Message] = []  # 泵已取出但未入 incoming 的消息
        self.ack_event = asyncio.Event()
        self.close_event = asyncio.Event()  # 外部请求关闭（接管/账号注销）
        self.close_reason = ""
        self.handover_done = asyncio.Event()  # 收尾完成（在途消息已回会话）

    def request_close(self, reason: str) -> None:
        self.close_reason = reason
        self.close_event.set()

    def start_pump(self, filt: str) -> None:
        self.pumps[filt] = asyncio.create_task(self._pump(filt))

    def stop_pump(self, filt: str) -> None:
        task = self.pumps.pop(filt, None)
        if task:
            task.cancel()

    async def _pump(self, filt: str) -> None:
        sub = self.session.subscriptions[filt]
        try:
            while True:
                msg = await sub.queue.get()
                self.inflight.append(msg)
                # 若泵在 put 期间被取消，消息保留在 inflight 中，
                # 由连接收尾按序回收进会话
                await self.incoming.put(msg)
                self.inflight.remove(msg)
        except (asyncio.CancelledError, KeyError):
            pass

    def purge_filter(self, filt: str) -> None:
        """权限收回后，清除在途但尚未投递的该过滤器消息（不含未确认消息）。"""
        kept: list[Message] = []
        while True:
            try:
                msg = self.incoming.get_nowait()
            except asyncio.QueueEmpty:
                break
            if not topics.match(filt, msg.topic):
                kept.append(msg)
        for msg in kept:  # 只减不增，put_nowait 不会满
            self.incoming.put_nowait(msg)
        self.inflight[:] = [
            m for m in self.inflight if not topics.match(filt, m.topic)
        ]
        self.session.ready = deque(
            m for m in self.session.ready if not topics.match(filt, m.topic)
        )


def create_app(
    max_queue: int = 100,
    max_inflight: int = DEFAULT_MAX_INFLIGHT,
    session_ttl: float = 0.0,
    max_offline_backlog: int = 0,
    reap_interval: float = 1.0,
) -> FastAPI:
    broker = Broker(max_queue=max_queue)
    sessions = SessionManager()
    reaper = SessionReaper(
        sessions,
        broker,
        session_ttl=session_ttl,
        max_offline_backlog=max_offline_backlog,
        interval=reap_interval,
    )

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await reaper.start()
        yield
        await reaper.stop()

    app = FastAPI(title="robotics-message-gateway", lifespan=lifespan)
    app.state.max_inflight = max_inflight
    app.state.broker = broker
    app.state.acl = ACLStore()
    app.state.sessions = sessions
    app.state.reaper = reaper

    def _client_id_from(authorization: str | None) -> str | None:
        if not authorization or not authorization.startswith("Bearer "):
            return None
        return app.state.acl.authenticate(authorization.removeprefix("Bearer ").strip())

    # ---------------- 运行期治理的追溯生效 ----------------

    async def _on_admin_event(event: AdminEvent) -> None:
        acl: ACLStore = app.state.acl
        broker: Broker = app.state.broker
        sessions: SessionManager = app.state.sessions
        session = sessions.get(event.client_id)
        if session is None:
            return
        conn: Connection | None = session.active
        if event.kind == "account_removed":
            # 身份已注销：在线连接立即断开，全部订阅静默收回
            # （连接随即关闭，无需逐条通知；其余客户端不受影响）
            if conn is not None:
                conn.request_close("account_removed")
            for filt, sub in list(session.subscriptions.items()):
                broker.unsubscribe(sub)
                session.revoked_subscriptions += 1
                if conn is not None:
                    conn.stop_pump(filt)
            session.subscriptions.clear()
            return
        # 追溯收回：重新校验该身份的既有订阅，不再被允许的立即失效
        for filt in list(session.subscriptions):
            if acl.authorize_subscribe(session.client_id, filt).allowed:
                continue
            sub = session.subscriptions.pop(filt)
            broker.unsubscribe(sub)
            session.revoked_subscriptions += 1
            if conn is not None:
                conn.stop_pump(filt)
                conn.purge_filter(filt)
                try:
                    await conn.ws.send_json(
                        {
                            "type": "unsubscribed",
                            "filter": filt,
                            "reason": "permission_revoked",
                        }
                    )
                except Exception:  # noqa: BLE001, S110 连接可能正在关闭，收回本身已完成
                    pass

    app.include_router(create_admin_router(app.state.acl, _on_admin_event))

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
        app.state.reaper.sweep()  # 离线积压上限即时生效
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
                "expiry": app.state.reaper.stats.to_dict(),
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
            await ws.close(code=CLOSE_INVALID_CREDENTIALS)
            return

        # 连接声明的身份必须与凭据归属一致，防止冒用他人会话
        client_id = ws.query_params.get("client_id") or authed_id
        if client_id != authed_id:
            await ws.send_json(_error("connect", "identity_mismatch"))
            await ws.close(code=CLOSE_IDENTITY_MISMATCH)
            return

        clean = ws.query_params.get("clean", "false").lower() == "true"
        if clean:
            old = sessions.get(client_id)
            if old is not None:
                if old.active is not None:
                    old.active.request_close("takeover")
                for sub in old.subscriptions.values():
                    broker.unsubscribe(sub)
        session = sessions.get_or_create(client_id, clean=clean)
        conn = Connection(ws, session, broker.max_queue)
        async with session.connect_lock:
            old_conn: Connection | None = session.active
            if old_conn is not None:
                # 接管语义：后连上的顶掉先前的；等旧连接收尾完成，
                # 其在途消息按序回到会话后再开始投递，保证不重、不乱序
                session.takeovers += 1
                old_conn.request_close("takeover")
                await old_conn.handover_done.wait()
            session.active = conn
            session.connected = True
        await ws.send_json({"type": "connected", "client_id": client_id})

        # 重连时恢复既有订阅的转发泵（离线积压由此继续投递）
        for filt in session.subscriptions:
            conn.start_pump(filt)

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
                    conn.ack_event.clear()
                    await conn.ack_event.wait()
                msg = await conn.incoming.get()
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
            conn.start_pump(filt)
            return {"type": "subscribed", "filter": filt, "duplicate": False}

        def handle_unsubscribe(filt: str) -> dict:
            sub = session.subscriptions.pop(filt, None)
            if sub is None:
                return _error("unsubscribe", "not_subscribed")
            broker.unsubscribe(sub)
            conn.stop_pump(filt)
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
            app.state.reaper.sweep()  # 离线积压上限即时生效
            return {"type": "published", "topic": topic, "seq": msg.seq}

        def handle_ack(data: dict) -> None:
            seq = data.get("seq")
            for msg in list(session.pending):
                if msg.seq == seq:
                    session.pending.remove(msg)
                    conn.ack_event.set()
                    return

        sender_task = asyncio.create_task(sender())
        close_task = asyncio.create_task(conn.close_event.wait())
        try:
            while True:
                recv_task = asyncio.create_task(ws.receive_json())
                done, _ = await asyncio.wait(
                    {recv_task, close_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if close_task in done:  # 被接管或账号被注销
                    recv_task.cancel()
                    try:
                        await ws.send_json(
                            {"type": "displaced", "reason": conn.close_reason}
                        )
                        await ws.close(
                            code=CLOSE_TAKEOVER
                            if conn.close_reason == "takeover"
                            else CLOSE_INVALID_CREDENTIALS
                        )
                    except Exception:  # noqa: BLE001, S110 关闭通知尽力而为
                        pass
                    break
                recv_task_done = recv_task
                data = recv_task_done.result()
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
            close_task.cancel()
            sender_task.cancel()
            for task in conn.pumps.values():
                task.cancel()
            if conn.pumps:
                # 客户端 abrupt 断开时外层可能取消本协程；屏蔽取消，
                # 确保泵退出与在途消息回收一定完成（不丢、不乱序）
                wait = asyncio.gather(*conn.pumps.values(), return_exceptions=True)
                while not wait.done():
                    try:
                        await asyncio.shield(wait)
                    except asyncio.CancelledError:
                        continue
            # 已出队未发送的消息按序保存到会话，重连后先于新消息投递：
            # 先是 incoming 中较早的消息，再是泵取消瞬间在途的消息
            while True:
                try:
                    session.ready.append(conn.incoming.get_nowait())
                except asyncio.QueueEmpty:
                    break
            session.ready.extend(conn.inflight)
            conn.inflight.clear()
            if session.active is conn:
                session.note_disconnected()
            conn.handover_done.set()
            # 订阅保留在会话中：离线期间消息在 broker 有界队列中积压，
            # 重连后依次重投 pending、ready，再消费积压，保证不丢、不重、不乱序。

    return app
