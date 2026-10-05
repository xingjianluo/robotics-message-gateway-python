"""FastAPI 应用装配：HTTP 与 WebSocket 入口。

HTTP：
- POST /publish   发布消息（Authorization: Bearer <token>）
- GET  /retained  按过滤器查询保留消息（仅返回有订阅权限的主题）
- GET  /stats     可观测指标（积压、丢弃、会话状态、治理指标）

运行期治理（管理员 token 保护，无需重启）：
- POST   /admin/clients            新增/更换账号 token
- DELETE /admin/clients/{id}       注销账号（在线连接被终止、会话回收）
- PUT    /admin/clients/{id}/acl   整体替换账号的发布/订阅主题范围；
                                   收紧后在线连接中不再被允许的订阅立即失效
- GET    /admin/clients            列出账号与规则（不含 token）

WebSocket /ws?token=&client_id=&clean=
- 客户端 -> 服务端：subscribe/unsubscribe/publish/ack
- 服务端 -> 客户端：subscribed/unsubscribed/published/message/error
- 身份绑定：client_id 缺省为凭据身份；显式声明时必须与凭据归属一致，
  否则以 4403 拒绝（identity_mismatch），不影响其他连接。
- 接管语义：同一身份再次连接时，新连接顶掉旧连接（旧连接收到
  session_taken_over 后以 4409 关闭）；旧连接未确认的消息留在会话
  pending 中按原顺序由新连接重投，不重、不乱序，接管次数计入指标。
- 断线重连（clean=false）：先按原顺序重投未确认消息，
  再继续投递离线期间积压的消息。
- 会话过期：离线超过 session_expiry 秒的会话被回收（订阅取消、
  积压与未确认消息丢弃并计数），内存真正释放。
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from collections.abc import Iterable
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass, field

from fastapi import FastAPI, Header, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse

from . import topics
from .auth import ACLStore, Action, DenyReason
from .broker import Broker
from .messages import Message
from .session import (
    METRIC_ACL_REVOKED_MESSAGES,
    METRIC_ACL_REVOKED_SUBSCRIPTIONS,
    SessionManager,
)

logger = logging.getLogger("gateway")

DEFAULT_MAX_INFLIGHT = 64  # 单连接已投递未确认上限，超出则施加背压
DEFAULT_SESSION_EXPIRY = 3600.0  # 离线会话过期时间（秒）
DEFAULT_REAP_INTERVAL = 60.0  # 过期会话扫描间隔（秒）
DEFAULT_ADMIN_TOKENS = ("t-admin",)  # 本地开发默认管理员凭据

# WebSocket 关闭码
CLOSE_INVALID_CREDENTIALS = 4401
CLOSE_IDENTITY_MISMATCH = 4403
CLOSE_SESSION_TAKEN_OVER = 4409


def _error(op: str, reason: str) -> dict:
    return {"type": "error", "op": op, "reason": reason}


@dataclass
class _Connection:
    """一条在线连接的运行态（供接管与运行期权限回收协调使用）。"""

    client_id: str
    ws: WebSocket
    incoming: asyncio.Queue[Message]
    done: asyncio.Event = field(default_factory=asyncio.Event)
    # 服务器主动关闭（接管/账号注销）时置位，用于唤醒接收循环
    close_event: asyncio.Event = field(default_factory=asyncio.Event)
    pumps: dict[str, asyncio.Task] = field(default_factory=dict)
    inflight: list[Message] = field(default_factory=list)  # 泵已取出但未入 incoming


def create_app(
    max_queue: int = 100,
    max_inflight: int = DEFAULT_MAX_INFLIGHT,
    session_expiry: float | None = DEFAULT_SESSION_EXPIRY,
    reap_interval: float = DEFAULT_REAP_INTERVAL,
    max_offline_backlog: int | None = None,
    admin_tokens: Iterable[str] = DEFAULT_ADMIN_TOKENS,
) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        reaper: asyncio.Task | None = None
        if session_expiry and reap_interval:

            async def _reap_loop() -> None:
                while True:
                    await asyncio.sleep(reap_interval)
                    reaped = app.state.sessions.reap_expired(
                        time.time(), session_expiry, app.state.broker
                    )
                    if reaped:
                        logger.info("reaped expired sessions: %s", reaped)

            reaper = asyncio.create_task(_reap_loop())
        yield
        if reaper is not None:
            reaper.cancel()

    app = FastAPI(title="robotics-message-gateway", lifespan=lifespan)
    app.state.max_inflight = max_inflight
    app.state.broker = Broker(max_queue=max_queue)
    app.state.acl = ACLStore()
    app.state.acl.set_admin_tokens(admin_tokens)
    app.state.sessions = SessionManager(backlog_cap=max_offline_backlog)
    app.state.connections = {}  # client_id -> _Connection（仅在线连接）

    def _client_id_from(authorization: str | None) -> str | None:
        if not authorization or not authorization.startswith("Bearer "):
            return None
        return app.state.acl.authenticate(authorization.removeprefix("Bearer ").strip())

    def _admin_denied(authorization: str | None) -> JSONResponse | None:
        """管理员鉴权：是管理员返回 None；否则返回可区分的 401/403。"""
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization.removeprefix("Bearer ").strip()
        if token is not None and app.state.acl.authenticate_admin(token):
            return None
        if token is not None and app.state.acl.authenticate(token) is not None:
            return JSONResponse(
                {"reason": DenyReason.NOT_ALLOWED.value}, status_code=403
            )
        return JSONResponse(
            {"reason": DenyReason.INVALID_CREDENTIALS.value}, status_code=401
        )

    async def _close_connection(
        client_id: str, reason: DenyReason, code: int
    ) -> None:
        """终止指定身份的在线连接并等待其收尾完成（不影响其他连接）。"""
        conn: _Connection | None = app.state.connections.get(client_id)
        if conn is None:
            return
        with suppress(Exception):  # 连接可能已断开
            await conn.ws.send_json(_error("connect", reason.value))
        with suppress(Exception):
            await conn.ws.close(code=code)
        conn.close_event.set()
        try:
            await asyncio.wait_for(conn.done.wait(), timeout=5)
        except TimeoutError:
            logger.warning("connection %s did not finish cleanup in time", client_id)

    async def _revoke_disallowed(client_id: str) -> None:
        """权限收紧的追溯生效：撤销该身份不再被允许的订阅。

        - 取消 broker 订阅、停止转发泵，并通知在线客户端；
        - 已出队未投递（ready/incoming/inflight）中仅匹配被撤销过滤器的
          消息被丢弃并计数，其余消息保持原有相对顺序；
        - 已投递未确认（pending）的消息原样保留，不丢、不乱序；
        - 只触碰该身份的会话与连接，不影响其他客户端。
        """
        acl: ACLStore = app.state.acl
        broker: Broker = app.state.broker
        sessions: SessionManager = app.state.sessions
        session = sessions.get(client_id)
        if session is None:
            return
        revoked = [
            f
            for f in session.subscriptions
            if not acl.authorize_subscribe(client_id, f).allowed
        ]
        if not revoked:
            return
        conn: _Connection | None = app.state.connections.get(client_id)
        stopped: list[asyncio.Task] = []
        for filt in revoked:
            sub = session.subscriptions.pop(filt)
            broker.unsubscribe(sub)
            sessions.metrics[METRIC_ACL_REVOKED_SUBSCRIPTIONS] += 1
            if conn is not None:
                task = conn.pumps.pop(filt, None)
                if task is not None:
                    task.cancel()
                    stopped.append(task)
        if stopped:  # 等泵停止，避免撤销后仍有消息进入 incoming
            await asyncio.gather(*stopped, return_exceptions=True)

        remaining = list(session.subscriptions)

        def keep(msg: Message) -> bool:
            return any(topics.match(f, msg.topic) for f in remaining)

        dropped = 0
        kept_ready = deque(m for m in session.ready if keep(m))
        dropped += len(session.ready) - len(kept_ready)
        session.ready = kept_ready
        if conn is not None:
            buffered: list[Message] = []
            while True:
                try:
                    buffered.append(conn.incoming.get_nowait())
                except asyncio.QueueEmpty:
                    break
            for msg in buffered:
                if keep(msg):
                    conn.incoming.put_nowait(msg)
                else:
                    dropped += 1
            before = len(conn.inflight)
            conn.inflight[:] = [m for m in conn.inflight if keep(m)]
            dropped += before - len(conn.inflight)
        sessions.metrics[METRIC_ACL_REVOKED_MESSAGES] += dropped
        if conn is not None:
            for filt in revoked:
                try:
                    await conn.ws.send_json(
                        {
                            "type": "unsubscribed",
                            "filter": filt,
                            "reason": "acl_revoked",
                        }
                    )
                except (WebSocketDisconnect, RuntimeError, OSError):
                    # 连接可能刚好断开
                    logger.debug("notify acl_revoked to %s failed", client_id)
                    break

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
                "gateway": {
                    **app.state.sessions.metrics,
                    "online_connections": len(app.state.connections),
                },
            }
        )

    # ---------------- 运行期治理（管理员） ----------------

    @app.post("/admin/clients")
    async def admin_create_client(
        body: dict, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        if (denied := _admin_denied(authorization)) is not None:
            return denied
        client_id = body.get("client_id")
        token = body.get("token")
        if (
            not isinstance(client_id, str)
            or not client_id
            or not isinstance(token, str)
            or not token
        ):
            return JSONResponse({"reason": "invalid_request"}, status_code=400)
        acl: ACLStore = app.state.acl
        owner = acl.authenticate(token)
        if owner is not None and owner != client_id:
            return JSONResponse({"reason": "token_in_use"}, status_code=409)
        created = not acl.client_exists(client_id)
        acl.add_client(client_id, token)
        logger.info("admin: client %s %s", client_id, "created" if created else "updated")
        return JSONResponse(
            {"client_id": client_id, "created": created},
            status_code=201 if created else 200,
        )

    @app.delete("/admin/clients/{client_id}")
    async def admin_delete_client(
        client_id: str, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        if (denied := _admin_denied(authorization)) is not None:
            return denied
        acl: ACLStore = app.state.acl
        if not acl.remove_client(client_id):
            return JSONResponse({"reason": "not_found"}, status_code=404)
        # 立即生效：终止在线连接（4401），回收会话（订阅取消、积压释放）
        await _close_connection(
            client_id, DenyReason.ACCOUNT_REVOKED, CLOSE_INVALID_CREDENTIALS
        )
        session = app.state.sessions.remove(client_id)
        if session is not None:
            for sub in session.subscriptions.values():
                app.state.broker.unsubscribe(sub)
        logger.info("admin: client %s deleted", client_id)
        return JSONResponse({"deleted": True, "client_id": client_id})

    @app.put("/admin/clients/{client_id}/acl")
    async def admin_set_acl(
        client_id: str,
        body: dict,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        if (denied := _admin_denied(authorization)) is not None:
            return denied
        acl: ACLStore = app.state.acl
        if not acl.client_exists(client_id):
            return JSONResponse({"reason": "not_found"}, status_code=404)
        publish = body.get("publish", [])
        subscribe = body.get("subscribe", [])
        if not (
            isinstance(publish, list)
            and all(isinstance(f, str) for f in publish)
            and isinstance(subscribe, list)
            and all(isinstance(f, str) for f in subscribe)
        ):
            return JSONResponse({"reason": "invalid_request"}, status_code=400)
        try:
            acl.set_client_rules(client_id, publish, subscribe)
        except ValueError as exc:
            return JSONResponse(
                {"reason": "invalid_filter", "detail": str(exc)}, status_code=400
            )
        # 追溯生效：在线连接中不再被允许的订阅立即失效
        await _revoke_disallowed(client_id)
        logger.info("admin: client %s acl updated", client_id)
        return JSONResponse(
            {"client_id": client_id, "publish": publish, "subscribe": subscribe}
        )

    @app.get("/admin/clients")
    def admin_list_clients(
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        if (denied := _admin_denied(authorization)) is not None:
            return denied
        return JSONResponse({"clients": app.state.acl.list_clients()})

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

        # 身份绑定：声明的 client_id 必须与凭据归属一致
        client_id = ws.query_params.get("client_id") or authed_id
        if client_id != authed_id:
            await ws.send_json(_error("connect", DenyReason.IDENTITY_MISMATCH.value))
            await ws.close(code=CLOSE_IDENTITY_MISMATCH)
            return

        clean = ws.query_params.get("clean", "false").lower() == "true"
        conn = _Connection(
            client_id=client_id,
            ws=ws,
            incoming=asyncio.Queue(maxsize=broker.max_queue),
        )

        # 接管语义：同一身份已有在线连接时，新连接顶掉旧连接；
        # 等待旧连接收尾（未确认消息留在 pending，已出队消息按序存入 ready），
        # 保证该身份的消息不重、不乱序。
        prev: _Connection | None = app.state.connections.get(client_id)
        if prev is not None:
            old_session = sessions.get(client_id)
            if old_session is not None:
                sessions.note_takeover(old_session)
            with suppress(Exception):  # 旧连接可能已自行断开
                await prev.ws.send_json(
                    _error("connect", DenyReason.SESSION_TAKEN_OVER.value)
                )
            with suppress(Exception):
                await prev.ws.close(code=CLOSE_SESSION_TAKEN_OVER)
            prev.close_event.set()
            try:
                await asyncio.wait_for(prev.done.wait(), timeout=5)
            except TimeoutError:
                logger.warning("takeover of %s: previous connection slow", client_id)
        app.state.connections[client_id] = conn

        if clean:
            old = sessions.get(client_id)
            if old is not None:
                for sub in old.subscriptions.values():
                    broker.unsubscribe(sub)
        session = sessions.get_or_create(client_id, clean=clean)
        session.connected = True
        session.disconnected_at = None
        await ws.send_json({"type": "connected", "client_id": client_id})

        incoming = conn.incoming
        pumps = conn.pumps
        inflight = conn.inflight
        ack_event = asyncio.Event()

        async def pump(filt: str) -> None:
            sub = session.subscriptions[filt]
            try:
                while True:
                    msg = await sub.queue.get()
                    inflight.append(msg)
                    # 取消时消息保留在 inflight 中，由连接收尾按序回收
                    await incoming.put(msg)
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
            sub.overflow_hook = session.note_enqueued_offline
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
                # 接收与服务器主动关闭（接管/注销）竞争：
                # TestClient 不会在服务器 close 后注入 disconnect，
                # 仅靠 receive_json 无法感知服务器侧关闭
                recv_task = asyncio.create_task(ws.receive_json())
                close_task = asyncio.create_task(conn.close_event.wait())
                done, _ = await asyncio.wait(
                    {recv_task, close_task}, return_when=asyncio.FIRST_COMPLETED
                )
                if recv_task in done:
                    close_task.cancel()
                    data = recv_task.result()  # WebSocketDisconnect 在此上抛
                else:
                    recv_task.cancel()
                    logger.info("client %s closed by server", client_id)
                    break
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
            # 取消已发出：泵只可能停在 queue.get()/incoming.put() 两处 await，
            # 取消后不会再有消息进入 incoming，因此可以同步回收状态，
            # 保证断线后的会话状态对后续请求（含 /stats）立即可见。
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
            session.disconnected_at = time.time()
            if app.state.connections.get(client_id) is conn:
                del app.state.connections[client_id]
            if pumps:  # 等待泵任务真正结束（仅收尾，不再改变会话状态）
                await asyncio.gather(*pumps.values(), return_exceptions=True)
            conn.done.set()
            # 订阅保留在会话中：离线期间消息在 broker 有界队列中积压，
            # 重连后依次重投 pending、ready，再消费积压，保证不丢、不重、不乱序。

    return app
