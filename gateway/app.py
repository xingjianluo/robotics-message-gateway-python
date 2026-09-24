"""FastAPI 应用装配：HTTP + WebSocket 路由。

本地直接运行：``uvicorn gateway.app:app --reload``（或 ``python -m main``）。

传输约定：
- HTTP  Basic 认证（``Authorization: Basic base64(client_id:token)``），
  仅用于 ``POST /publish``；查询类接口默认开放便于本地验证。
- WS    连接参数 ``?client_id=..&token=..``（或同样支持 Authorization 头）。
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import logging
from typing import Any

from fastapi import Body, FastAPI, Header, Query, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .auth import Authenticator, ClientCredential, Permission
from .broker import Broker
from .errors import ErrorCode, GatewayError
from .protocol import (
    encode_ack,
    encode_delivery,
    encode_error,
    parse_client_message,
)

logger = logging.getLogger(__name__)


# ---------- HTTP 请求模型（模块顶层，避免 Pydantic 前向引用问题） ----------
class PublishRequest(BaseModel):
    topic: str
    payload: Any = None
    retain: bool = False


class PermissionModel(BaseModel):
    filter: str
    actions: list[str] = Field(default_factory=lambda: ["pub", "sub"])


class CredentialModel(BaseModel):
    token: str
    permissions: list[PermissionModel] = Field(default_factory=list)


# 每条连接的异步发送队列上限：约为在途窗口的 2 倍，用于吸收瞬时突发
SEND_QUEUE_FACTOR = 2


def default_credentials() -> dict[str, ClientCredential]:
    """开箱即用的本地演示账号。"""
    pubsub = frozenset({"pub", "sub"})
    pub = frozenset({"pub"})
    sub = frozenset({"sub"})
    return {
        "robot-r1": ClientCredential(
            "robot-r1", "token-r1",
            (Permission("robots/r1/status", pub), Permission("robots/r1/cmd", pubsub)),
        ),
        "dashboard": ClientCredential(
            "dashboard", "token-dash",
            (Permission("robots/+/status", sub), Permission("robots/#", sub)),
        ),
        "ops": ClientCredential("ops", "token-ops", (Permission("#", pubsub),)),
    }


def create_app(
    credentials: dict[str, ClientCredential] | None = None,
    max_inflight: int = 100,
    max_queue: int = 1000,
) -> FastAPI:
    auth = Authenticator(credentials if credentials is not None else default_credentials())
    broker = Broker(auth, max_inflight=max_inflight, max_queue=max_queue)
    send_queue_max = max(4, max_inflight * SEND_QUEUE_FACTOR)

    app = FastAPI(title="robotics-message-gateway", version="0.1.0")
    app.state.broker = broker
    app.state.auth = auth

    # ---------- 错误处理 ----------
    def error_body(code: ErrorCode, message: str) -> dict:
        return {"status": "error", "code": code.value, "message": message}

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
        status_code = {
            ErrorCode.MISSING_CREDENTIALS: 401,
            ErrorCode.INVALID_CREDENTIALS: 401,
            ErrorCode.PUBLISH_FORBIDDEN: 403,
            ErrorCode.SUBSCRIBE_FORBIDDEN: 403,
        }.get(exc.code, 400)
        broker.metrics.reject(exc.code.value)
        logger.warning("http reject code=%s path=%s", exc.code.value, request.url.path)
        return JSONResponse(status_code=status_code, content=error_body(exc.code, str(exc)))

    # ---------- HTTP ----------
    def _basic_credentials(authorization: str | None, client_id: str | None, token: str | None):
        if authorization and authorization.lower().startswith("basic "):
            try:
                raw = base64.b64decode(authorization[6:].strip(), validate=True).decode()
                cid, _, tok = raw.partition(":")
                return cid, tok
            except (binascii.Error, UnicodeDecodeError, ValueError):
                raise GatewayError(ErrorCode.INVALID_CREDENTIALS, "malformed basic header")
        return client_id, token

    @app.get("/")
    async def root() -> dict:
        return {
            "service": "robotics-message-gateway",
            "endpoints": [
                "POST /publish",
                "GET  /retained",
                "GET  /metrics",
                "GET  /sessions/{client_id}",
                "POST /admin/credentials",
                "WS   /ws?client_id=&token=",
            ],
        }

    @app.post("/publish")
    async def http_publish(
        publish_req: PublishRequest = Body(...),
        authorization: str | None = Header(default=None),
        client_id: str | None = Query(default=None),
        token: str | None = Query(default=None),
    ) -> dict:
        cid, tok = _basic_credentials(authorization, client_id, token)
        publisher = auth.authenticate(cid, tok)
        message = broker.publish(
            publish_req.topic, publish_req.payload, publisher, retained=publish_req.retain
        )
        return {
            "status": "ok",
            "seq": message.seq,
            "topic": message.topic,
            "retained": publish_req.retain,
        }

    @app.get("/retained")
    async def http_retained(filter: str = Query(default="#", alias="filter")) -> dict:
        hits = broker.matching_retained(filter)
        return {
            "filter": filter,
            "messages": [
                {"topic": m.topic, "seq": m.seq, "payload": m.payload} for m in hits
            ],
        }

    @app.get("/metrics")
    async def http_metrics() -> dict:
        return broker.metrics_snapshot()

    @app.get("/sessions/{client_id}")
    async def http_session(client_id: str) -> dict:
        session = broker.registry.get(client_id)
        if session is None:
            return JSONResponse(
                status_code=404,
                content=error_body(ErrorCode.SESSION_NOT_FOUND, f"no session {client_id!r}"),
            )
        return session.snapshot()

    @app.post("/admin/credentials")
    async def update_credentials(
        cred_payload: dict[str, CredentialModel] = Body(...),
        authorization: str | None = Header(default=None),
        client_id: str | None = Query(default=None),
        token: str | None = Query(default=None),
    ) -> dict:
        # 只有被授予 "#" 且同时具备 pub/sub 的超管可更新规则
        cid, tok = _basic_credentials(authorization, client_id, token)
        admin = auth.authenticate(cid, tok)
        if not auth.is_admin(admin):
            raise GatewayError(
                ErrorCode.PUBLISH_FORBIDDEN,
                "admin scope required: permission '#' with actions pub+sub",
            )
        new_creds: dict[str, ClientCredential] = {}
        for cid_key, cred_model in cred_payload.items():
            perms = tuple(
                Permission(p.filter, frozenset(p.actions)) for p in cred_model.permissions
            )
            new_creds[cid_key] = ClientCredential(cid_key, cred_model.token, perms)
        auth.update_credentials(new_creds)
        revoked = broker.reconcile_subscriptions()
        logger.info("credentials updated by=%s version=%d revoked=%d", admin, auth.version, len(revoked))
        return {"status": "ok", "version": auth.version, "revoked": revoked}

    # ---------- WebSocket ----------
    @app.websocket("/ws")
    async def websocket_endpoint(
        websocket: WebSocket,
        client_id: str | None = Query(default=None),
        token: str | None = Query(default=None),
        authorization: str | None = Query(default=None),
    ) -> None:
        cid, tok = _basic_credentials(authorization, client_id, token)
        try:
            verified = auth.authenticate(cid, tok)
        except GatewayError as exc:
            # 握手阶段拒绝：凭据缺失/非法可区分（4401/4403 自定义 close code）
            await websocket.close(
                code=4401 if exc.code == ErrorCode.MISSING_CREDENTIALS else 4403
            )
            broker.metrics.reject(exc.code.value)
            logger.warning("ws auth reject code=%s", exc.code.value)
            return

        # 重复连接拒绝：不影响旧连接
        existing = broker.registry.get(verified)
        if existing is not None and existing.connected:
            await websocket.close(code=4409)
            broker.metrics.reject(ErrorCode.DUPLICATE_CONNECTION.value)
            logger.warning("ws duplicate connection client=%s", verified)
            return

        await websocket.accept()
        session, initial = broker.connect(verified)
        send_queue: asyncio.Queue[str] = asyncio.Queue(maxsize=send_queue_max)
        for delivery in initial:
            send_queue.put_nowait(encode_delivery(delivery))

        def on_available() -> None:
            # broker 回调发生在事件循环线程；取走当前就绪消息。
            # 发送队列满（慢消费者）时放弃该投递：会话腾窗口并按序提升排队
            # 消息，丢弃结论计入指标，绝不阻塞发布者或其他连接。
            while True:
                ready = session.drain_pending()
                if not ready:
                    return
                for delivery in ready:
                    frame = encode_delivery(delivery)
                    try:
                        send_queue.put_nowait(frame)
                    except asyncio.QueueFull:
                        broker.abandon(verified, delivery.delivery_id, "send_queue_full")
                        logger.warning(
                            "send queue full client=%s delivery_id=%d seq=%s -> abandoned",
                            verified, delivery.delivery_id, delivery.seq,
                        )
                        # abandon 可能把排队消息提升为新的就绪项，继续外层循环
                        break

        session.on_available = on_available

        async def sender() -> None:
            while True:
                frame = await send_queue.get()
                await websocket.send_text(frame)

        sender_task = asyncio.create_task(sender(), name=f"sender:{verified}")
        try:
            while True:
                raw = await websocket.receive_text()
                logger.info("ws recv client=%s raw=%s", verified, raw)
                try:
                    frame = parse_client_message(raw)
                except GatewayError as exc:
                    await websocket.send_text(encode_error(None, exc.code, str(exc)))
                    broker.metrics.reject(exc.code.value)
                    continue
                req_id = frame.get("id")
                action = frame["action"]
                try:
                    if action == "subscribe":
                        result = broker.subscribe(verified, frame["filter"])
                        await websocket.send_text(
                            encode_ack(req_id, "subscribe", **result)
                        )
                    elif action == "unsubscribe":
                        result = broker.unsubscribe(verified, frame["filter"])
                        await websocket.send_text(
                            encode_ack(req_id, "unsubscribe", **result)
                        )
                    elif action == "publish":
                        message = broker.publish(
                            frame["topic"],
                            frame.get("payload"),
                            verified,
                            retained=bool(frame.get("retain", False)),
                        )
                        await websocket.send_text(
                            encode_ack(
                                req_id, "publish",
                                seq=message.seq, topic=message.topic,
                            )
                        )
                    else:  # ack
                        result = broker.acknowledge(verified, int(frame["delivery_id"]))
                        await websocket.send_text(
                            encode_ack(req_id, "ack", result=result)
                        )
                except GatewayError as exc:
                    await websocket.send_text(
                        encode_error(req_id, exc.code, str(exc))
                    )
                # broker 处理期间可能产生新可发消息（subscribe 的保留消息等）
                on_available()
        except WebSocketDisconnect:
            logger.info("ws disconnect client=%s", verified)
        except Exception:
            logger.exception("ws loop error client=%s", verified)
        finally:
            sender_task.cancel()
            broker.disconnect(verified)

    return app


app = create_app()
