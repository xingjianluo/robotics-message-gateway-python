"""WebSocket 线上协议的编解码。

客户端 -> 服务端（JSON 文本帧）：
    {"action": "subscribe",   "id": "c-1", "filter": "a/b/+"}
    {"action": "unsubscribe", "id": "c-2", "filter": "a/b/+"}
    {"action": "publish",     "id": "c-3", "topic": "a/b/c", "payload": {...}, "retain": false}
    {"action": "ack",         "id": "c-4", "delivery_id": 12}

服务端 -> 客户端：
    {"type": "ack",     "id": "c-1", "action": "subscribe", "status": "ok", ...}
    {"type": "error",   "id": "c-3", "status": "error", "code": "...", "message": "..."}
    {"type": "message", "topic": ..., "payload": ..., "seq": 3,
     "delivery_id": 12, "retained": false, "duplicate": false}

任何无法解析或缺少关键字段的帧返回 ``malformed_payload``，
action 未知返回 ``unsupported_action``，两者可区分。
"""

from __future__ import annotations

import json
from typing import Any

from .errors import ErrorCode, GatewayError
from .models import Delivery

_ALLOWED_ACTIONS = frozenset({"subscribe", "unsubscribe", "publish", "ack"})
_REQUIRED_FIELDS = {
    "subscribe": ("filter",),
    "unsubscribe": ("filter",),
    "publish": ("topic",),
    "ack": ("delivery_id",),
}


def parse_client_message(raw: str | bytes) -> dict[str, Any]:
    try:
        frame = json.loads(raw)
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, f"invalid JSON: {exc}") from exc
    if not isinstance(frame, dict):
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, "frame must be a JSON object")
    action = frame.get("action")
    if not isinstance(action, str):
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, "missing string field 'action'")
    if action not in _ALLOWED_ACTIONS:
        raise GatewayError(
            ErrorCode.UNSUPPORTED_ACTION,
            f"action {action!r} not in {sorted(_ALLOWED_ACTIONS)}",
        )
    for field_name in _REQUIRED_FIELDS[action]:
        if field_name not in frame:
            raise GatewayError(
                ErrorCode.MALFORMED_PAYLOAD,
                f"action {action!r} requires field {field_name!r}",
            )
    if action == "ack" and not isinstance(frame["delivery_id"], int):
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, "'delivery_id' must be an integer")
    if action in ("subscribe", "unsubscribe") and not isinstance(frame["filter"], str):
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, "'filter' must be a string")
    if action == "publish" and not isinstance(frame["topic"], str):
        raise GatewayError(ErrorCode.MALFORMED_PAYLOAD, "'topic' must be a string")
    return frame


def encode_delivery(d: Delivery) -> str:
    return _dumps({
        "type": "message",
        "delivery_id": d.delivery_id,
        "seq": d.seq,
        "topic": d.topic,
        "payload": d.payload,
        "publisher": d.publisher,
        "retained": d.retained,
        "duplicate": d.duplicate,
    })


def encode_ack(request_id: Any, action: str, status: str = "ok", **extra: Any) -> str:
    payload = {"type": "ack", "id": request_id, "action": action, "status": status}
    payload.update(extra)
    return _dumps(payload)


def encode_error(request_id: Any, code: ErrorCode | str, message: str, **extra: Any) -> str:
    code_value = code.value if isinstance(code, ErrorCode) else str(code)
    payload = {
        "type": "error",
        "id": request_id,
        "status": "error",
        "code": code_value,
        "message": message,
    }
    payload.update(extra)
    return _dumps(payload)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":"))
