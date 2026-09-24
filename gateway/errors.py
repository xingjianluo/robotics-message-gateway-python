"""错误码与异常定义。

所有拒绝原因使用可区分的错误码，HTTP / WebSocket / 测试日志
都能据此判定失败的具体类别。
"""

from enum import Enum


class ErrorCode(str, Enum):
    # 认证类
    MISSING_CREDENTIALS = "missing_credentials"
    INVALID_CREDENTIALS = "invalid_credentials"
    # 主题类
    EMPTY_TOPIC = "empty_topic"
    TOPIC_TOO_LONG = "topic_too_long"
    EMPTY_LEVEL = "empty_level"
    BAD_WILDCARD_POSITION = "bad_wildcard_position"
    ILLEGAL_CHARACTER = "illegal_character"
    # 授权类
    PUBLISH_FORBIDDEN = "publish_forbidden"
    SUBSCRIBE_FORBIDDEN = "subscribe_forbidden"
    # 协议 / 会话类
    MALFORMED_PAYLOAD = "malformed_payload"
    UNSUPPORTED_ACTION = "unsupported_action"
    DUPLICATE_CONNECTION = "duplicate_connection"
    SESSION_NOT_FOUND = "session_not_found"
    UNKNOWN_DELIVERY_ID = "unknown_delivery_id"
    RATE_LIMITED = "rate_limited"


class GatewayError(Exception):
    """带错误码的网关异常。"""

    def __init__(self, code: ErrorCode, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or code.value)
