"""本地机器人消息网关。

不依赖任何外部消息中间件，进程内提供基于层级主题的发布/订阅、
保留消息、访问控制、可靠会话恢复与可观测指标。
"""

from .models import ClientId, Message, TopicFilter
from .errors import ErrorCode, GatewayError

__all__ = [
    "ClientId",
    "Message",
    "TopicFilter",
    "ErrorCode",
    "GatewayError",
]
