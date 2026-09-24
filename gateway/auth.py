"""按客户端与主题范围的访问控制。

- 凭据：token -> client_id 映射，由 ``set_tokens`` 整体替换。
- 规则：有序列表，默认拒绝；任一规则匹配即放行。
- 规则更新立即生效：鉴权在每次发布/订阅时实时查询本存储，
  因此既有连接与后续请求行为一致。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

from . import topics


class Action(str, Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"


class DenyReason(str, Enum):
    """可区分的拒绝原因。"""

    INVALID_CREDENTIALS = "invalid_credentials"
    NOT_ALLOWED = "not_allowed"


@dataclass(frozen=True)
class ACLRule:
    client_id: str  # "*" 表示所有客户端
    action: Action
    topic_filter: str  # 复用订阅过滤器语法做主题范围匹配


@dataclass(frozen=True)
class AuthDecision:
    allowed: bool
    reason: DenyReason | None = None


class ACLStore:
    """线程安全的 ACL 存储。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tokens: dict[str, str] = {}  # token -> client_id
        self._rules: list[ACLRule] = []

    def set_tokens(self, tokens: dict[str, str]) -> None:
        with self._lock:
            self._tokens = dict(tokens)

    def set_rules(self, rules: list[ACLRule]) -> None:
        for r in rules:
            verdict = topics.validate_filter(r.topic_filter)
            if not verdict.ok:
                raise ValueError(f"invalid rule filter {r.topic_filter!r}: {verdict.reason}")
        with self._lock:
            self._rules = list(rules)

    def authenticate(self, token: str | None) -> str | None:
        """返回 client_id；无效凭据返回 None。"""
        if not token:
            return None
        with self._lock:
            return self._tokens.get(token)

    def authorize(self, client_id: str, action: Action, topic: str) -> AuthDecision:
        with self._lock:
            rules = list(self._rules)
        for rule in rules:
            if rule.client_id not in ("*", client_id):
                continue
            if rule.action is not action:
                continue
            if topics.match(rule.topic_filter, topic):
                return AuthDecision(True)
        return AuthDecision(False, DenyReason.NOT_ALLOWED)

    def authorize_subscribe(self, client_id: str, topic_filter: str) -> AuthDecision:
        """订阅鉴权：规则过滤器必须完全覆盖请求的过滤器。"""
        with self._lock:
            rules = list(self._rules)
        for rule in rules:
            if rule.client_id not in ("*", client_id):
                continue
            if rule.action is not Action.SUBSCRIBE:
                continue
            if topics.covers(rule.topic_filter, topic_filter):
                return AuthDecision(True)
        return AuthDecision(False, DenyReason.NOT_ALLOWED)
