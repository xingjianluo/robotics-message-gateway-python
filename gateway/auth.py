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
    """线程安全的 ACL 存储。

    运行期治理：账号（token）与规则均可逐项增删改，鉴权实时查询本存储，
    因此改动对既有连接与后续请求立即生效。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tokens: dict[str, str] = {}  # token -> client_id
        self._rules: list[ACLRule] = []
        self._admin_tokens: set[str] = set()

    # ---------------- 批量装载（启动/测试用） ----------------

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

    # ---------------- 管理凭据 ----------------

    def set_admin_tokens(self, tokens: set[str]) -> None:
        with self._lock:
            self._admin_tokens = set(tokens)

    def is_admin(self, token: str | None) -> bool:
        if not token:
            return False
        with self._lock:
            return token in self._admin_tokens

    # ---------------- 运行期账号治理 ----------------

    def add_account(self, token: str, client_id: str) -> str | None:
        """新增/更新账号。token 已被其他身份占用时返回 "token_in_use"。"""
        with self._lock:
            owner = self._tokens.get(token)
            if owner is not None and owner != client_id:
                return "token_in_use"
            self._tokens[token] = client_id
            return None

    def remove_account(self, client_id: str) -> bool:
        """注销账号：移除其全部 token 与专属规则。返回账号是否存在过。"""
        with self._lock:
            tokens = [t for t, c in self._tokens.items() if c == client_id]
            rules_before = len(self._rules)
            self._rules = [r for r in self._rules if r.client_id != client_id]
            for t in tokens:
                del self._tokens[t]
            return bool(tokens) or len(self._rules) != rules_before

    def client_exists(self, client_id: str) -> bool:
        with self._lock:
            return client_id in self._tokens.values()

    def list_accounts(self) -> dict[str, list[str]]:
        """client_id -> 其 token 列表（仅供管理端使用）。"""
        with self._lock:
            accounts: dict[str, list[str]] = {}
            for token, client_id in self._tokens.items():
                accounts.setdefault(client_id, []).append(token)
            return accounts

    # ---------------- 运行期规则治理 ----------------

    def set_client_rules(
        self, client_id: str, publish: list[str], subscribe: list[str]
    ) -> None:
        """整体替换某账号的发布/订阅主题范围（过滤器非法时抛 ValueError）。"""
        new_rules: list[ACLRule] = []
        for filt in publish:
            verdict = topics.validate_filter(filt)
            if not verdict.ok:
                raise ValueError(f"invalid publish filter {filt!r}: {verdict.reason}")
            new_rules.append(ACLRule(client_id, Action.PUBLISH, filt))
        for filt in subscribe:
            verdict = topics.validate_filter(filt)
            if not verdict.ok:
                raise ValueError(f"invalid subscribe filter {filt!r}: {verdict.reason}")
            new_rules.append(ACLRule(client_id, Action.SUBSCRIBE, filt))
        with self._lock:
            kept = [r for r in self._rules if r.client_id != client_id]
            self._rules = kept + new_rules

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
