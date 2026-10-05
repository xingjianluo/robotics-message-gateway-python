"""按客户端与主题范围的访问控制。

- 凭据：token -> client_id 映射，由 ``set_tokens`` 整体替换。
- 规则：有序列表，默认拒绝；任一规则匹配即放行。
- 规则更新立即生效：鉴权在每次发布/订阅时实时查询本存储，
  因此既有连接与后续请求行为一致。
"""
from __future__ import annotations

import threading
from collections.abc import Iterable
from dataclasses import dataclass
from enum import Enum

from . import topics


class Action(str, Enum):
    PUBLISH = "publish"
    SUBSCRIBE = "subscribe"


class DenyReason(str, Enum):
    """可区分的拒绝原因。"""

    INVALID_CREDENTIALS = "invalid_credentials"
    NOT_ALLOWED = "not_allowed"
    IDENTITY_MISMATCH = "identity_mismatch"  # 声明的 client_id 与凭据归属不一致
    SESSION_TAKEN_OVER = "session_taken_over"  # 同身份新连接接管，旧连接被顶掉
    ACCOUNT_REVOKED = "account_revoked"  # 账号被注销，连接被终止


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

    运行期治理（不重启生效）：
    - ``add_client`` / ``remove_client``：新增/注销账号（token 即时生效/失效）；
    - ``set_client_rules``：整体替换某账号的发布/订阅主题范围；
    - 管理操作本身由独立的管理员 token 集合保护（``set_admin_tokens``）。
    """

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._tokens: dict[str, str] = {}  # token -> client_id
        self._rules: list[ACLRule] = []
        self._admin_tokens: set[str] = set()

    # ---------------- 凭据与规则 ----------------

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

    # ---------------- 运行期账号治理 ----------------

    def set_admin_tokens(self, tokens: Iterable[str]) -> None:
        with self._lock:
            self._admin_tokens = set(tokens)

    def authenticate_admin(self, token: str | None) -> bool:
        if not token:
            return False
        with self._lock:
            return token in self._admin_tokens

    def client_exists(self, client_id: str) -> bool:
        with self._lock:
            return client_id in self._tokens.values()

    def token_in_use(self, token: str) -> bool:
        with self._lock:
            return token in self._tokens

    def add_client(self, client_id: str, token: str) -> None:
        """新增账号，或替换已有账号的 token（旧 token 立即失效）。调用方需先校验。"""
        with self._lock:
            # 注销该账号的旧 token，保证 token -> client_id 唯一
            for t in [t for t, c in self._tokens.items() if c == client_id]:
                del self._tokens[t]
            self._tokens[token] = client_id

    def remove_client(self, client_id: str) -> bool:
        """注销账号：删除其全部 token 与规则。返回账号是否存在过。"""
        with self._lock:
            old_tokens = [t for t, c in self._tokens.items() if c == client_id]
            for t in old_tokens:
                del self._tokens[t]
            old_len = len(self._rules)
            self._rules = [r for r in self._rules if r.client_id != client_id]
            return bool(old_tokens) or len(self._rules) != old_len

    def set_client_rules(
        self,
        client_id: str,
        publish_filters: list[str],
        subscribe_filters: list[str],
    ) -> None:
        """整体替换某账号的主题范围（先校验，全部合法才生效）。"""
        new_rules: list[ACLRule] = []
        for f in publish_filters:
            verdict = topics.validate_filter(f)
            if not verdict.ok:
                raise ValueError(f"invalid publish filter {f!r}: {verdict.reason.value}")
            new_rules.append(ACLRule(client_id, Action.PUBLISH, f))
        for f in subscribe_filters:
            verdict = topics.validate_filter(f)
            if not verdict.ok:
                raise ValueError(f"invalid subscribe filter {f!r}: {verdict.reason.value}")
            new_rules.append(ACLRule(client_id, Action.SUBSCRIBE, f))
        with self._lock:
            self._rules = [r for r in self._rules if r.client_id != client_id]
            self._rules.extend(new_rules)

    def list_clients(self) -> list[dict]:
        """列出全部账号及其规则（不含 token 明文）。"""
        with self._lock:
            client_ids = sorted(set(self._tokens.values()))
            rules = list(self._rules)
        clients = []
        for cid in client_ids:
            clients.append(
                {
                    "client_id": cid,
                    "publish": [r.topic_filter for r in rules
                                if r.client_id == cid and r.action is Action.PUBLISH],
                    "subscribe": [r.topic_filter for r in rules
                                  if r.client_id == cid and r.action is Action.SUBSCRIBE],
                }
            )
        return clients

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
