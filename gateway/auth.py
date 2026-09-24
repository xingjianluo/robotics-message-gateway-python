"""认证与基于客户端 + 主题范围的访问控制。

权限模型：
- 每个客户端持有一个不透明 token；client_id + token 双因素校验。
- 权限为若干条目 ``Permission(filter, actions)``，filter 支持与订阅
  相同的层级通配（``+`` / ``#``），actions 为 ``"pub"`` / ``"sub"`` 子集。
- 判定发生在每次发布/订阅时（而不是仅在建连时），因此规则热更新后，
  既有连接与后续请求立即按同一版本生效。
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .errors import ErrorCode, GatewayError
from .models import ClientId
from .topics import matches, validate_publish_topic, validate_subscribe_filter


@dataclass(frozen=True, slots=True)
class Permission:
    """允许 client 对 filter 范围内的主题执行 actions。"""

    filter: str
    actions: frozenset[str] = frozenset({"pub", "sub"})

    def levels(self) -> list[str]:
        return validate_subscribe_filter(self.filter)


@dataclass(frozen=True, slots=True)
class ClientCredential:
    client_id: ClientId
    token: str
    permissions: tuple[Permission, ...] = ()


class Authenticator:
    """凭据校验 + ACL 判定；credentials 版本随热更新递增。"""

    def __init__(self, credentials: dict[str, ClientCredential] | None = None) -> None:
        self._lock = threading.Lock()
        self._credentials: dict[str, ClientCredential] = {}
        self._version: int = 0
        self.update_credentials(credentials or {})

    # ---- 认证 ----
    def authenticate(self, client_id: str | None, token: str | None) -> ClientId:
        if not client_id or not token:
            raise GatewayError(
                ErrorCode.MISSING_CREDENTIALS,
                "both client_id and token are required",
            )
        with self._lock:
            cred = self._credentials.get(client_id)
        if cred is None or cred.token != token:
            # 不区分“用户不存在”与“口令错误”，但与“未提供凭据”可区分
            raise GatewayError(
                ErrorCode.INVALID_CREDENTIALS,
                f"invalid credentials for client {client_id!r}",
            )
        return client_id

    # ---- 授权 ----
    def can_publish(self, client_id: ClientId, topic: str) -> bool:
        return self._allowed(client_id, topic, "pub", validate_publish_topic)

    def can_subscribe(self, client_id: ClientId, topic_filter: str) -> bool:
        return self._allowed(client_id, topic_filter, "sub", validate_subscribe_filter)

    def _allowed(self, client_id: ClientId, target: str, action: str, validator) -> bool:
        try:
            target_levels = validator(target)
        except GatewayError:
            return False
        with self._lock:
            cred = self._credentials.get(client_id)
        if cred is None:
            return False
        for perm in cred.permissions:
            if action in perm.actions and matches(perm.levels(), target_levels):
                return True
        return False

    # ---- 热更新 ----
    def update_credentials(self, credentials: dict[str, ClientCredential]) -> None:
        # 提前校验：非法权限过滤器拒绝整批更新
        normalized: dict[str, ClientCredential] = {}
        for cid, cred in credentials.items():
            if cred.client_id != cid:
                raise ValueError(f"credential key {cid!r} != client_id {cred.client_id!r}")
            for perm in cred.permissions:
                perm.levels()
                if not perm.actions <= {"pub", "sub"}:
                    raise ValueError(f"unknown actions in permission: {perm.actions}")
            normalized[cid] = cred
        with self._lock:
            self._credentials = normalized
            self._version += 1

    def is_known(self, client_id: ClientId) -> bool:
        with self._lock:
            return client_id in self._credentials

    def is_admin(self, client_id: ClientId) -> bool:
        """超管定义：拥有过滤器 '#' 且 actions 同时包含 pub 与 sub。"""
        with self._lock:
            cred = self._credentials.get(client_id)
        if cred is None:
            return False
        return any(
            perm.filter == "#" and {"pub", "sub"} <= set(perm.actions)
            for perm in cred.permissions
        )

    @property
    def version(self) -> int:
        with self._lock:
            return self._version
