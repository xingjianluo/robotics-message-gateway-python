"""运行期账号与权限治理的管理接口。

端点（均需管理凭据 ``Authorization: Bearer <admin-token>``）：
- GET    /admin/accounts                     列出全部账号
- POST   /admin/accounts                     新增/更新账号 {"client_id","token"}
- DELETE /admin/accounts/{client_id}         注销账号（在线连接随即被断开）
- PUT    /admin/accounts/{client_id}/acl     调整发布/订阅范围
                                             {"publish":[...],"subscribe":[...]}

错误结果可互相区分：
- 401 invalid_credentials：缺失/未知凭据
- 403 not_admin：凭据有效但不是管理凭据
- 400 invalid_request：请求体不合法（缺字段、非法主题过滤器等）
- 404 unknown_client：目标账号不存在
- 409 token_in_use：token 已被其他身份占用

任何成功改动都会通过 ``on_changed`` 回调通知应用层做追溯处理
（收回既有连接中不再被允许的订阅、断开被注销身份的连接），
保证规则改完即刻对在线连接生效。
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable

from fastapi import APIRouter, Header
from fastapi.responses import JSONResponse

from . import topics
from .auth import ACLStore


class AdminReason(str, Enum):
    """管理接口的可区分失败原因。"""

    INVALID_CREDENTIALS = "invalid_credentials"
    NOT_ADMIN = "not_admin"
    INVALID_REQUEST = "invalid_request"
    UNKNOWN_CLIENT = "unknown_client"
    TOKEN_IN_USE = "token_in_use"


@dataclass(frozen=True)
class AdminEvent:
    """一次管理改动。kind: account_added / account_removed / rules_changed。"""

    kind: str
    client_id: str


OnChanged = Callable[[AdminEvent], Awaitable[None]]


def _fail(status: int, reason: AdminReason, detail: str = "") -> JSONResponse:
    body = {"ok": False, "reason": reason.value}
    if detail:
        body["detail"] = detail
    return JSONResponse(body, status_code=status)


def create_admin_router(acl: ACLStore, on_changed: OnChanged) -> APIRouter:
    router = APIRouter(prefix="/admin")

    def _check_admin(authorization: str | None) -> JSONResponse | None:
        """返回 None 表示通过；否则为可区分的失败响应。"""
        token = None
        if authorization and authorization.startswith("Bearer "):
            token = authorization.removeprefix("Bearer ").strip()
        if acl.is_admin(token):
            return None
        if token and acl.authenticate(token) is not None:
            return _fail(403, AdminReason.NOT_ADMIN)
        return _fail(401, AdminReason.INVALID_CREDENTIALS)

    @router.get("/accounts")
    def list_accounts(
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        if (deny := _check_admin(authorization)) is not None:
            return deny
        return JSONResponse({"ok": True, "accounts": acl.list_accounts()})

    @router.post("/accounts")
    async def add_account(
        body: dict, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        if (deny := _check_admin(authorization)) is not None:
            return deny
        client_id = str(body.get("client_id", "")).strip()
        token = str(body.get("token", "")).strip()
        if not client_id or not token:
            return _fail(
                400, AdminReason.INVALID_REQUEST, "client_id 与 token 均不能为空"
            )
        if (err := acl.add_account(token, client_id)) is not None:
            return _fail(409, AdminReason.TOKEN_IN_USE, err)
        await on_changed(AdminEvent("account_added", client_id))
        return JSONResponse({"ok": True, "client_id": client_id})

    @router.delete("/accounts/{client_id}")
    async def remove_account(
        client_id: str, authorization: str | None = Header(default=None)
    ) -> JSONResponse:
        if (deny := _check_admin(authorization)) is not None:
            return deny
        if not acl.remove_account(client_id):
            return _fail(404, AdminReason.UNKNOWN_CLIENT)
        await on_changed(AdminEvent("account_removed", client_id))
        return JSONResponse({"ok": True, "removed": client_id})

    @router.put("/accounts/{client_id}/acl")
    async def set_acl(
        client_id: str,
        body: dict,
        authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        if (deny := _check_admin(authorization)) is not None:
            return deny
        if not acl.client_exists(client_id):
            return _fail(404, AdminReason.UNKNOWN_CLIENT)
        publish, subscribe = body.get("publish"), body.get("subscribe")
        if (
            not isinstance(publish, list)
            or not isinstance(subscribe, list)
            or not all(isinstance(f, str) for f in publish + subscribe)
        ):
            return _fail(
                400,
                AdminReason.INVALID_REQUEST,
                "publish/subscribe 必须为字符串数组",
            )
        for filt in publish + subscribe:
            verdict = topics.validate_filter(filt)
            if not verdict.ok:
                return _fail(
                    400,
                    AdminReason.INVALID_REQUEST,
                    f"非法过滤器 {filt!r}: {verdict.reason.value}",
                )
        acl.set_client_rules(client_id, publish, subscribe)
        await on_changed(AdminEvent("rules_changed", client_id))
        return JSONResponse({"ok": True, "client_id": client_id})

    return router
