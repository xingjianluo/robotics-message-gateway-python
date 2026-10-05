"""运行期账号与权限治理的管理接口。

能力（骨架，逐步实现）：
- 新增/注销客户端账号（token <-> client_id）；
- 调整某账号的发布/订阅主题范围；
- 改动立即生效：既有连接中不再被允许的订阅被追溯收回；
- 管理操作本身受管理凭据保护，身份无效/无权操作/请求不合法可区分。
"""
from __future__ import annotations

from fastapi import APIRouter

from .auth import ACLStore
from .broker import Broker
from .session import SessionManager


def create_admin_router(
    acl: ACLStore,
    broker: Broker,
    sessions: SessionManager,
) -> APIRouter:
    """创建管理路由（骨架：先返回空路由，逐步填充端点）。"""
    router = APIRouter(prefix="/admin")
    # TODO: 账号增删、规则调整、追溯收回
    return router
