"""公共夹具与测试日志辅助。

每个用例通过 log_case 打印：输入、判定依据、最终状态，
使失败原因在日志中可区分。
"""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from gateway.app import create_app
from gateway.auth import ACLRule, Action

logging.basicConfig(level=logging.INFO, format="%(message)s")
log = logging.getLogger("gateway.test")

TOKENS = {
    "t-admin": "admin",
    "t-pub": "publisher",
    "t-sub": "subscriber",
    "t-arm": "arm-only",
    # 需要多个并存身份的场景（慢消费者隔离、并发发布）使用独立身份
    "t-sub2": "subscriber-2",
    "t-pub1": "publisher-1",
    "t-pub2": "publisher-2",
    "t-pub3": "publisher-3",
}

# 管理接口凭据（与客户端凭据分离）
ADMIN_TOKEN = "t-root"

RULES = [
    ACLRule("admin", Action.PUBLISH, "#"),
    ACLRule("admin", Action.SUBSCRIBE, "#"),
    ACLRule("publisher", Action.PUBLISH, "robot/#"),
    ACLRule("subscriber", Action.SUBSCRIBE, "robot/#"),
    ACLRule("arm-only", Action.SUBSCRIBE, "robot/arm/#"),
    ACLRule("arm-only", Action.PUBLISH, "robot/arm/#"),
    ACLRule("subscriber-2", Action.SUBSCRIBE, "robot/#"),
    ACLRule("publisher-1", Action.PUBLISH, "robot/#"),
    ACLRule("publisher-2", Action.PUBLISH, "robot/#"),
    ACLRule("publisher-3", Action.PUBLISH, "robot/#"),
]


def log_case(case: str, inputs: dict, basis: str, state: dict) -> None:
    """打印用例的输入、判定依据与最终状态。"""
    log.info(
        "\n[用例] %s\n  输入: %s\n  判定依据: %s\n  最终状态: %s",
        case, inputs, basis, state,
    )


@pytest.fixture()
def app():
    application = create_app()
    application.state.acl.set_tokens(dict(TOKENS))
    application.state.acl.set_rules(list(RULES))
    application.state.acl.set_admin_tokens({ADMIN_TOKEN})
    return application


@pytest.fixture()
def client(app):
    with TestClient(app) as c:
        yield c


def auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}
