"""测试公共夹具与判定日志工具。

每个关键用例都打印：输入、判定依据、最终状态，失败时能据错误码区分原因。
"""

from __future__ import annotations

import json
import logging

import pytest

from gateway.app import create_app
from gateway.auth import ClientCredential, Permission

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)


def make_credentials() -> dict[str, ClientCredential]:
    """测试用固定凭据：发布者 / 订阅者 / 超管 / 只读。"""
    return {
        "pub-r1": ClientCredential(
            "pub-r1", "t-pub",
            (Permission("robots/r1/status", frozenset({"pub"})),),
        ),
        "sub-status": ClientCredential(
            "sub-status", "t-sub",
            (Permission("robots/+/status", frozenset({"sub"})),),
        ),
        "sub-all": ClientCredential(
            "sub-all", "t-all",
            (Permission("robots/#", frozenset({"sub"})),),
        ),
        "ops": ClientCredential(
            "ops", "t-ops",
            (Permission("#", frozenset({"pub", "sub"})),),
        ),
    }


@pytest.fixture()
def app():
    application = create_app(make_credentials(), max_inflight=4, max_queue=4)
    return application


@pytest.fixture()
def broker(app):
    return app.state.broker


@pytest.fixture()
def client(app):
    from fastapi.testclient import TestClient

    with TestClient(app) as c:
        yield c


def log_case(case: str, inp, basis, final) -> None:
    logging.getLogger("case").info(
        "CASE=%s INPUT=%s BASIS=%s FINAL=%s",
        case, json.dumps(inp, ensure_ascii=False, default=str), basis,
        json.dumps(final, ensure_ascii=False, default=str),
    )


def auth(client_id: str, token: str) -> dict:
    import base64

    raw = base64.b64encode(f"{client_id}:{token}".encode()).decode()
    return {"Authorization": f"Basic {raw}"}
