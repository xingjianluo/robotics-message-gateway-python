"""服务入口：uvicorn main:app

运行参数可用环境变量覆盖（默认值见 gateway.app.create_app）：
- GATEWAY_MAX_QUEUE            单订阅积压上限（默认 100）
- GATEWAY_MAX_INFLIGHT         单连接未确认上限（默认 64）
- GATEWAY_SESSION_TTL          离线会话过期秒数（默认 0=不过期）
- GATEWAY_MAX_OFFLINE_BACKLOG  单客户端离线积压总量上限（默认 0=不限）
- GATEWAY_REAP_INTERVAL        回收扫描间隔秒数（默认 1.0）
"""
import os

from gateway.app import create_app

app = create_app(
    max_queue=int(os.getenv("GATEWAY_MAX_QUEUE", "100")),
    max_inflight=int(os.getenv("GATEWAY_MAX_INFLIGHT", "64")),
    session_ttl=float(os.getenv("GATEWAY_SESSION_TTL", "0")),
    max_offline_backlog=int(os.getenv("GATEWAY_MAX_OFFLINE_BACKLOG", "0")),
    reap_interval=float(os.getenv("GATEWAY_REAP_INTERVAL", "1.0")),
)
