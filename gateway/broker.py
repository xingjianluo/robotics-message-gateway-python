"""核心消息代理：保留消息存储、订阅匹配、扇出、指标汇总。

关键不变量：
- 发布序号 seq 全局单调递增（单线程事件循环），作为全局顺序与去重依据。
- 每次发布都对“当前匹配的已建立会话”扇出一次；重复订阅同一过滤器
  不会产生两条订阅，因此同一消息不会向同一客户端重复投递。
- 保留消息按“具体主题”保留最后一条；新订阅建立时投递过滤器匹配的
  每个主题各一条（retained=true），按主题字典序保证确定性。
- 任何拒绝（主题非法 / 越权）只拒绝当前操作，不触碰其他连接，
  并按可区分原因计数。
"""

from __future__ import annotations

import itertools
import logging
import time
from dataclasses import replace

from .auth import Authenticator
from .errors import ErrorCode, GatewayError
from .metrics import Metrics
from .models import Delivery, Message, Subscription
from .sessions import Session, SessionRegistry
from .topics import matches, validate_publish_topic, validate_subscribe_filter

logger = logging.getLogger(__name__)


class Broker:
    def __init__(
        self,
        authenticator: Authenticator,
        max_inflight: int = 100,
        max_queue: int = 1000,
    ) -> None:
        self.auth = authenticator
        self.registry = SessionRegistry(max_inflight=max_inflight, max_queue=max_queue)
        self.metrics = Metrics()
        # 具体主题 -> 最后一条保留消息；payload 为 None 表示墓碑（清除）
        self._retained: dict[str, Message] = {}
        self._seq = itertools.count(1)

    # ---- 发布 ----
    def publish(
        self,
        topic: str,
        payload,
        publisher: str | None,
        retained: bool = False,
    ) -> Message:
        # 1) 主题校验（错误码可区分）
        topic_levels = validate_publish_topic(topic)
        # 2) 授权（越权发布不影响任何其他连接）
        if publisher is not None and not self.auth.can_publish(publisher, topic):
            self._reject(ErrorCode.PUBLISH_FORBIDDEN, publisher, topic)
            raise GatewayError(
                ErrorCode.PUBLISH_FORBIDDEN,
                f"client {publisher!r} not allowed to publish {topic!r}",
            )

        message = Message(
            seq=next(self._seq),
            topic=topic,
            payload=payload,
            publisher=publisher,
            retained=False,
            published_at_ms=time.time_ns() // 1_000_000,
        )
        # 3) 保留消息处理（在扇出前，保证新订阅者可见）
        if retained:
            if payload is None:
                self._retained.pop(topic, None)
            else:
                self._retained[topic] = replace(message, retained=True)
                self.metrics.inc("retained_total")

        # 4) 向匹配的已订阅会话扇出
        delivered = 0
        dropped = 0
        for session in self.registry.all_sessions():
            if not session.state.subscriptions:
                continue
            if self._session_matches(session, topic_levels):
                if session.accept(message):
                    delivered += 1
                else:
                    dropped += 1
                    self.metrics.drop(session.client_id, "queue_full")
            self.metrics.set_backlog(session.client_id, session.backlog())
        self.metrics.inc("published_total")
        self.metrics.inc("delivered_total", delivered)
        if dropped:
            self.metrics.inc("dropped_total", dropped)
        logger.info(
            "publish seq=%d topic=%r publisher=%s retained=%s fanout=%d dropped=%d",
            message.seq, topic, publisher, retained, delivered, dropped,
        )
        return message

    # ---- 订阅 ----
    def subscribe(self, client_id: str, topic_filter: str, qos: int = 1) -> dict:
        filter_levels = validate_subscribe_filter(topic_filter)
        if not self.auth.can_subscribe(client_id, topic_filter):
            self._reject(ErrorCode.SUBSCRIBE_FORBIDDEN, client_id, topic_filter)
            raise GatewayError(
                ErrorCode.SUBSCRIBE_FORBIDDEN,
                f"client {client_id!r} not allowed to subscribe {topic_filter!r}",
            )
        session = self.registry.get_or_create(client_id)
        is_new = session.add_subscription(Subscription(client_id, topic_filter, qos=1))
        retained_hits = self.matching_retained(topic_filter) if is_new else []
        # 新订阅者：立即接收匹配的保留消息（每个主题最后一条）
        for rmsg in retained_hits:
            if not session.accept(rmsg):
                self.metrics.drop(client_id, "queue_full")
                logger.warning("retained delivery dropped for %s on %s", client_id, rmsg.topic)
        self.metrics.set_backlog(client_id, session.backlog())
        logger.info(
            "subscribe client=%s filter=%r new=%s retained_matched=%d subscriptions=%s",
            client_id, topic_filter, is_new, len(retained_hits), session.snapshot()["subscriptions"],
        )
        return {
            "filter": topic_filter,
            "is_new": is_new,
            "retained_matched": len(retained_hits),
        }

    def unsubscribe(self, client_id: str, topic_filter: str) -> dict:
        validate_subscribe_filter(topic_filter)
        session = self.registry.get(client_id)
        removed = session.remove_subscription(topic_filter) if session else False
        if session:
            self.metrics.set_backlog(client_id, session.backlog())
        logger.info("unsubscribe client=%s filter=%r removed=%s", client_id, topic_filter, removed)
        return {"filter": topic_filter, "removed": removed}

    def matching_retained(self, topic_filter: str) -> list[Message]:
        filter_levels = validate_subscribe_filter(topic_filter)
        hits = [m for t, m in self._retained.items() if matches(filter_levels, t.split("/"))]
        hits.sort(key=lambda m: m.topic)
        return hits

    # ---- 连接生命周期 ----
    def connect(self, client_id: str) -> tuple[Session, list[Delivery]]:
        session = self.registry.get_or_create(client_id)
        deliveries = session.attach()
        recovered = sum(1 for d in deliveries if d.duplicate)
        if recovered:
            self.metrics.inc("reconnect_recovered_total", recovered)
        self.metrics.set_backlog(client_id, session.backlog())
        logger.info(
            "connect client=%s redeliver=%d fresh=%d backlog=%d",
            client_id, recovered, len(deliveries) - recovered, session.backlog(),
        )
        return session, deliveries

    def disconnect(self, client_id: str) -> None:
        session = self.registry.get(client_id)
        if session is not None and session.connected:
            session.detach()
            self.metrics.set_backlog(client_id, session.backlog())
            logger.info("disconnect client=%s backlog_retained=%d", client_id, session.backlog())

    def acknowledge(self, client_id: str, delivery_id: int) -> str:
        session = self.registry.get(client_id)
        if session is None:
            raise GatewayError(ErrorCode.SESSION_NOT_FOUND, f"no session for {client_id!r}")
        result = session.acknowledge(delivery_id)
        if result == "duplicate":
            self.metrics.inc("duplicate_acks_total")
        self.metrics.set_backlog(client_id, session.backlog())
        return result

    def reconcile_subscriptions(self) -> list[dict]:
        """权限规则热更新后调用：移除既有会话中不再被允许的订阅。

        保证既有连接与后续新请求按同一套 ACL 判定；被撤销订阅的在途
        消息保留（仍按会话确认协议处理），之后不再收到新匹配消息。
        """
        revoked: list[dict] = []
        for session in self.registry.all_sessions():
            for topic_filter in list(session.state.subscriptions):
                if not self.auth.can_subscribe(session.client_id, topic_filter):
                    session.remove_subscription(topic_filter)
                    revoked.append({"client_id": session.client_id, "filter": topic_filter})
                    logger.info(
                        "revoke subscription client=%s filter=%r reason=acl_update",
                        session.client_id, topic_filter,
                    )
            self.metrics.set_backlog(session.client_id, session.backlog())
        return revoked

    def abandon(self, client_id: str, delivery_id: int, reason: str = "send_queue_full") -> bool:
        """传输层（慢消费者发送队列满）放弃一条在途投递并腾窗口。"""
        session = self.registry.get(client_id)
        abandoned = session.abandon(delivery_id, reason) if session else False
        if abandoned:
            self.metrics.drop(client_id, reason)
            self.metrics.set_backlog(client_id, session.backlog())
        return abandoned

    # ---- 可观测 ----
    def metrics_snapshot(self) -> dict:
        snap = self.metrics.snapshot()
        snap["sessions"] = [s.snapshot() for s in self.registry.all_sessions()]
        return snap

    def retained_snapshot(self) -> dict:
        return {t: {"seq": m.seq, "payload": m.payload} for t, m in self._retained.items()}

    # ---- 内部 ----
    def _session_matches(self, session: Session, topic_levels: list[str]) -> bool:
        return any(
            matches(f.split("/"), topic_levels)
            for f in session.state.subscriptions
        )

    def _reject(self, code: ErrorCode, client_id: str | None, target: str) -> None:
        self.metrics.reject(code.value)
        logger.warning("reject code=%s client=%s target=%r", code.value, client_id, target)
