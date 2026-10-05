import asyncio, logging, time
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03d %(message)s")
import gateway.app as appmod

# 包一层：在 finally 各阶段打日志
orig = appmod.create_app
from fastapi.testclient import TestClient
from tests.conftest import RULES, TOKENS, auth

MAX_QUEUE = 4; MAX_INFLIGHT = 4; TOTAL = 40
for attempt in range(6):
    app = orig(max_queue=MAX_QUEUE, max_inflight=MAX_INFLIGHT)
    app.state.acl.set_tokens(dict(TOKENS)); app.state.acl.set_rules(list(RULES))
    with TestClient(app) as client:
        with client.websocket_connect("/ws?token=t-sub&client_id=slow-1") as slow, \
             client.websocket_connect("/ws?token=t-sub&client_id=fast-1") as fast:
            slow.receive_json(); fast.receive_json()
            for ws in (slow, fast):
                ws.send_json({"op": "subscribe", "filter": "robot/#"}); ws.receive_json()
            for i in range(TOTAL):
                client.post("/publish", json={"topic": "robot/arm", "payload": f"m{i}"}, headers=auth("t-pub"))
                msg = fast.receive_json()
                fast.send_json({"op": "ack", "seq": msg["message"]["seq"]})
        t0 = time.monotonic()
        conserved = None
        for _ in range(200):
            stats = client.get("/stats", headers=auth("t-sub")).json()
            s = {x["client_id"]: x for x in stats["sessions"]}["slow-1"]
            conserved = s["pending_unacked"] + s["ready"] + s["backlog"] + s["dropped"]
            if not s["connected"] and conserved == TOTAL:
                break
            time.sleep(0.01)
        logging.info("attempt %d: settled=%s after %.3fs connected=%s conserved=%s",
                     attempt, conserved == TOTAL and not s["connected"], time.monotonic() - t0, s["connected"], conserved)
