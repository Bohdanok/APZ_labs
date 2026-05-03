"""facade-service — Task 1 banking API on top of the Lab 5 K8s deployment.

Service discovery: the URLs of logging-service and counter-service are
read from the K8s ConfigMap (LOGGING_SERVICE_URL, COUNTER_SERVICE_URL),
which resolve through K8s DNS to the corresponding ClusterIP Services —
so the facade reaches whichever pod kube-proxy load-balances to. No
static addresses in code.

Architecture difference vs Lab 3:
- POST /transaction fans out concurrently to:
    * logging-service via HTTP (sync, write to Hazelcast Distributed Map)
    * Hazelcast Queue ("messages_queue") — counter-service consumes
      asynchronously and persists to PostgreSQL.
  Counter is decoupled from the request path. The facade-observed
  "counter contribution" is just MQ enqueue time.

- Per-instance accumulators expose the timing breakdown (logging total,
  MQ enqueue total) at GET /timings, comparable to Labs 1 and 3.
- /admin/reset wipes counter balances + clears the HZ map + drains the
  queue, so perf scenarios start from a clean slate.
"""
import asyncio
import json
import os
import socket
import time
import uuid

import hazelcast
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

LOGGING_SERVICE_URL = os.environ["LOGGING_SERVICE_URL"]
COUNTER_SERVICE_URL = os.environ["COUNTER_SERVICE_URL"]
HZ_NODES = [n.strip() for n in os.environ["HZ_NODES"].split(",")]
HZ_CLUSTER_NAME = os.environ["HZ_CLUSTER_NAME"]
MQ_QUEUE_NAME = os.environ["MQ_QUEUE_NAME"]

hz_client = None
counter_queue = None
http_client: httpx.AsyncClient | None = None


class Stats:
    def __init__(self):
        self.logging_total = 0.0
        self.logging_calls = 0
        self.mq_total = 0.0
        self.mq_calls = 0
        self.lock = asyncio.Lock()

    async def add_logging(self, dt):
        async with self.lock:
            self.logging_total += dt
            self.logging_calls += 1

    async def add_mq(self, dt):
        async with self.lock:
            self.mq_total += dt
            self.mq_calls += 1

    async def reset(self):
        async with self.lock:
            self.logging_total = 0.0
            self.logging_calls = 0
            self.mq_total = 0.0
            self.mq_calls = 0

    def snapshot(self):
        return {
            "instance": INSTANCE_ID,
            "logging_total_seconds": self.logging_total,
            "logging_calls": self.logging_calls,
            "logging_avg_ms": (self.logging_total / self.logging_calls * 1000)
                if self.logging_calls else 0.0,
            "mq_total_seconds": self.mq_total,
            "mq_calls": self.mq_calls,
            "mq_avg_ms": (self.mq_total / self.mq_calls * 1000) if self.mq_calls else 0.0,
        }


stats = Stats()


@app.on_event("startup")
async def startup():
    global hz_client, counter_queue, http_client
    print(f"[facade:{INSTANCE_ID}] cfg LOGGING={LOGGING_SERVICE_URL} "
          f"COUNTER={COUNTER_SERVICE_URL} HZ_NODES={HZ_NODES} "
          f"cluster={HZ_CLUSTER_NAME} queue={MQ_QUEUE_NAME}", flush=True)
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    http_client = httpx.AsyncClient(timeout=10.0, limits=limits)
    for i in range(20):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES, cluster_name=HZ_CLUSTER_NAME
            )
            counter_queue = hz_client.get_queue(MQ_QUEUE_NAME).blocking()
            print(f"[facade:{INSTANCE_ID}] connected to MQ", flush=True)
            return
        except Exception as e:
            print(f"[facade:{INSTANCE_ID}] MQ retry {i}: {e}", flush=True)
            await asyncio.sleep(3)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


class TransactionIn(BaseModel):
    user_id: str
    amount: float


async def _post_logging(payload: dict) -> str:
    t0 = time.perf_counter()
    try:
        r = await http_client.post(f"{LOGGING_SERVICE_URL}/log", json=payload)
        r.raise_for_status()
        return r.json().get("instance", "?")
    finally:
        await stats.add_logging(time.perf_counter() - t0)


async def _enqueue_counter(payload: dict) -> None:
    t0 = time.perf_counter()
    try:
        # Hazelcast queue.put is blocking — run in the default executor
        # so we don't stall the event loop on slow MQ.
        await asyncio.get_running_loop().run_in_executor(
            None, lambda: counter_queue.put(json.dumps(payload))
        )
    finally:
        await stats.add_mq(time.perf_counter() - t0)


@app.post("/transaction")
async def post_transaction(txn: TransactionIn):
    if counter_queue is None:
        raise HTTPException(503, "MQ not connected yet")
    transaction_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex[:8]}"
    payload = {
        "transaction_id": transaction_id,
        "user_id": txn.user_id,
        "amount": txn.amount,
    }

    log_task = asyncio.create_task(_post_logging(payload))
    mq_task = asyncio.create_task(_enqueue_counter(payload))
    try:
        log_pod, _ = await asyncio.gather(log_task, mq_task)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"logging-service: {e}")

    return {
        "transaction_id": transaction_id,
        "status": "accepted",
        "facade_instance": INSTANCE_ID,
        "logging_pod": log_pod,
    }


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    bal_task = asyncio.create_task(
        http_client.get(f"{COUNTER_SERVICE_URL}/balance/{user_id}")
    )
    log_task = asyncio.create_task(
        http_client.get(f"{LOGGING_SERVICE_URL}/log/user/{user_id}")
    )
    bal_resp, log_resp = await asyncio.gather(bal_task, log_task)
    bal_resp.raise_for_status()
    log_resp.raise_for_status()
    return {
        "user_id": user_id,
        "balance": bal_resp.json()["balance"],
        "transactions": log_resp.json()["transactions"],
        "counter_pod": bal_resp.json().get("instance"),
        "logging_pod": log_resp.json().get("instance"),
    }


@app.get("/accounts")
async def get_accounts():
    r = await http_client.get(f"{COUNTER_SERVICE_URL}/balance")
    r.raise_for_status()
    return r.json()


@app.get("/timings")
async def get_timings():
    return stats.snapshot()


@app.post("/timings/reset")
async def reset_timings():
    await stats.reset()
    return {"status": "ok", "instance": INSTANCE_ID}


@app.post("/admin/reset")
async def admin_reset():
    """Clears state for a fresh perf scenario:
       - drain the MQ queue
       - clear Hazelcast Distributed Map (transactions log)
       - TRUNCATE accounts in PostgreSQL
       - reset facade timing counters

    Best-effort: failures of individual steps are reported but don't abort.
    """
    errors: list[str] = []

    # Drain the MQ queue without blocking the event loop.
    try:
        if counter_queue is not None:
            def _drain():
                drained = 0
                while counter_queue.poll(0) is not None:
                    drained += 1
                return drained
            drained = await asyncio.get_running_loop().run_in_executor(None, _drain)
            print(f"[facade:{INSTANCE_ID}] drained {drained} messages from MQ", flush=True)
    except Exception as e:
        errors.append(f"mq drain: {e}")

    try:
        r = await http_client.post(f"{LOGGING_SERVICE_URL}/log/clear", timeout=10.0)
        r.raise_for_status()
    except Exception as e:
        errors.append(f"logging clear: {e}")

    try:
        r = await http_client.post(f"{COUNTER_SERVICE_URL}/balance/reset", timeout=10.0)
        r.raise_for_status()
    except Exception as e:
        errors.append(f"counter reset: {e}")

    await stats.reset()
    return {"status": "ok" if not errors else "partial", "errors": errors,
            "instance": INSTANCE_ID}
