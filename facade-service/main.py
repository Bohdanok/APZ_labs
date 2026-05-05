"""facade-service — Task 1 banking API on top of the Lab 3 cluster.

POST /transaction  {user_id, amount}    →  fans out concurrently to:
                                            * a randomly chosen logging-service
                                              instance (with fallback to the
                                              other instances on RequestError)
                                            * counter-service (PostgreSQL)
                                          returns {transaction_id, balance}

GET  /user/{user_id}                    →  {balance, transactions}
GET  /accounts                          →  {balances}
GET  /timings , POST /timings/reset     →  per-backend cumulative wall time
                                            (separately for logging vs counter)
"""
from __future__ import annotations

import asyncio
import os
import random
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

LOGGING_NODES = [n.strip() for n in os.getenv(
    "LOGGING_NODES", "http://localhost:8001"
).split(",") if n.strip()]
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002")


class Stats:
    def __init__(self) -> None:
        self.logging_total = 0.0
        self.counter_total = 0.0
        self.logging_calls = 0
        self.counter_calls = 0
        self.logging_failovers = 0
        self.lock = asyncio.Lock()

    async def add(self, which: str, dt: float) -> None:
        async with self.lock:
            if which == "logging":
                self.logging_total += dt
                self.logging_calls += 1
            else:
                self.counter_total += dt
                self.counter_calls += 1

    async def add_failover(self) -> None:
        async with self.lock:
            self.logging_failovers += 1

    async def reset(self) -> None:
        async with self.lock:
            self.logging_total = 0.0
            self.counter_total = 0.0
            self.logging_calls = 0
            self.counter_calls = 0
            self.logging_failovers = 0

    def snapshot(self) -> dict:
        return {
            "logging_total_seconds": self.logging_total,
            "counter_total_seconds": self.counter_total,
            "logging_calls": self.logging_calls,
            "counter_calls": self.counter_calls,
            "logging_failovers": self.logging_failovers,
            "logging_avg_ms": (self.logging_total / self.logging_calls * 1000)
            if self.logging_calls else 0.0,
            "counter_avg_ms": (self.counter_total / self.counter_calls * 1000)
            if self.counter_calls else 0.0,
        }


stats = Stats()


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    app.state.client = httpx.AsyncClient(timeout=10.0, limits=limits)
    print(f"[facade] logging nodes = {LOGGING_NODES}")
    print(f"[facade] counter url   = {COUNTER_URL}")
    yield
    await app.state.client.aclose()


app = FastAPI(title="facade-service", lifespan=lifespan)


class TransactionIn(BaseModel):
    user_id: str
    amount: float


async def _logging_request(method: str, path: str, json_body: dict | None = None) -> httpx.Response:
    """Pick a random logging-service node; on RequestError fall through to the
    others in random order. Each *attempt's* wall-time is added to stats so
    failed-over calls also show up in the timing.
    """
    client: httpx.AsyncClient = app.state.client
    nodes = LOGGING_NODES.copy()
    random.shuffle(nodes)
    last_exc: Exception | None = None
    for idx, base in enumerate(nodes):
        url = f"{base}{path}"
        t0 = time.perf_counter()
        try:
            if method == "GET":
                r = await client.get(url)
            else:
                r = await client.post(url, json=json_body)
            r.raise_for_status()
            await stats.add("logging", time.perf_counter() - t0)
            if idx > 0:
                await stats.add_failover()
            return r
        except (httpx.RequestError, httpx.HTTPStatusError) as e:
            await stats.add("logging", time.perf_counter() - t0)
            last_exc = e
            continue
    raise HTTPException(503, f"all logging nodes failed: {last_exc}")


async def _counter_request(method: str, path: str, json_body: dict | None = None) -> httpx.Response:
    client: httpx.AsyncClient = app.state.client
    url = f"{COUNTER_URL}{path}"
    t0 = time.perf_counter()
    try:
        if method == "GET":
            r = await client.get(url)
        else:
            r = await client.post(url, json=json_body)
        r.raise_for_status()
        return r
    finally:
        await stats.add("counter", time.perf_counter() - t0)


@app.post("/transaction")
async def post_transaction(txn: TransactionIn):
    transaction_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex[:8]}"
    payload = {"transaction_id": transaction_id, "user_id": txn.user_id, "amount": txn.amount}

    log_task = asyncio.create_task(_logging_request("POST", "/log", payload))
    cnt_task = asyncio.create_task(_counter_request("POST", "/balance", payload))
    try:
        _, cnt_resp = await asyncio.gather(log_task, cnt_task)
    except httpx.HTTPError as e:
        raise HTTPException(502, f"upstream error: {e}")

    return {"transaction_id": transaction_id, "balance": cnt_resp.json()["balance"]}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    bal_t = asyncio.create_task(_counter_request("GET", f"/balance/{user_id}"))
    log_t = asyncio.create_task(_logging_request("GET", f"/log/user/{user_id}"))
    bal, log = await asyncio.gather(bal_t, log_t)
    return {
        "user_id": user_id,
        "balance": bal.json()["balance"],
        "transactions": log.json()["transactions"],
    }


@app.get("/accounts")
async def get_accounts():
    r = await _counter_request("GET", "/balance")
    return r.json()


@app.get("/transactions")
async def get_transactions():
    """All transactions from a randomly chosen logging-service replica."""
    r = await _logging_request("GET", "/log")
    return r.json()


@app.get("/timings")
async def get_timings():
    return stats.snapshot()


@app.post("/timings/reset")
async def reset_timings():
    await stats.reset()
    return {"status": "ok"}


@app.post("/admin/reset")
async def admin_reset():
    """Wipes counter balances. Used between perf scenarios.

    Hazelcast map is cleared opportunistically — we don't fail the call if
    individual logging instances are unreachable (e.g. mid-failover test).
    """
    client: httpx.AsyncClient = app.state.client
    errors: list[str] = []
    try:
        await client.post(f"{COUNTER_URL}/balance/reset")
    except Exception as e:
        errors.append(f"counter: {e}")
    for base in LOGGING_NODES:
        try:
            await client.post(f"{base}/log/clear")
        except Exception:
            pass
    await stats.reset()
    return {"status": "ok", "errors": errors}


@app.get("/health")
async def health():
    return {"status": "ok"}
