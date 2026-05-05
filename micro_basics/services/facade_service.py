import asyncio
import os
import time
import uuid
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

LOGGING_URL = os.getenv("LOGGING_URL", "http://localhost:8001")
COUNTER_URL = os.getenv("COUNTER_URL", "http://localhost:8002")

# Per the assignment: accumulate per-service call durations into two variables.
class Stats:
    def __init__(self) -> None:
        self.logging_total = 0.0
        self.counter_total = 0.0
        self.logging_calls = 0
        self.counter_calls = 0
        self.lock = asyncio.Lock()

    async def add(self, which: str, dt: float) -> None:
        async with self.lock:
            if which == "logging":
                self.logging_total += dt
                self.logging_calls += 1
            else:
                self.counter_total += dt
                self.counter_calls += 1

    async def reset(self) -> None:
        async with self.lock:
            self.logging_total = 0.0
            self.counter_total = 0.0
            self.logging_calls = 0
            self.counter_calls = 0

    def snapshot(self) -> dict:
        return {
            "logging_total_seconds": self.logging_total,
            "counter_total_seconds": self.counter_total,
            "logging_calls": self.logging_calls,
            "counter_calls": self.counter_calls,
            "logging_avg_ms": (self.logging_total / self.logging_calls * 1000)
            if self.logging_calls
            else 0.0,
            "counter_avg_ms": (self.counter_total / self.counter_calls * 1000)
            if self.counter_calls
            else 0.0,
        }


stats = Stats()


@asynccontextmanager
async def lifespan(app: FastAPI):
    limits = httpx.Limits(max_connections=200, max_keepalive_connections=200)
    app.state.client = httpx.AsyncClient(timeout=30.0, limits=limits)
    yield
    await app.state.client.aclose()


app = FastAPI(title="facade-service", lifespan=lifespan)


class TransactionIn(BaseModel):
    user_id: str
    amount: float


async def _post_logging(client: httpx.AsyncClient, payload: dict) -> None:
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{LOGGING_URL}/log", json=payload)
        r.raise_for_status()
    finally:
        await stats.add("logging", time.perf_counter() - t0)


async def _post_counter(client: httpx.AsyncClient, payload: dict) -> dict:
    t0 = time.perf_counter()
    try:
        r = await client.post(f"{COUNTER_URL}/balance", json=payload)
        r.raise_for_status()
        return r.json()
    finally:
        await stats.add("counter", time.perf_counter() - t0)


@app.post("/transaction")
async def post_transaction(txn: TransactionIn):
    transaction_id = f"{int(time.time() * 1_000_000)}-{uuid.uuid4().hex[:8]}"
    payload = {
        "transaction_id": transaction_id,
        "user_id": txn.user_id,
        "amount": txn.amount,
    }
    client: httpx.AsyncClient = app.state.client

    log_task = asyncio.create_task(_post_logging(client, payload))
    counter_task = asyncio.create_task(_post_counter(client, payload))
    try:
        _, counter_resp = await asyncio.gather(log_task, counter_task)
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"upstream error: {e}")

    balance = counter_resp["balance"]
    print(
        f"[facade] tid={transaction_id} user={txn.user_id} {txn.amount:+g} -> balance={balance:g}",
        flush=True,
    )
    return {"transaction_id": transaction_id, "balance": balance}


@app.get("/user/{user_id}")
async def get_user(user_id: str):
    client: httpx.AsyncClient = app.state.client

    async def fetch_balance():
        t0 = time.perf_counter()
        try:
            r = await client.get(f"{COUNTER_URL}/balance/{user_id}")
            r.raise_for_status()
            return r.json()
        finally:
            await stats.add("counter", time.perf_counter() - t0)

    async def fetch_txns():
        t0 = time.perf_counter()
        try:
            r = await client.get(f"{LOGGING_URL}/log/user/{user_id}")
            r.raise_for_status()
            return r.json()
        finally:
            await stats.add("logging", time.perf_counter() - t0)

    bal, txns = await asyncio.gather(fetch_balance(), fetch_txns())
    return {"user_id": user_id, "balance": bal["balance"], "transactions": txns["transactions"]}


@app.get("/accounts")
async def get_accounts():
    client: httpx.AsyncClient = app.state.client
    t0 = time.perf_counter()
    try:
        r = await client.get(f"{COUNTER_URL}/balance")
        r.raise_for_status()
    finally:
        await stats.add("counter", time.perf_counter() - t0)
    return r.json()


@app.get("/timings")
async def get_timings():
    return stats.snapshot()


@app.post("/timings/reset")
async def reset_timings():
    await stats.reset()
    return {"status": "ok"}


@app.get("/health")
async def health():
    return {"status": "ok"}
