"""counter-service — per-user balances in PostgreSQL.

Replaces the previous "INSERT msg / SELECT COUNT(*)" stub. The Task 1 spec
that Lab 3 inherits requires balance tracking: each transaction
{user_id, ±amount} updates that user's balance, and the service can
return one user's balance or all balances.

Atomicity under concurrency is handled by a single SQL statement —
   INSERT ... ON CONFLICT (user_id) DO UPDATE SET balance = ... RETURNING balance
PostgreSQL's row-level lock on the UPDATE serialises concurrent writers
to the same account, so 10 clients hammering one user_id end at the
correct balance with no lost updates.
"""
from __future__ import annotations

import asyncio
import os
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "messages_db")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

DDL = """
CREATE TABLE IF NOT EXISTS accounts (
    user_id TEXT PRIMARY KEY,
    balance NUMERIC NOT NULL DEFAULT 0
);
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    last_err = None
    for attempt in range(20):
        try:
            pool = await asyncpg.create_pool(
                host=DB_HOST, port=DB_PORT, user=DB_USER,
                password=DB_PASS, database=DB_NAME,
                min_size=2, max_size=20,
            )
            break
        except Exception as e:
            last_err = e
            print(f"[counter] postgres not ready (attempt {attempt + 1}): {e}")
            await asyncio.sleep(1.5)
    else:
        raise RuntimeError(f"could not connect to postgres: {last_err}")

    async with pool.acquire() as conn:
        await conn.execute(DDL)

    app.state.pool = pool
    print("[counter] connected to postgres, accounts table ready")
    yield
    await pool.close()


app = FastAPI(title="counter-service", lifespan=lifespan)


class TxnIn(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@app.post("/balance")
async def apply_txn(t: TxnIn):
    pool: asyncpg.Pool = app.state.pool
    async with pool.acquire() as conn:
        balance = await conn.fetchval(
            """
            INSERT INTO accounts (user_id, balance)
            VALUES ($1, $2)
            ON CONFLICT (user_id)
            DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
            RETURNING balance
            """,
            t.user_id,
            t.amount,
        )
    if os.getenv("VERBOSE_LOG", "0") == "1":
        print(f"[counter] {t.transaction_id} user={t.user_id} {t.amount:+} -> {balance}")
    return {"user_id": t.user_id, "balance": float(balance)}


@app.get("/balance/{user_id}")
async def get_user(user_id: str):
    pool: asyncpg.Pool = app.state.pool
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance FROM accounts WHERE user_id = $1", user_id
        )
    bal = float(row["balance"]) if row else 0.0
    return {"user_id": user_id, "balance": bal}


@app.get("/balance")
async def get_all():
    pool: asyncpg.Pool = app.state.pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, balance FROM accounts")
    return {"balances": {r["user_id"]: float(r["balance"]) for r in rows}}


@app.post("/balance/reset")
async def reset_all():
    """Convenience for perf-test runs — wipes all balances."""
    pool: asyncpg.Pool = app.state.pool
    async with pool.acquire() as conn:
        await conn.execute("TRUNCATE accounts")
    return {"status": "ok"}


@app.get("/health")
async def health():
    pool: asyncpg.Pool = getattr(app.state, "pool", None)
    if pool is None:
        raise HTTPException(503, "no pool")
    async with pool.acquire() as conn:
        await conn.execute("SELECT 1")
    return {"status": "ok"}
