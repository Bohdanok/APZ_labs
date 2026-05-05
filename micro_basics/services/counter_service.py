import asyncio
from collections import defaultdict

from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="counter-service")

balances: dict[str, float] = defaultdict(float)
locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
global_lock = asyncio.Lock()


class TxnIn(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


async def _user_lock(user_id: str) -> asyncio.Lock:
    async with global_lock:
        return locks[user_id]


@app.post("/balance")
async def apply_txn(t: TxnIn):
    lock = await _user_lock(t.user_id)
    async with lock:
        balances[t.user_id] += t.amount
        new_balance = balances[t.user_id]
    print(
        f"[counter] tid={t.transaction_id} user={t.user_id} {t.amount:+g} -> balance={new_balance:g}",
        flush=True,
    )
    return {"user_id": t.user_id, "balance": new_balance}


@app.get("/balance/{user_id}")
async def get_user(user_id: str):
    return {"user_id": user_id, "balance": balances.get(user_id, 0.0)}


@app.get("/balance")
async def get_all():
    return {"balances": dict(balances)}


@app.get("/health")
async def health():
    return {"status": "ok", "users": len(balances)}
