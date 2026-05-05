import asyncio

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="logging-service")

transactions: dict[str, dict] = {}
lock = asyncio.Lock()


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@app.post("/log")
async def log_transaction(t: Transaction):
    async with lock:
        if t.transaction_id in transactions:
            print(f"[logging] DUPLICATE tid={t.transaction_id}", flush=True)
            return {"status": "duplicate", "transaction_id": t.transaction_id}
        transactions[t.transaction_id] = t.model_dump()
    print(
        f"[logging] stored tid={t.transaction_id} user={t.user_id} amount={t.amount:+g}",
        flush=True,
    )
    return {"status": "ok"}


@app.get("/log")
async def get_all():
    return {"transactions": list(transactions.values())}


@app.get("/log/user/{user_id}")
async def get_by_user(user_id: str):
    return {
        "transactions": [t for t in transactions.values() if t["user_id"] == user_id]
    }


@app.get("/log/{transaction_id}")
async def get_by_id(transaction_id: str):
    t = transactions.get(transaction_id)
    if t is None:
        raise HTTPException(status_code=404, detail="not found")
    return t


@app.get("/health")
async def health():
    return {"status": "ok", "count": len(transactions)}
