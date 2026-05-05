"""logging-service — transaction store backed by Hazelcast Distributed Map.

Each replica connects as a Hazelcast client to the 3-node cluster
(hz1, hz2, hz3). Transactions are stored as JSON strings in a single
distributed map keyed by transaction_id; any logging-service instance
returns the same data because the map is cluster-wide.

Schema of the value: {"transaction_id", "user_id", "amount"}.
"""
from __future__ import annotations

import json
import os
import socket
import time
from contextlib import asynccontextmanager

import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

HZ_NODES = os.getenv("HZ_NODES", "localhost:5701").split(",")
CLUSTER_NAME = os.getenv("HZ_CLUSTER", "dev")
INSTANCE_ID = os.getenv("INSTANCE_ID", socket.gethostname())


def _connect_hazelcast():
    last_err = None
    for attempt in range(20):
        try:
            client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES,
                cluster_name=CLUSTER_NAME,
                cluster_connect_timeout=10.0,
            )
            return client
        except Exception as e:
            last_err = e
            print(f"[{INSTANCE_ID}] hazelcast connect failed (attempt {attempt + 1}): {e}")
            time.sleep(2.0)
    raise RuntimeError(f"could not connect to hazelcast: {last_err}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    client = _connect_hazelcast()
    txn_map = client.get_map("transactions").blocking()
    app.state.client = client
    app.state.txn_map = txn_map
    print(f"[{INSTANCE_ID}] connected to hazelcast nodes={HZ_NODES}")
    yield
    client.shutdown()


app = FastAPI(title="logging-service", lifespan=lifespan)


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@app.post("/log")
async def log_txn(t: Transaction):
    txn_map = app.state.txn_map
    payload = t.model_dump()
    # putIfAbsent makes this idempotent on retries / fan-out duplicates.
    existing = txn_map.put_if_absent(t.transaction_id, json.dumps(payload))
    if existing is not None:
        return {"status": "duplicate", "transaction_id": t.transaction_id, "instance": INSTANCE_ID}
    print(f"[{INSTANCE_ID}] stored tid={t.transaction_id} user={t.user_id} amount={t.amount:+}")
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/log")
async def get_all():
    txn_map = app.state.txn_map
    values = txn_map.values()
    return {
        "instance": INSTANCE_ID,
        "transactions": [json.loads(v) for v in values],
    }


@app.get("/log/user/{user_id}")
async def get_by_user(user_id: str):
    txn_map = app.state.txn_map
    values = txn_map.values()
    txns = [json.loads(v) for v in values]
    return {
        "instance": INSTANCE_ID,
        "transactions": [t for t in txns if t["user_id"] == user_id],
    }


@app.get("/log/{transaction_id}")
async def get_by_id(transaction_id: str):
    txn_map = app.state.txn_map
    raw = txn_map.get(transaction_id)
    if raw is None:
        raise HTTPException(404, "not found")
    return json.loads(raw)


@app.post("/log/clear")
async def clear_all():
    """Wipes the distributed map — used between perf scenarios."""
    txn_map = app.state.txn_map
    txn_map.clear()
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/health")
async def health():
    txn_map = getattr(app.state, "txn_map", None)
    if txn_map is None:
        raise HTTPException(503, "no map")
    return {"status": "ok", "instance": INSTANCE_ID, "size": txn_map.size()}
