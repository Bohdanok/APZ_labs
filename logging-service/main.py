"""logging-service — transaction store backed by a Hazelcast Distributed Map.

Each replica connects to the 3-node Hazelcast StatefulSet cluster as a
client. Transactions are stored as JSON values keyed by transaction_id;
the map is shared across the cluster, so any logging-service replica
returns the same data.

Schema of the value: {"transaction_id", "user_id", "amount"}.
"""
import asyncio
import json
import os
import socket

import hazelcast
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI()

INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

HZ_NODES = [n.strip() for n in os.environ["HZ_NODES"].split(",")]
HZ_CLUSTER_NAME = os.environ["HZ_CLUSTER_NAME"]
HZ_MAP_NAME = os.environ["HZ_MAP_NAME"]

hz_client = None
txn_map = None


@app.on_event("startup")
async def startup():
    global hz_client, txn_map
    print(f"[logging:{INSTANCE_ID}] cfg HZ_NODES={HZ_NODES} cluster={HZ_CLUSTER_NAME} "
          f"map={HZ_MAP_NAME}", flush=True)
    for i in range(20):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES, cluster_name=HZ_CLUSTER_NAME
            )
            txn_map = hz_client.get_map(HZ_MAP_NAME).blocking()
            print(f"[logging:{INSTANCE_ID}] connected to Hazelcast", flush=True)
            return
        except Exception as e:
            print(f"[logging:{INSTANCE_ID}] HZ retry {i}: {e}", flush=True)
            await asyncio.sleep(3)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


class Transaction(BaseModel):
    transaction_id: str
    user_id: str
    amount: float


@app.post("/log")
async def log_txn(t: Transaction):
    if txn_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    payload = t.model_dump()
    # put_if_absent makes this idempotent on retries / fan-out duplicates.
    existing = txn_map.put_if_absent(t.transaction_id, json.dumps(payload))
    if existing is not None:
        return {"status": "duplicate", "transaction_id": t.transaction_id,
                "instance": INSTANCE_ID}
    if os.environ.get("VERBOSE_LOG", "0") == "1":
        print(f"[logging:{INSTANCE_ID}] stored tid={t.transaction_id} "
              f"user={t.user_id} {t.amount:+}", flush=True)
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/log")
async def get_all():
    if txn_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    return {
        "instance": INSTANCE_ID,
        "transactions": [json.loads(v) for v in txn_map.values()],
    }


@app.get("/log/user/{user_id}")
async def get_by_user(user_id: str):
    if txn_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    txns = [json.loads(v) for v in txn_map.values()]
    return {
        "instance": INSTANCE_ID,
        "transactions": [t for t in txns if t["user_id"] == user_id],
    }


@app.get("/log/{transaction_id}")
async def get_by_id(transaction_id: str):
    if txn_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    raw = txn_map.get(transaction_id)
    if raw is None:
        raise HTTPException(404, "not found")
    return json.loads(raw)


@app.post("/log/clear")
async def clear_all():
    if txn_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    txn_map.clear()
    return {"status": "ok", "instance": INSTANCE_ID}
