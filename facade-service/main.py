import os
import uuid
import json
import asyncio
import time
import socket
import httpx
import hazelcast
from fastapi import FastAPI, Body, HTTPException
from fastapi.responses import PlainTextResponse

app = FastAPI()

INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

LOGGING_SERVICE_URL = os.environ["LOGGING_SERVICE_URL"]
COUNTER_SERVICE_URL = os.environ["COUNTER_SERVICE_URL"]
HZ_NODES = [n.strip() for n in os.environ["HZ_NODES"].split(",")]
HZ_CLUSTER_NAME = os.environ["HZ_CLUSTER_NAME"]
MQ_QUEUE_NAME = os.environ["MQ_QUEUE_NAME"]

hz_client = None
counter_queue = None


@app.on_event("startup")
async def startup():
    global hz_client, counter_queue
    print(f"[facade:{INSTANCE_ID}] config -> LOGGING={LOGGING_SERVICE_URL} COUNTER={COUNTER_SERVICE_URL} "
          f"HZ_NODES={HZ_NODES} HZ_CLUSTER={HZ_CLUSTER_NAME} MQ={MQ_QUEUE_NAME}", flush=True)
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


@app.post("/facade")
async def post_facade(msg: str = Body(..., embed=True)):
    msg_uuid = str(uuid.uuid4())
    payload = {"uuid": msg_uuid, "msg": msg}

    t0 = time.perf_counter()
    log_pod = "?"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.post(f"{LOGGING_SERVICE_URL}/log", json=payload, timeout=2.0)
            if r.status_code != 200:
                raise HTTPException(503, f"logging-service returned {r.status_code}")
            try:
                log_pod = r.json().get("instance", "?")
            except Exception:
                pass
        except httpx.RequestError as e:
            raise HTTPException(503, f"logging-service unreachable: {e}")
    log_dt = time.perf_counter() - t0

    t1 = time.perf_counter()
    if counter_queue is None:
        raise HTTPException(500, "MQ unavailable")
    counter_queue.put(json.dumps({"msg": msg}))
    mq_dt = time.perf_counter() - t1

    print(f"[facade:{INSTANCE_ID}] uuid={msg_uuid} logging_pod={log_pod} "
          f"log_dt={log_dt*1000:.1f}ms mq_dt={mq_dt*1000:.1f}ms", flush=True)
    return {
        "status": "ok",
        "uuid": msg_uuid,
        "facade_instance": INSTANCE_ID,
        "logging_pod": log_pod,
        "log_ms": round(log_dt * 1000, 2),
        "mq_ms": round(mq_dt * 1000, 2),
    }


@app.get("/facade", response_class=PlainTextResponse)
async def get_facade():
    log_text = "logs unavailable"
    msg_text = "null"
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"{LOGGING_SERVICE_URL}/log", timeout=2.0)
            log_text = r.text
        except httpx.RequestError:
            pass
        try:
            r = await client.get(f"{COUNTER_SERVICE_URL}/message", timeout=2.0)
            msg_text = r.text
        except httpx.RequestError:
            pass
    return f"facade={INSTANCE_ID} | logs=[{log_text}] | counter=[{msg_text}]"
