import os
import socket
import asyncio
import hazelcast
from fastapi import FastAPI, HTTPException
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

app = FastAPI()

INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

HZ_NODES = [n.strip() for n in os.environ["HZ_NODES"].split(",")]
HZ_CLUSTER_NAME = os.environ["HZ_CLUSTER_NAME"]
HZ_MAP_NAME = os.environ["HZ_MAP_NAME"]

hz_client = None
message_map = None


@app.on_event("startup")
async def startup():
    global hz_client, message_map
    print(f"[logging:{INSTANCE_ID}] config -> HZ_NODES={HZ_NODES} cluster={HZ_CLUSTER_NAME} map={HZ_MAP_NAME}",
          flush=True)
    for i in range(20):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES, cluster_name=HZ_CLUSTER_NAME
            )
            message_map = hz_client.get_map(HZ_MAP_NAME).blocking()
            print(f"[logging:{INSTANCE_ID}] connected to Hazelcast", flush=True)
            return
        except Exception as e:
            print(f"[logging:{INSTANCE_ID}] HZ retry {i}: {e}", flush=True)
            await asyncio.sleep(3)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


class LogMessage(BaseModel):
    uuid: str
    msg: str


@app.post("/log")
async def log_message(data: LogMessage):
    if message_map is None:
        raise HTTPException(503, "hazelcast unavailable")
    message_map.put(data.uuid, data.msg)
    print(f"[logging:{INSTANCE_ID}] stored {data.uuid} -> {data.msg}", flush=True)
    return {"status": "success", "instance": INSTANCE_ID}


@app.get("/log", response_class=PlainTextResponse)
async def get_logs():
    if message_map is None:
        return "Hazelcast unavailable"
    values = list(message_map.values())
    return ", ".join(values) if values else "No messages"
