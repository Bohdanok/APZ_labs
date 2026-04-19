from fastapi import FastAPI, Body, HTTPException
from fastapi.responses import PlainTextResponse
import uuid
import httpx
import os
import random
import hazelcast
import json

app = FastAPI()

CONFIG_SERVER = os.getenv("CONFIG_SERVER", "http://config-server:8000")
hz_nodes = os.getenv("HZ_NODES", "hz1:5701,hz2:5701,hz3:5701").split(",")

try:
    hz_client = hazelcast.HazelcastClient(cluster_members=hz_nodes, cluster_name="dev")
    counter_queue = hz_client.get_queue("messages_queue").blocking()
except Exception as e:
    print(f"Hazelcast setup failed: {e}")
    counter_queue = None

async def get_service_urls(service_name: str):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{CONFIG_SERVER}/services/{service_name}", timeout=3.0)
            resp.raise_for_status()
            return resp.json()
        except httpx.RequestError:
            return []

@app.post("/facade")
async def post_facade(msg: str = Body(..., embed=True)):
    msg_uuid = str(uuid.uuid4())
    payload = {"uuid": msg_uuid, "msg": msg}
    
    # 1. Sync HTTP POST to Logging Service (via Registry)
    logging_nodes = await get_service_urls("logging-service")
    if not logging_nodes:
        raise HTTPException(status_code=503, detail="logging-service недоступний")
    
    random.shuffle(logging_nodes)
    success = False
    async with httpx.AsyncClient() as client:
        for node in logging_nodes:
            try:
                await client.post(f"{node}/log", json=payload, timeout=2.0)
                success = True
                break
            except httpx.RequestError:
                continue

    if not success:
        raise HTTPException(status_code=503, detail="Всі екземпляри logging-service недоступні")

    # 2. Async Message Queue to Counter Service
    if counter_queue:
        counter_queue.put(json.dumps({"msg": msg}))
    else:
        raise HTTPException(status_code=500, detail="Черга повідомлень недоступна")

    return {"status": "ok", "uuid": msg_uuid}

@app.get("/facade", response_class=PlainTextResponse)
async def get_facade():
    logging_nodes = await get_service_urls("logging-service")
    counter_nodes = await get_service_urls("counter-service")
    
    log_response_text = "Логи недоступні"
    async with httpx.AsyncClient() as client:
        if logging_nodes:
            random.shuffle(logging_nodes)
            for node in logging_nodes:
                try:
                    resp = await client.get(f"{node}/log", timeout=2.0)
                    log_response_text = resp.text
                    break
                except httpx.RequestError:
                    continue
        
        msg_text = "null" # Graceful fallback if counter is down
        if counter_nodes:
            try:
                # Assuming 1 counter service for GET read
                msg_response = await client.get(f"{counter_nodes[0]}/message", timeout=2.0)
                msg_text = msg_response.text
            except httpx.RequestError:
                pass
        
    return f"{log_response_text} | Рахунок: {msg_text}"
