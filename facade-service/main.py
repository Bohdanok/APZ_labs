from fastapi import FastAPI, Body, HTTPException
from fastapi.responses import PlainTextResponse
import uuid
import httpx
import os
import random

app = FastAPI()

LOGGING_NODES = os.getenv("LOGGING_NODES", "http://localhost:8001").split(",")
MESSAGES_URL = os.getenv("MESSAGES_URL", "http://localhost:8002/message")

@app.post("/facade")
async def post_facade(msg: str = Body(..., embed=True)):
    msg_uuid = str(uuid.uuid4())
    payload = {"uuid": msg_uuid, "msg": msg}
    
    nodes = list(LOGGING_NODES)
    random.shuffle(nodes)
    
    success = False
    async with httpx.AsyncClient() as client:
        for node in nodes:
            try:
                await client.post(f"{node}/log", json=payload, timeout=2.0)
                success = True
                break
            except httpx.RequestError:
                continue
        
        try:
            await client.post(MESSAGES_URL, json={"msg": msg})
        except httpx.RequestError:
            pass

    if not success:
        raise HTTPException(status_code=503, detail="Всі екземпляри logging-service недоступні")
        
    return {"status": "ok", "uuid": msg_uuid}

@app.get("/facade", response_class=PlainTextResponse)
async def get_facade():
    nodes = list(LOGGING_NODES)
    random.shuffle(nodes)
    
    log_response_text = "Логи недоступні"
    async with httpx.AsyncClient() as client:
        for node in nodes:
            try:
                resp = await client.get(f"{node}/log", timeout=2.0)
                log_response_text = resp.text
                break
            except httpx.RequestError:
                continue
        
        try:
            msg_response = await client.get(MESSAGES_URL, timeout=2.0)
            msg_text = msg_response.text
        except httpx.RequestError:
            msg_text = "Counter-service недоступний"
        
    return f"{log_response_text}: {msg_text}"