from fastapi import FastAPI, Body
from fastapi.responses import PlainTextResponse
import uuid
import httpx

app = FastAPI()

LOGGING_URL = "http://localhost:8001/log"
MESSAGES_URL = "http://localhost:8002/message"

@app.post("/facade")
async def post_facade(msg: str = Body(..., embed=True)):
    msg_uuid = str(uuid.uuid4())
    payload = {"uuid": msg_uuid, "msg": msg}
    
    async with httpx.AsyncClient() as client:
        await client.post(LOGGING_URL, json=payload)
        
    return {"status": "ok", "uuid": msg_uuid}

@app.get("/facade", response_class=PlainTextResponse)
async def get_facade():
    async with httpx.AsyncClient() as client:
        log_response = await client.get(LOGGING_URL)
        msg_response = await client.get(MESSAGES_URL)
        
    return f"{log_response.text}: {msg_response.text}"
