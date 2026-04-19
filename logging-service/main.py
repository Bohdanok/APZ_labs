from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
import hazelcast
import httpx
import os
import asyncio

app = FastAPI()

hz_nodes = os.getenv("HZ_NODES", "hz1:5701,hz2:5701,hz3:5701").split(",")
CONFIG_SERVER = os.getenv("CONFIG_SERVER", "http://config-server:8000")
SERVICE_NAME = "logging-service"
ADVERTISED_URL = os.getenv("ADVERTISED_URL", "http://localhost:8000")

try:
    client = hazelcast.HazelcastClient(cluster_members=hz_nodes, cluster_name="dev")
    message_map = client.get_map("messages_map").blocking()
except Exception as e:
    print(f"Не вдалося підключитись до Hazelcast: {e}")
    message_map = None

async def register_with_config_server():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.post(
                    f"{CONFIG_SERVER}/register",
                    json={"service_name": SERVICE_NAME, "address": ADVERTISED_URL}
                )
                print(f"Успішно зареєстровано {ADVERTISED_URL} в config-server")
                break
            except httpx.RequestError:
                print("Очікування config-server...")
                await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(register_with_config_server())

class LogMessage(BaseModel):
    uuid: str
    msg: str

@app.post("/log")
async def log_message(data: LogMessage):
    if message_map:
        message_map.put(data.uuid, data.msg)
    print(f"Отримано повідомлення: {data.msg} з UUID: {data.uuid}")
    return {"status": "success"}

@app.get("/log", response_class=PlainTextResponse)
async def get_logs():
    if not message_map:
        return "Hazelcast недоступний"
    values = message_map.values()
    return ", ".join(values) if values else "Немає повідомлень"
