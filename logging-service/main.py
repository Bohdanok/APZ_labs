from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
import hazelcast
import os

app = FastAPI()

hz_nodes = os.getenv("HZ_NODES", "localhost:5701").split(",")
try:
    client = hazelcast.HazelcastClient(
        cluster_members=hz_nodes,
        cluster_name="dev"
    )
    message_map = client.get_map("messages_map").blocking()
except Exception as e:
    print(f"Не вдалося підключитись до Hazelcast: {e}")
    message_map = None

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