from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse

app = FastAPI()

message_map = {}

class LogMessage(BaseModel):
    uuid: str
    msg: str

@app.post("/log")
async def log_message(data: LogMessage):
    message_map[data.uuid] = data.msg
    print(f"Отримано повідомлення: {data.msg} з UUID: {data.uuid}")
    return {"status": "success"}

@app.get("/log", response_class=PlainTextResponse)
async def get_logs():
    return ", ".join(message_map.values())
