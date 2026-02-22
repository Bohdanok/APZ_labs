from fastapi import FastAPI, Body
from fastapi.responses import PlainTextResponse
import uuid
import httpx
import grpc
import logging_pb2
import logging_pb2_grpc
from tenacity import retry, stop_after_attempt, wait_fixed

app = FastAPI()
MESSAGES_URL = "http://localhost:8002/message"

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
def send_grpc_log(msg_uuid: str, msg: str):
    print("Спроба відправки через gRPC...")
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = logging_pb2_grpc.LoggingServiceStub(channel)
        response = stub.LogMessage(logging_pb2.LogRequest(uuid=msg_uuid, msg=msg))
        return response.status

def get_grpc_logs():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = logging_pb2_grpc.LoggingServiceStub(channel)
        response = stub.GetMessages(logging_pb2.EmptyRequest())
        return response.messages

@app.post("/facade")
async def post_facade(msg: str = Body(..., embed=True)):
    msg_uuid = str(uuid.uuid4())
    try:
        send_grpc_log(msg_uuid, msg)
    except Exception as e:
        return {"status": "error", "message": "Logging service is unavailable"}
    
    return {"status": "ok", "uuid": msg_uuid}

@app.get("/facade", response_class=PlainTextResponse)
async def get_facade():
    try:
        log_text = get_grpc_logs()
    except Exception:
        log_text = "error_fetching_logs"
        
    async with httpx.AsyncClient() as client:
        try:
            msg_response = await client.get(MESSAGES_URL)
            msg_text = msg_response.text
        except Exception:
            msg_text = "error_fetching_message"
            
    return f"{log_text}: {msg_text}"
