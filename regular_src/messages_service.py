from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()

@app.get("/message", response_class=PlainTextResponse)
async def get_message():
    message =  "Hi from Bohdan!"
    print(message)

    return message