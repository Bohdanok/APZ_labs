from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
import psycopg2
import os

app = FastAPI()

class MessageData(BaseModel):
    msg: str

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        database=os.getenv("DB_NAME", "messages_db"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PASS", "password")
    )

try:
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS messages (
                    id SERIAL PRIMARY KEY,
                    msg TEXT NOT NULL
                )
            """)
            conn.commit()
except Exception as e:
    print(f"Помилка ініціалізації БД: {e}")

@app.post("/message")
async def post_message(data: MessageData):
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO messages (msg) VALUES (%s)", (data.msg,))
                conn.commit()
    except Exception as e:
        print(f"Помилка запису в БД: {e}")
    return {"status": "saved"}

@app.get("/message", response_class=PlainTextResponse)
async def get_message():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM messages")
                count = cur.fetchone()[0]
                return f"Hi from Bohdan! Total messages in DB: {count}"
    except Exception as e:
        return "Hi from Bohdan! (DB connection error)"