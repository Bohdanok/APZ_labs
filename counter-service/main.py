from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import PlainTextResponse
import psycopg2
import os
import hazelcast
import httpx
import asyncio
import threading
import json

app = FastAPI()

CONFIG_SERVER = os.getenv("CONFIG_SERVER", "http://config-server:8000")
SERVICE_NAME = "counter-service"
ADVERTISED_URL = os.getenv("ADVERTISED_URL", "http://localhost:8000")
hz_nodes = os.getenv("HZ_NODES", "hz1:5701,hz2:5701,hz3:5701").split(",")

hz_client = None
counter_queue = None

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        database=os.getenv("DB_NAME", "messages_db"),
        user=os.getenv("DB_USER", "user"),
        password=os.getenv("DB_PASS", "password")
    )

import time

def init_db():
    max_retries = 5
    for i in range(max_retries):
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
            print("Таблицю messages успішно ініціалізовано.")
            return # Exit the loop if successful
        except Exception as e:
            print(f"Помилка ініціалізації БД (спроба {i+1}/{max_retries}): {e}")
            time.sleep(3) # Wait 3 seconds before trying again

def queue_listener():
    print("Запуск слухача черги Hazelcast...")
    while True:
        try:
            if counter_queue:
                msg = counter_queue.take()
                parsed_msg = json.loads(msg)
                with get_db_connection() as conn:
                    with conn.cursor() as cur:
                        cur.execute("INSERT INTO messages (msg) VALUES (%s)", (parsed_msg['msg'],))
                        conn.commit()
                print(f"Збережено з черги: {parsed_msg['msg']}")
        except Exception as e:
            print(f"Помилка обробки повідомлення з черги: {e}")

async def register_with_config_server():
    async with httpx.AsyncClient() as client:
        while True:
            try:
                await client.post(
                    f"{CONFIG_SERVER}/register",
                    json={"service_name": SERVICE_NAME, "address": ADVERTISED_URL}
                )
                print("Успішно зареєстровано в config-server")
                break
            except httpx.RequestError:
                await asyncio.sleep(3)

@app.on_event("startup")
async def startup_event():
    global hz_client, counter_queue
    init_db()
    asyncio.create_task(register_with_config_server())
    try:
        hz_client = hazelcast.HazelcastClient(cluster_members=hz_nodes, cluster_name="dev")
        counter_queue = hz_client.get_queue("messages_queue").blocking()
        threading.Thread(target=queue_listener, daemon=True).start()
    except Exception as e:
        print(f"Hazelcast connection error: {e}")

@app.get("/message", response_class=PlainTextResponse)
async def get_message():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM messages")
                count = cur.fetchone()[0]
                return f"Total messages in DB: {count}"
    except Exception as e:
        return "DB connection error"
    