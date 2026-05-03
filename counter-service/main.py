import os
import socket
import time
import json
import threading
import psycopg2
import hazelcast
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()

INSTANCE_ID = os.environ.get("HOSTNAME", socket.gethostname())

HZ_NODES = [n.strip() for n in os.environ["HZ_NODES"].split(",")]
HZ_CLUSTER_NAME = os.environ["HZ_CLUSTER_NAME"]
MQ_QUEUE_NAME = os.environ["MQ_QUEUE_NAME"]
DB_HOST = os.environ["DB_HOST"]
DB_PORT = int(os.environ.get("DB_PORT", "5432"))
DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]
DB_PASSWORD = os.environ["DB_PASSWORD"]

hz_client = None
counter_queue = None


def get_db_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, database=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
    )


def init_db():
    for i in range(20):
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "CREATE TABLE IF NOT EXISTS messages "
                        "(id SERIAL PRIMARY KEY, msg TEXT NOT NULL)"
                    )
                    conn.commit()
            print(f"[counter:{INSTANCE_ID}] DB ready", flush=True)
            return
        except Exception as e:
            print(f"[counter:{INSTANCE_ID}] DB init retry {i}: {e}", flush=True)
            time.sleep(3)


def queue_listener():
    print(f"[counter:{INSTANCE_ID}] queue listener started", flush=True)
    while True:
        try:
            if counter_queue is None:
                time.sleep(1)
                continue
            raw = counter_queue.take()
            parsed = json.loads(raw)
            with get_db_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("INSERT INTO messages (msg) VALUES (%s)", (parsed["msg"],))
                    conn.commit()
            print(f"[counter:{INSTANCE_ID}] persisted msg='{parsed['msg']}'", flush=True)
        except Exception as e:
            print(f"[counter:{INSTANCE_ID}] queue err: {e}", flush=True)
            time.sleep(1)


@app.on_event("startup")
def startup():
    global hz_client, counter_queue
    print(f"[counter:{INSTANCE_ID}] config -> HZ_NODES={HZ_NODES} cluster={HZ_CLUSTER_NAME} "
          f"queue={MQ_QUEUE_NAME} DB={DB_HOST}:{DB_PORT}/{DB_NAME}", flush=True)
    init_db()
    for i in range(20):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES, cluster_name=HZ_CLUSTER_NAME
            )
            counter_queue = hz_client.get_queue(MQ_QUEUE_NAME).blocking()
            threading.Thread(target=queue_listener, daemon=True).start()
            print(f"[counter:{INSTANCE_ID}] connected to MQ", flush=True)
            return
        except Exception as e:
            print(f"[counter:{INSTANCE_ID}] MQ retry {i}: {e}", flush=True)
            time.sleep(3)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/message", response_class=PlainTextResponse)
async def get_message():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM messages")
                cnt = cur.fetchone()[0]
                return f"Total messages in DB: {cnt} (served by {INSTANCE_ID})"
    except Exception as e:
        return f"DB connection error: {e}"
