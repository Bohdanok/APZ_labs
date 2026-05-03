"""counter-service — per-user balances in PostgreSQL, fed by a Hazelcast Queue.

Replaces the previous "INSERT msg / SELECT COUNT(*)" stub. The Task 1 spec
that Lab 5 inherits requires balance tracking, so transactions are now
shaped {transaction_id, user_id, amount} (signed amount) and we maintain
an `accounts(user_id, balance)` table.

Atomicity under concurrency comes from a single SQL statement —
   INSERT ... ON CONFLICT (user_id) DO UPDATE SET balance = ... RETURNING balance
PostgreSQL's row-level lock on the UPDATE serialises concurrent writers to
the same account, so 10 parallel +1's on the same user_id end at the
correct balance with no lost updates.

Multiple worker threads consume from the Hazelcast Queue in parallel;
this is what lets a 100K-transaction perf run drain in seconds rather
than minutes.
"""
import json
import os
import socket
import threading
import time

import hazelcast
import psycopg2
import psycopg2.pool
from fastapi import FastAPI

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
NUM_WORKERS = int(os.environ.get("COUNTER_WORKERS", "8"))

DDL = """
CREATE TABLE IF NOT EXISTS accounts (
    user_id TEXT PRIMARY KEY,
    balance NUMERIC NOT NULL DEFAULT 0
);
"""

hz_client = None
counter_queue = None
db_pool: psycopg2.pool.ThreadedConnectionPool | None = None

# Per-pod stats, exposed via GET /stats so perf_test can detect queue drain.
stats_lock = threading.Lock()
stats = {
    "db_total_seconds": 0.0,
    "db_calls": 0,
    "db_errors": 0,
}


def init_db():
    global db_pool
    last_err = None
    for i in range(20):
        try:
            db_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=2, maxconn=NUM_WORKERS + 4,
                host=DB_HOST, port=DB_PORT, database=DB_NAME,
                user=DB_USER, password=DB_PASSWORD,
            )
            conn = db_pool.getconn()
            try:
                with conn.cursor() as cur:
                    cur.execute(DDL)
                conn.commit()
            finally:
                db_pool.putconn(conn)
            print(f"[counter:{INSTANCE_ID}] DB ready (pool size {NUM_WORKERS + 4})", flush=True)
            return
        except Exception as e:
            last_err = e
            print(f"[counter:{INSTANCE_ID}] DB init retry {i}: {e}", flush=True)
            time.sleep(3)
    raise RuntimeError(f"DB never came up: {last_err}")


def apply_txn(payload: dict) -> float | None:
    """Atomic upsert; returns the new balance or None on error."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO accounts (user_id, balance)
                VALUES (%s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
                RETURNING balance
                """,
                (payload["user_id"], payload["amount"]),
            )
            balance = cur.fetchone()[0]
        conn.commit()
        return float(balance)
    except Exception:
        conn.rollback()
        raise
    finally:
        db_pool.putconn(conn)


def queue_worker(worker_id: int):
    """Pull JSON from MQ; apply each transaction to PG."""
    while True:
        try:
            if counter_queue is None:
                time.sleep(0.5)
                continue
            raw = counter_queue.take()       # blocking
            try:
                payload = json.loads(raw)
            except Exception:
                with stats_lock:
                    stats["db_errors"] += 1
                continue

            t0 = time.perf_counter()
            try:
                balance = apply_txn(payload)
                dt = time.perf_counter() - t0
                with stats_lock:
                    stats["db_total_seconds"] += dt
                    stats["db_calls"] += 1
                if os.environ.get("VERBOSE_LOG", "0") == "1":
                    print(f"[counter:{INSTANCE_ID}:w{worker_id}] tid={payload['transaction_id']} "
                          f"user={payload['user_id']} {payload['amount']:+} -> {balance} "
                          f"({dt*1000:.1f}ms)", flush=True)
            except Exception as e:
                with stats_lock:
                    stats["db_errors"] += 1
                print(f"[counter:{INSTANCE_ID}:w{worker_id}] DB write failed: {e}", flush=True)

        except Exception as e:
            print(f"[counter:{INSTANCE_ID}:w{worker_id}] worker err: {e}", flush=True)
            time.sleep(0.5)


@app.on_event("startup")
def startup():
    global hz_client, counter_queue
    print(f"[counter:{INSTANCE_ID}] cfg HZ_NODES={HZ_NODES} cluster={HZ_CLUSTER_NAME} "
          f"queue={MQ_QUEUE_NAME} DB={DB_HOST}:{DB_PORT}/{DB_NAME} workers={NUM_WORKERS}",
          flush=True)
    init_db()
    for i in range(20):
        try:
            hz_client = hazelcast.HazelcastClient(
                cluster_members=HZ_NODES, cluster_name=HZ_CLUSTER_NAME
            )
            counter_queue = hz_client.get_queue(MQ_QUEUE_NAME).blocking()
            for w in range(NUM_WORKERS):
                threading.Thread(target=queue_worker, args=(w,), daemon=True).start()
            print(f"[counter:{INSTANCE_ID}] connected to MQ, {NUM_WORKERS} workers running",
                  flush=True)
            return
        except Exception as e:
            print(f"[counter:{INSTANCE_ID}] MQ retry {i}: {e}", flush=True)
            time.sleep(3)


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/balance/{user_id}")
def get_user_balance(user_id: str):
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT balance FROM accounts WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
    finally:
        db_pool.putconn(conn)
    return {"user_id": user_id, "balance": float(row[0]) if row else 0.0,
            "instance": INSTANCE_ID}


@app.get("/balance")
def get_all_balances():
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id, balance FROM accounts")
            rows = cur.fetchall()
    finally:
        db_pool.putconn(conn)
    return {"balances": {r[0]: float(r[1]) for r in rows}, "instance": INSTANCE_ID}


@app.post("/balance/reset")
def reset_balances():
    """Wipes all balances — used between perf scenarios."""
    conn = db_pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE accounts")
        conn.commit()
    finally:
        db_pool.putconn(conn)
    with stats_lock:
        stats["db_total_seconds"] = 0.0
        stats["db_calls"] = 0
        stats["db_errors"] = 0
    return {"status": "ok", "instance": INSTANCE_ID}


@app.get("/stats")
def get_stats():
    with stats_lock:
        s = dict(stats)
    queue_size = -1
    try:
        if counter_queue is not None:
            queue_size = counter_queue.size()
    except Exception:
        pass
    return {
        "instance": INSTANCE_ID,
        "db_total_seconds": s["db_total_seconds"],
        "db_calls": s["db_calls"],
        "db_errors": s["db_errors"],
        "db_avg_ms": (s["db_total_seconds"] / s["db_calls"] * 1000) if s["db_calls"] else 0.0,
        "queue_size": queue_size,
        "workers": NUM_WORKERS,
    }
