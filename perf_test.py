"""
Performance test for Lab 5.

Two scenarios from Lab 1:
  - 10 accounts (10 concurrent clients)
  -  1 account  (1 sequential client)

For each scenario we send N=100 messages, then read back via GET /facade.
Per-request timing is reported by the facade itself (log_ms / mq_ms),
so we can split the total time into:
  - logging-service contribution    (sync HTTP POST to logging-service)
  - counter-service / MQ contribution (Hazelcast queue.put on facade side)

Run:
    python perf_test.py
"""
import asyncio
import time
import statistics
import httpx

import os

# FACADE_URL can override; default points to minikube NodePort
BASE = os.environ.get("FACADE_URL", "http://192.168.49.2:30080")
URL_POST = f"{BASE}/facade"
URL_GET = f"{BASE}/facade"

NUM_REQUESTS = 100


async def send_one(client, i):
    return await client.post(URL_POST, json={"msg": f"perf_msg_{i}"}, timeout=15.0)


async def run_concurrent(num_clients: int, total_requests: int):
    """num_clients run in parallel, splitting total_requests evenly."""
    results = []
    t_start = time.perf_counter()

    async def worker(idx, share):
        async with httpx.AsyncClient() as c:
            for i in range(share):
                r = await send_one(c, idx * share + i)
                if r.status_code == 200:
                    results.append(r.json())

    share = total_requests // num_clients
    workers = [worker(k, share) for k in range(num_clients)]
    await asyncio.gather(*workers)

    total = time.perf_counter() - t_start
    log_times = [r["log_ms"] for r in results]
    mq_times = [r["mq_ms"] for r in results]
    return {
        "scenario": f"{num_clients} client(s) x {share} req",
        "total_time_s": round(total, 3),
        "throughput_rps": round(len(results) / total, 2) if total > 0 else 0,
        "successful": len(results),
        "logging_avg_ms": round(statistics.mean(log_times), 2) if log_times else 0,
        "logging_p95_ms": round(statistics.quantiles(log_times, n=20)[-1], 2) if len(log_times) >= 20 else 0,
        "mq_avg_ms": round(statistics.mean(mq_times), 2) if mq_times else 0,
        "mq_p95_ms": round(statistics.quantiles(mq_times, n=20)[-1], 2) if len(mq_times) >= 20 else 0,
    }


def fmt(d):
    return (
        f"  total_time         = {d['total_time_s']} s\n"
        f"  successful         = {d['successful']}\n"
        f"  throughput         = {d['throughput_rps']} req/s\n"
        f"  logging avg / p95  = {d['logging_avg_ms']} / {d['logging_p95_ms']} ms\n"
        f"  counter  avg / p95 = {d['mq_avg_ms']} / {d['mq_p95_ms']} ms"
    )


async def main():
    print(f"=== Lab 5 perf test, NUM_REQUESTS = {NUM_REQUESTS} ===\n")

    print(">>> 1 account scenario")
    one = await run_concurrent(num_clients=1, total_requests=NUM_REQUESTS)
    print(fmt(one), "\n")

    print(">>> 10 accounts scenario")
    ten = await run_concurrent(num_clients=10, total_requests=NUM_REQUESTS)
    print(fmt(ten), "\n")

    async with httpx.AsyncClient() as c:
        r = await c.get(URL_GET, timeout=10.0)
        print(">>> GET /facade response")
        print(r.text)


if __name__ == "__main__":
    asyncio.run(main())
