"""Task-1 performance test, ported to the Lab 5 K8s stack.

Two scenarios mandated by Task 1 (Lab 5 spec says to repeat the same
test and compare against Labs 1 and 3):

  A) 10 clients, 10K txns each, each on its own user_id (+1 per txn)
     Expected: every account ends at 10000.
  B) 10 clients, 10K txns each, all on a shared user_id (+1 per txn)
     Expected: that single account ends at 100000.

Lab 5 quirk: counter-service is fed by a Hazelcast Queue (MQ), so the
facade returns *before* the DB write actually completes. After all POSTs
finish we therefore wait for the queue to drain (poll counter /stats
across all counter pods until total db_calls == expected) before reading
final balances. Wall time is reported two ways:
  - "POST wall"  — until last facade response
  - "drain wall" — until counter has persisted everything

Each "client" is an asyncio Task with its own httpx.AsyncClient (its own
pool) — closer to "10 separate clients" than 10 coroutines on one client.

Facade /timings is per-pod, so we aggregate across all facade pods via
`kubectl exec ... curl /timings` (kubectl access required).

Usage:
  # default — assumes `kubectl port-forward svc/facade-service 8000:8000`
  python3 perf_test.py --scenario A
  python3 perf_test.py --scenario B
"""
from __future__ import annotations

import argparse
import asyncio
import json
import subprocess
import sys
import time

import httpx

NAMESPACE = "apz-lab5"


# ---------- pod-aggregate helpers (use kubectl) ---------- #

def _list_pods(label: str) -> list[str]:
    out = subprocess.check_output([
        "kubectl", "get", "pods", "-n", NAMESPACE, "-l", label,
        "-o", "jsonpath={.items[*].metadata.name}",
    ])
    return out.decode().split()


def _curl_in_pod(pod: str, path: str) -> dict:
    out = subprocess.check_output([
        "kubectl", "exec", "-n", NAMESPACE, pod, "--",
        "curl", "-s", f"http://localhost:8000{path}",
    ])
    return json.loads(out)


def aggregate_facade_timings() -> dict:
    pods = _list_pods("app=facade-service")
    agg = {"logging_total_seconds": 0.0, "logging_calls": 0,
           "mq_total_seconds": 0.0, "mq_calls": 0, "pods": []}
    for p in pods:
        try:
            d = _curl_in_pod(p, "/timings")
        except Exception as e:
            agg["pods"].append({"pod": p, "error": str(e)})
            continue
        agg["logging_total_seconds"] += d["logging_total_seconds"]
        agg["logging_calls"] += d["logging_calls"]
        agg["mq_total_seconds"] += d["mq_total_seconds"]
        agg["mq_calls"] += d["mq_calls"]
        agg["pods"].append({"pod": p, "calls": d["logging_calls"]})
    agg["logging_avg_ms"] = (
        agg["logging_total_seconds"] / agg["logging_calls"] * 1000
        if agg["logging_calls"] else 0.0
    )
    agg["mq_avg_ms"] = (
        agg["mq_total_seconds"] / agg["mq_calls"] * 1000
        if agg["mq_calls"] else 0.0
    )
    return agg


def aggregate_counter_stats() -> dict:
    pods = _list_pods("app=counter-service")
    agg = {"db_total_seconds": 0.0, "db_calls": 0, "db_errors": 0,
           "queue_size_max": 0, "pods": []}
    for p in pods:
        try:
            d = _curl_in_pod(p, "/stats")
        except Exception as e:
            agg["pods"].append({"pod": p, "error": str(e)})
            continue
        agg["db_total_seconds"] += d["db_total_seconds"]
        agg["db_calls"] += d["db_calls"]
        agg["db_errors"] += d["db_errors"]
        if d["queue_size"] > agg["queue_size_max"]:
            agg["queue_size_max"] = d["queue_size"]
        agg["pods"].append({"pod": p, "calls": d["db_calls"],
                            "queue_size": d["queue_size"]})
    agg["db_avg_ms"] = (
        agg["db_total_seconds"] / agg["db_calls"] * 1000
        if agg["db_calls"] else 0.0
    )
    return agg


def reset_facade_timings():
    for p in _list_pods("app=facade-service"):
        try:
            subprocess.check_output([
                "kubectl", "exec", "-n", NAMESPACE, p, "--",
                "curl", "-s", "-X", "POST",
                "http://localhost:8000/timings/reset",
            ])
        except Exception as e:
            print(f"  warn: timing reset on {p}: {e}", flush=True)


# ---------- workload ---------- #

async def _client_loop(client_idx, user_id, txns_per_client, facade_url, progress_every):
    ok = 0
    fail = 0
    limits = httpx.Limits(max_connections=64, max_keepalive_connections=64)
    async with httpx.AsyncClient(timeout=30.0, limits=limits) as client:
        for i in range(txns_per_client):
            try:
                r = await client.post(
                    f"{facade_url}/transaction",
                    json={"user_id": user_id, "amount": 1},
                )
                if r.status_code == 200:
                    ok += 1
                else:
                    fail += 1
            except Exception:
                fail += 1
            if progress_every and (i + 1) % progress_every == 0:
                print(f"  client {client_idx} ({user_id}): "
                      f"{i + 1}/{txns_per_client}", flush=True)
    return ok, fail


async def _post(facade_url, path):
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.post(f"{facade_url}{path}")
        r.raise_for_status()
        return r.json()


async def _get(facade_url, path):
    async with httpx.AsyncClient(timeout=30.0) as c:
        r = await c.get(f"{facade_url}{path}")
        r.raise_for_status()
        return r.json()


async def wait_for_drain(expected: int, poll_interval: float = 1.0,
                        timeout: float = 1800.0) -> tuple[float, dict]:
    """Poll counter /stats until total db_calls reaches expected."""
    print(f"  waiting for queue to drain ({expected} writes expected)…", flush=True)
    t0 = time.perf_counter()
    last_calls = -1
    last_progress_t = t0
    while True:
        cs = aggregate_counter_stats()
        calls = cs["db_calls"]
        elapsed = time.perf_counter() - t0
        if calls >= expected:
            return elapsed, cs
        if calls != last_calls:
            rate = (calls - max(last_calls, 0)) / max(0.001, time.perf_counter() - last_progress_t)
            print(f"    drained {calls}/{expected} ({rate:.0f} writes/s, "
                  f"queue_size={cs['queue_size_max']})", flush=True)
            last_calls = calls
            last_progress_t = time.perf_counter()
        if elapsed > timeout:
            raise TimeoutError(f"queue drain timed out at {calls}/{expected}")
        await asyncio.sleep(poll_interval)


async def run_scenario(name, user_ids, n_clients, txns_per_client, facade_url):
    expected_total = n_clients * txns_per_client
    print(f"\n=== Scenario {name} ===")
    print(f"  clients={n_clients}, txns/client={txns_per_client}, total={expected_total}")
    await _post(facade_url, "/admin/reset")
    reset_facade_timings()
    # Wait for resets to propagate (counter pod queue listeners need a moment).
    await asyncio.sleep(1.0)

    progress_every = max(1, txns_per_client // 4)
    t0 = time.perf_counter()
    results = await asyncio.gather(*[
        _client_loop(i, user_ids[i], txns_per_client, facade_url, progress_every)
        for i in range(n_clients)
    ])
    post_wall = time.perf_counter() - t0
    ok = sum(r[0] for r in results)
    fail = sum(r[1] for r in results)
    print(f"  POST phase done: wall={post_wall:.2f}s ok={ok} fail={fail}")

    drain_wall, cs = await wait_for_drain(ok)

    facade = aggregate_facade_timings()
    accounts = await _get(facade_url, "/accounts")

    total_wall = post_wall + drain_wall
    rps_post = ok / post_wall if post_wall > 0 else 0.0
    rps_total = ok / total_wall if total_wall > 0 else 0.0

    print(f"  POST wall          = {post_wall:.2f} s ({rps_post:.1f} req/s)")
    print(f"  drain wall         = {drain_wall:.2f} s")
    print(f"  total wall         = {total_wall:.2f} s ({rps_total:.1f} req/s)")
    print(f"  successful txns    = {ok}")
    print(f"  failed txns        = {fail}")
    print(f"  --- facade-side timing (logging is sync, MQ enqueue is sync, counter DB write is async) ---")
    print(f"  logging cumulative = {facade['logging_total_seconds']:.2f} s "
          f"(avg {facade['logging_avg_ms']:.2f} ms over {facade['logging_calls']} calls)")
    print(f"  MQ enqueue cum.    = {facade['mq_total_seconds']:.2f} s "
          f"(avg {facade['mq_avg_ms']:.2f} ms over {facade['mq_calls']} calls)")
    print(f"  --- counter-side timing (actual DB writes, async via MQ) ---")
    print(f"  counter DB cum.    = {cs['db_total_seconds']:.2f} s "
          f"(avg {cs['db_avg_ms']:.2f} ms over {cs['db_calls']} writes, errors={cs['db_errors']})")
    print(f"  facade pods        = {[p.get('pod') for p in facade['pods']]}")
    print(f"  counter pods       = {[(p.get('pod'), p.get('calls')) for p in cs['pods']]}")
    print(f"  final balances     = {accounts['balances']}")

    return {
        "scenario": name,
        "post_wall": post_wall,
        "drain_wall": drain_wall,
        "total_wall": total_wall,
        "rps_post": rps_post,
        "rps_total": rps_total,
        "ok": ok,
        "fail": fail,
        "facade": facade,
        "counter": cs,
        "balances": accounts["balances"],
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--facade", default="http://localhost:8000",
                        help="reachable facade URL (default: http://localhost:8000 "
                             "— assumes `kubectl port-forward svc/facade-service 8000:8000`)")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--txns", type=int, default=10000)
    parser.add_argument("--scenario", choices=["A", "B", "both"], default="both")
    parser.add_argument("--prefix", default="run")
    args = parser.parse_args()

    if args.scenario in ("A", "both"):
        users_a = [f"{args.prefix}A-user{i}" for i in range(args.clients)]
        await run_scenario("A: distinct accounts", users_a,
                           args.clients, args.txns, args.facade)

    if args.scenario in ("B", "both"):
        shared = f"{args.prefix}B-shared"
        users_b = [shared] * args.clients
        await run_scenario("B: shared account", users_b,
                           args.clients, args.txns, args.facade)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
