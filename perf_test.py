"""Task-1 performance test, ported to the Lab 3 stack.

Two scenarios mandated by Task 1 (Lab 3 spec says to repeat the same test
and compare against Lab 1 numbers):

  A) 10 clients, 10K txns each, each client uses its own user_id (+1 per txn).
     Expected: every account ends at 10000.
  B) 10 clients, 10K txns each, all targeting one shared user_id (+1 per txn).
     Expected: that single account ends at 100000.

Each "client" is an asyncio task with its own httpx.AsyncClient (own
connection pool) — closer to "10 separate clients" than 10 coroutines
sharing one client. Reads /timings to attribute wall-time across logging
vs counter, and /accounts to verify final balances.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
import time

import httpx


async def _client_loop(
    client_idx: int,
    user_id: str,
    txns_per_client: int,
    facade_url: str,
    progress_every: int = 0,
) -> tuple[int, int]:
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
                print(f"  client {client_idx} ({user_id}): {i + 1}/{txns_per_client}", flush=True)
    return ok, fail


async def _post(facade_url: str, path: str) -> dict:
    async with httpx.AsyncClient(timeout=20.0) as c:
        r = await c.post(f"{facade_url}{path}")
        r.raise_for_status()
        return r.json()


async def _get(facade_url: str, path: str) -> dict:
    async with httpx.AsyncClient(timeout=20.0) as c:
        r = await c.get(f"{facade_url}{path}")
        r.raise_for_status()
        return r.json()


async def run_scenario(
    name: str,
    user_ids: list[str],
    n_clients: int,
    txns_per_client: int,
    facade_url: str,
) -> dict:
    assert len(user_ids) == n_clients
    print(f"\n=== Scenario {name} ===")
    print(f"  clients={n_clients}, txns/client={txns_per_client}, total={n_clients * txns_per_client}")
    await _post(facade_url, "/admin/reset")
    await _post(facade_url, "/timings/reset")

    progress_every = max(1, txns_per_client // 4)
    t0 = time.perf_counter()
    results = await asyncio.gather(
        *[
            _client_loop(i, user_ids[i], txns_per_client, facade_url, progress_every)
            for i in range(n_clients)
        ]
    )
    wall = time.perf_counter() - t0

    ok = sum(r[0] for r in results)
    fail = sum(r[1] for r in results)
    timings = await _get(facade_url, "/timings")
    accounts = await _get(facade_url, "/accounts")

    rps = ok / wall if wall > 0 else 0.0
    print(f"  wall_time          = {wall:.2f} s")
    print(f"  successful txns    = {ok}")
    print(f"  failed txns        = {fail}")
    print(f"  throughput         = {rps:.1f} req/s")
    print(f"  logging cumulative = {timings['logging_total_seconds']:.2f} s "
          f"(avg {timings['logging_avg_ms']:.2f} ms over {timings['logging_calls']} calls, "
          f"failovers={timings['logging_failovers']})")
    print(f"  counter cumulative = {timings['counter_total_seconds']:.2f} s "
          f"(avg {timings['counter_avg_ms']:.2f} ms over {timings['counter_calls']} calls)")
    print(f"  final balances     = {accounts['balances']}")

    return {
        "scenario": name,
        "wall_time": wall,
        "ok": ok,
        "fail": fail,
        "rps": rps,
        "timings": timings,
        "balances": accounts["balances"],
    }


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--facade", default="http://localhost:8000")
    parser.add_argument("--clients", type=int, default=10)
    parser.add_argument("--txns", type=int, default=10000)
    parser.add_argument("--scenario", choices=["A", "B", "both"], default="both")
    parser.add_argument("--prefix", default="run", help="user_id prefix to namespace this run")
    args = parser.parse_args()

    if args.scenario in ("A", "both"):
        users_a = [f"{args.prefix}A-user{i}" for i in range(args.clients)]
        await run_scenario("A: distinct accounts", users_a, args.clients, args.txns, args.facade)

    if args.scenario in ("B", "both"):
        shared = f"{args.prefix}B-shared"
        users_b = [shared] * args.clients
        await run_scenario("B: shared account", users_b, args.clients, args.txns, args.facade)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
