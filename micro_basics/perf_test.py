"""Performance & correctness test for the facade-service.

Scenarios from the assignment:
  A) 10 clients, 10K txns each, each client uses its own user_id (+1 per txn).
     Expected: every account ends at 10000.
  B) 10 clients, 10K txns each, all targeting one shared user_id (+1 per txn).
     Expected: that single account ends at 100000.

Each "client" is an asyncio task that owns its own httpx.AsyncClient — this gives
real connection-level concurrency (each task pipelines requests over its own pool),
which is closer to "10 separate clients" than 10 coroutines sharing one client.
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


async def _reset_timings(facade_url: str) -> None:
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.post(f"{facade_url}/timings/reset")
        r.raise_for_status()


async def _get_timings(facade_url: str) -> dict:
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(f"{facade_url}/timings")
        r.raise_for_status()
        return r.json()


async def _get_accounts(facade_url: str) -> dict:
    async with httpx.AsyncClient(timeout=10.0) as c:
        r = await c.get(f"{facade_url}/accounts")
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
    await _reset_timings(facade_url)

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
    timings = await _get_timings(facade_url)
    accounts = await _get_accounts(facade_url)

    rps = ok / wall if wall > 0 else 0.0
    print(f"  wall_time          = {wall:.2f} s")
    print(f"  successful txns    = {ok}")
    print(f"  failed txns        = {fail}")
    print(f"  throughput         = {rps:.1f} req/s")
    print(f"  logging cumulative = {timings['logging_total_seconds']:.2f} s "
          f"(avg {timings['logging_avg_ms']:.2f} ms over {timings['logging_calls']} calls)")
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


async def reset_state(facade_url: str) -> None:
    """Clear in-memory state by restarting nothing — instead the runner brings
    up a fresh stack between scenarios. Here we only reset timing counters."""
    await _reset_timings(facade_url)


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
