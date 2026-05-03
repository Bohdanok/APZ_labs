# How to run and where to take screenshots — Lab 5

Same template as Labs 1 / 3 — copy-paste each block, snap the screenshot
at the "📸" cue. Long perf runs are flagged.

> Working directory throughout:
> `/home/julfy/Documents/6th_term/APZ/Task_5_micro_consul/APZ_labs`

> **Prereq.** Docker Desktop with Kubernetes enabled (or `minikube`).
> `kubectl get nodes` must show at least one Ready node.

---

## 0. One-time setup

### 0.1 Build the three images into the K8s-visible Docker daemon

```bash
cd /home/julfy/Documents/6th_term/APZ/Task_5_micro_consul/APZ_labs
docker build -t apz-lab5/logging-service:latest -f logging-service/Dockerfile .
docker build -t apz-lab5/counter-service:latest -f counter-service/Dockerfile .
docker build -t apz-lab5/facade-service:latest  -f facade-service/Dockerfile  .
```

### 0.2 Apply manifests

```bash
kubectl apply -f k8s/
kubectl get pods -n apz-lab5 -w
```

Wait until **all pods show `READY 1/1` and `STATUS Running`**, then
Ctrl+C the watch.

### 0.3 Make facade reachable from the host

In a **dedicated terminal that stays open** for the whole session:

```bash
kubectl port-forward -n apz-lab5 svc/facade-service 8000:8000
```

If port 8000 is already taken (e.g. Lab 1/3 still running):
```bash
docker ps --format 'table {{.Names}}\t{{.Ports}}' | grep ':8000'
# stop whatever it is, or use a different port:
kubectl port-forward -n apz-lab5 svc/facade-service 8001:8000
# and then export FACADE_URL=http://localhost:8001 before running curl/perf_test
```

---

## 📸 Screenshot 1 — Pods are up (proves "три екземпляра кожного сервісу")

```bash
kubectl get pods -n apz-lab5
```

→ **Take screenshot.** Frame: lines for —
- `facade-service-...` × 2 (READY 1/1)
- `logging-service-...` × 3 (READY 1/1)
- `counter-service-...` × 2 (READY 1/1)
- `hazelcast-{0,1,2}` (READY 1/1)
- `postgres-0` (READY 1/1)

This satisfies the assignment requirement that each service can run
in multiple instances.

---

## 📸 Screenshot 2 — Service Discovery (req. 2): no hardcoded IPs

```bash
kubectl get svc -n apz-lab5
echo "----"
kubectl get endpoints -n apz-lab5
```

→ **Take screenshot.** The `Service` table shows ClusterIPs (these
resolve from the ConfigMap-stored DNS names); the `Endpoints` table
shows the actual pod IPs that kube-proxy round-robins between. Proves
the IPs are dynamic and discovered, not coded.

---

## 📸 Screenshot 3 — ConfigMap (reqs. 3 & 4): Hazelcast and MQ config

```bash
kubectl get configmap app-config -n apz-lab5 -o yaml
echo "----"
kubectl get secret db-secret -n apz-lab5 -o yaml
```

→ **Take screenshot.** Shows the `data:` block with `HZ_NODES`,
`HZ_CLUSTER_NAME`, `HZ_MAP_NAME`, `MQ_QUEUE_NAME`, `LOGGING_SERVICE_URL`,
`COUNTER_SERVICE_URL`, `DB_HOST`, `DB_NAME`. This is the Consul-KV
equivalent. Secret holds DB credentials.

---

## 📸 Screenshot 4 — Sanity transactions (counter actually computes balance now)

```bash
echo "=== POST alice +100 ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"alice","amount":100}'  ; echo
echo "=== POST alice  -30 ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"alice","amount":-30}'  ; echo
echo "=== POST bob   +250 ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"bob","amount":250}'    ; echo
echo "=== POST bob    -50 ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"bob","amount":-50}'    ; echo
echo "=== POST carol +500 ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"carol","amount":500}'  ; echo
sleep 2  # let the MQ drain
echo "=== GET /accounts ===" ; curl -s http://localhost:8000/accounts | python3 -m json.tool
echo "=== GET /user/alice ===" ; curl -s http://localhost:8000/user/alice | python3 -m json.tool
echo "=== GET /user/bob ===" ; curl -s http://localhost:8000/user/bob | python3 -m json.tool
echo "=== GET /timings ===" ; curl -s http://localhost:8000/timings | python3 -m json.tool
```

→ **Take screenshot.** Frame: the curl outputs. Critical proof points:
- POST returns `{"transaction_id":"...","status":"accepted","facade_instance":"facade-service-...","logging_pod":"logging-service-..."}`.
- `/accounts` shows `{"alice": 70, "bob": 200, "carol": 500}` — these are **balances** (running totals), not message counts.
- `/user/alice` shows the transactions list (from `logging-service`) AND the balance (from `counter-service`).
- `/timings` shows non-zero `logging_total_seconds`, `mq_total_seconds`.

---

## 📸 Screenshot 5 — Three logging-service pods each got traffic

Open **three side-by-side terminal panes**:

```bash
kubectl logs -n apz-lab5 -l app=logging-service --tail=30 --prefix
```

Or one pane per pod (more readable):

```bash
kubectl get pods -n apz-lab5 -l app=logging-service -o name
# then for each name:
kubectl logs -n apz-lab5 <pod-name> --tail=30
```

For more visible traffic before the screenshot:
```bash
for i in {1..15}; do
  curl -s -X POST http://localhost:8000/transaction \
       -H 'Content-Type: application/json' \
       -d "{\"user_id\":\"loadtest-$i\",\"amount\":1}" \
       | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('logging_pod'))"
done
```

→ **Take screenshot.** Each pod's logs should include matching POST
`/log` access lines, and the curl loop will print the `logging_pod`
that handled each request — should round-robin across all three.

---

## 📸 Screenshot 6 — counter-service is doing PG writes (per-pod stats)

```bash
for p in $(kubectl get pods -n apz-lab5 -l app=counter-service -o jsonpath='{.items[*].metadata.name}'); do
  echo "=== $p ==="
  kubectl exec -n apz-lab5 "$p" -- curl -s http://localhost:8000/stats | python3 -m json.tool
done
echo "=== accounts table ==="
kubectl exec -n apz-lab5 postgres-0 -- psql -U user -d messages_db \
        -c "SELECT user_id, balance FROM accounts ORDER BY user_id;"
```

→ **Take screenshot.** Top half: `/stats` from each counter pod
showing `db_calls`, `db_avg_ms`, `queue_size`. Bottom half: actual rows
in PostgreSQL — concrete proof that counter persists balances per user.

---

## 📸 Screenshot 7 — Failover: kill one logging-service pod

```bash
echo "=== before ==="; kubectl get pods -n apz-lab5 -l app=logging-service
LSPOD=$(kubectl get pods -n apz-lab5 -l app=logging-service -o jsonpath='{.items[0].metadata.name}')
echo "=== killing $LSPOD ==="; kubectl delete pod -n apz-lab5 "$LSPOD" --grace-period=0 --force
sleep 2
echo "=== POST during outage ==="; curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d '{"user_id":"failover-ls","amount":42}'; echo
sleep 1
echo "=== GET ==="; curl -s http://localhost:8000/user/failover-ls | python3 -m json.tool
echo "=== after ==="; kubectl get pods -n apz-lab5 -l app=logging-service
```

→ **Take screenshot.** Frame:
- `before`: 3 pods Running.
- delete output.
- POST returns 200 (kube-proxy routed to a surviving pod, OR the new pod started so fast it caught the request).
- `after`: a new pod with a different suffix, all 3 Running again.

---

## 📸 Screenshot 8 — Failover: kill one counter-service pod

```bash
echo "=== before ==="; kubectl get pods -n apz-lab5 -l app=counter-service
CSPOD=$(kubectl get pods -n apz-lab5 -l app=counter-service -o jsonpath='{.items[0].metadata.name}')
echo "=== killing $CSPOD ==="; kubectl delete pod -n apz-lab5 "$CSPOD" --grace-period=0 --force
sleep 2
for i in {1..5}; do
  curl -s -X POST http://localhost:8000/transaction -H 'Content-Type: application/json' -d "{\"user_id\":\"failover-cs\",\"amount\":1}"; echo
done
sleep 3
echo "=== balance ==="; curl -s http://localhost:8000/user/failover-cs | python3 -m json.tool
echo "=== after ==="; kubectl get pods -n apz-lab5 -l app=counter-service
```

→ **Take screenshot.** Counter is fed via MQ — the surviving pod just
keeps consuming from the queue, so all 5 transactions are processed
even though one pod was killed mid-test. Final balance should be 5.

---

## ⚠️ Long-running steps — perf scenarios A and B

Each scenario is a few minutes of POSTs + a queue drain that depends on
DB write speed (8 workers × 2 pods → ~8K writes/s on this hardware → drain
in ~12-15s for 100K txns).

### 📸 Screenshot 9 — Scenario A (10 distinct accounts → 10 000 each)

```bash
python3 perf_test.py --scenario A --clients 10 --txns 10000
```

→ **Take screenshot once finished.** Frame: the final block —
- `POST wall = ... s`
- `drain wall = ... s`
- `total wall = ... s`
- `successful txns = 100000`, `failed = 0`
- `logging cumulative ... avg ___ ms`
- `MQ enqueue cum. ... avg ___ ms`
- `counter DB cum. ... avg ___ ms`
- `final balances = {... user0..user9 → 10000.0}`

### 📸 Screenshot 10 — Scenario B (1 shared account → 100 000)

```bash
python3 perf_test.py --scenario B --clients 10 --txns 10000
```

→ **Take screenshot once finished.** Same fields, but
`final balances = {'runB-shared': 100000.0}` — proves no lost updates
even though MQ feeds 16 PG-writers all hammering the same row.

### 📸 Screenshot 11 — Per-backend timing breakdown

After Scenario B, run:

```bash
echo "=== facade aggregate timings ==="
for p in $(kubectl get pods -n apz-lab5 -l app=facade-service -o jsonpath='{.items[*].metadata.name}'); do
  echo "--- $p ---"
  kubectl exec -n apz-lab5 "$p" -- curl -s http://localhost:8000/timings | python3 -m json.tool
done
echo "=== counter aggregate stats ==="
for p in $(kubectl get pods -n apz-lab5 -l app=counter-service -o jsonpath='{.items[*].metadata.name}'); do
  echo "--- $p ---"
  kubectl exec -n apz-lab5 "$p" -- curl -s http://localhost:8000/stats | python3 -m json.tool
done
```

→ **Take screenshot.** Shows per-pod attribution of work: how many
calls each facade replica handled, how many writes each counter
replica processed, average latency. This is where the comparison-table
numbers come from.

---

## 📊 Step 12 — Fill in the comparison table in REPORT.md (no screenshot)

After Screenshots 9, 10, 11, copy the numbers into the table in
`REPORT.md` § 6. Lab 1 and Lab 3 baselines are already in the report
from the previous lab runs (272.72 / 282.94 s for Lab 1; 428.12 / 429.20 s
for Lab 3).

---

## Cleanup

```bash
# Delete deployments + statefulsets but keep PG data
kubectl delete -f k8s/

# OR, full wipe including PV:
kubectl delete namespace apz-lab5
```

---

## Quick checklist

| # | Assignment requirement | Proof |
|---|---|---|
| 1 | «декілька екземплярів кожного сервісу» | `kubectl get pods` showing replicas |
| 2 | Service Discovery via K8s (req. 2) | `kubectl get svc/endpoints`, dynamic IPs |
| 3 | Hazelcast + MQ config in K8s KV (reqs. 3, 4) | `kubectl get configmap` + secret |
| 4 | counter actually computes balance | curl returns balance, `psql` shows `accounts` table |
| 5 | three logging-service replicas each get traffic | per-pod logs + per-call `logging_pod` field |
| 6 | counter persists in PG | `/stats` per pod + psql query |
| 7 | failover ls — calls reroute | delete pod, POST keeps working |
| 8 | failover cs — MQ keeps draining | delete pod, balance is still correct |
| 9 | perf scenario A → 10 000 each | `final balances` |
| 10 | perf scenario B → 100 000 shared | `final balances` |
| 11 | per-service contribution to total time | timing breakdown |
| 12 | comparison table Lab1 / Lab3 / Lab5 | filled-in REPORT.md § 6 |
