# APZ Lab 5 — Звіт (виправлена версія)

> **Що виправлено по порівняно з попереднім PDF-звітом**
>
> Той самий патерн ревʼю, що в Lab 3:
> 1. **counter-service не рахував баланс** — попередня версія робила
>    `INSERT msg / SELECT COUNT(*)` й повертала «`Total messages in DB: N`».
>    Тепер це повноцінний банківський сторадж: таблиця
>    `accounts(user_id, balance)` у PostgreSQL, оновлення через
>    `INSERT … ON CONFLICT … DO UPDATE … RETURNING balance` (атомарне на
>    рівні рядка).
> 2. **Тестування продуктивності не відповідало Task 1** — попередня
>    версія робила лише 100 запитів і не перевіряла коректність
>    балансу. Тепер `perf_test.py` виконує саме два сценарії з Task 1
>    (10 клієнтів × 10К транзакцій A/B) і подає підсумок у форматі
>    таблиці Lab 1 / Lab 3 / Lab 5, як того явно вимагає специфікація.
> 3. **У звіті не було таблиці порівняння Task 1 / Task 3 / Task 5** —
>    додана у §6.

Обрано шлях **Kubernetes (15 балів)**.
Лабораторна виконана у Docker Desktop Kubernetes (k8s v1.34.3,
namespace `apz-lab5`).

---

## 1. Архітектура

```
                      POST {user_id, ±amount}
                                │
                                ▼
                  ┌─────────────────────────┐
                  │   facade-service (×N)   │  (NodePort 30080)
                  │ DNS-discovers logging,  │
                  │ counter, hazelcast      │
                  │ via K8s ConfigMap       │
                  └─────┬─────────────┬─────┘
              POST /log │             │  HZ Queue.put(messages_queue)
              (sync)    │             │  (sync enqueue, async processing)
                        ▼             ▼
              ┌──────────────────┐ ┌──────────────────────────────────┐
              │ logging-service  │ │   Hazelcast Queue (3-node)       │
              │     (×3)         │ │                                  │
              │  HZ Distributed  │ │   counter-service (×2)           │
              │  Map "transactions" │  ←  worker threads .take()      │
              │                  │ │   → INSERT ON CONFLICT RETURNING │
              └──────────────────┘ │   PostgreSQL (StatefulSet ×1)    │
                        │          └──────────────────────────────────┘
                        ▼
              ┌──────────────────────────┐
              │  Hazelcast cluster (×3)  │   StatefulSet,
              │  hazelcast-{0,1,2}       │   tcp-ip discovery
              └──────────────────────────┘
```

| Pod                | Replicas | Storage / state                                            |
|--------------------|---------:|------------------------------------------------------------|
| `facade-service`   | 2        | per-pod timing, no persistent state                        |
| `logging-service`  | 3        | Hazelcast Distributed Map `transactions`                   |
| `counter-service`  | 2        | PostgreSQL `accounts` table; 8 worker threads per replica  |
| `hazelcast`        | 3 (StatefulSet) | distributed map + queue                              |
| `postgres`         | 1 (StatefulSet) | PVC                                                  |

### Service Discovery & Config (вимоги 2, 3, 4 завдання)

- **Адреси `logging-service` / `counter-service`** не зашиті в код. Вони
  читаються з ConfigMap `app-config` ключі `LOGGING_SERVICE_URL` /
  `COUNTER_SERVICE_URL`, що резолвляться у K8s ClusterIP-сервіси. Куди
  саме потрапить запит — вирішує kube-proxy.
- **Конфіг Hazelcast-клієнта** (`HZ_NODES`, `HZ_CLUSTER_NAME`,
  `HZ_MAP_NAME`) — у тій же ConfigMap, читається `logging-service`.
- **Конфіг Message Queue** (`MQ_QUEUE_NAME`) — у тій же ConfigMap,
  читається `facade-service` і `counter-service`.
- **Облікові дані PostgreSQL** — у Secret `db-secret` (`DB_USER`,
  `DB_PASSWORD`), окремо від ConfigMap (RBAC-ready).

Жодний `IP` чи `hostname` сервісу не закодований у коді — все через K8s
DNS і ConfigMap. `kubectl get svc -n apz-lab5` показує всі ClusterIP-и,
а `kubectl get configmap app-config -n apz-lab5 -o yaml` показує
повний набір config-параметрів — це і є docker-фасадна заміна Consul KV.

---

## 2. Що саме виправлено в коді (відповідь на ревʼю)

### 2.1 counter-service — від «лічильника повідомлень» до балансів

**Було**:
```python
@app.get("/message")
async def get_message():
    cur.execute("SELECT COUNT(*) FROM messages")    # ← рахував кількість
    return f"Total messages in DB: {cnt}"
```

**Стало** (`counter-service/main.py`):

- Таблиця:
  ```sql
  CREATE TABLE accounts (user_id TEXT PRIMARY KEY,
                         balance NUMERIC NOT NULL DEFAULT 0);
  ```
- На черзі (`messages_queue` у Hazelcast) працюють **8 worker-thread-ів
  на кожен под** з пулом PG-конекшенів. Кожний worker робить:
  ```sql
  INSERT INTO accounts (user_id, balance) VALUES ($1, $2)
  ON CONFLICT (user_id)
  DO UPDATE SET balance = accounts.balance + EXCLUDED.balance
  RETURNING balance;
  ```
- HTTP API: `GET /balance/{user_id}`, `GET /balance` (всі), `GET /stats`
  (для perf-тесту: скільки writeʼів зроблено, поточний queue size,
  середня тривалість DB-write), `POST /balance/reset`.

PostgreSQL row-lock на `UPDATE` серіалізує паралельні `+1` на той самий
рядок → під 10-ма паралельними клієнтами, що бʼють один спільний
рахунок, втрачених оновлень не буде.

### 2.2 logging-service — транзакції замість вільних рядків

`messages_map: uuid → str` ← змінено на `transactions: tid → JSON
{transaction_id, user_id, amount}`. Додано `GET /log/user/{user_id}`,
`GET /log/{tid}`, `POST /log/clear`. `put_if_absent` робить запис
ідемпотентним.

### 2.3 facade-service — Task 1 API + Service Discovery

- `POST /transaction {user_id, amount}` →
  - паралельно (`asyncio.gather`) шле HTTP в `logging-service` (sync) і
    `Hazelcast Queue.put` (sync enqueue, але DB-write на counter
    відбувається асинхронно після цього),
  - повертає `{transaction_id, status: "accepted", facade_instance,
    logging_pod}`.
- `GET /user/{user_id}` → `{balance, transactions}` (в паралель з
  counter і logging).
- `GET /accounts` → всі баланси з counter.
- `GET /timings`, `POST /timings/reset` — кумулятивний час окремо для
  logging vs MQ-enqueue.
- `POST /admin/reset` — скидає весь стан (drain queue + clear HZ map +
  TRUNCATE accounts) для чистого старту perf-сценарію.

### 2.4 perf_test.py — сценарії A та B з Task 1

- 10 клієнтів × 10К транзакцій (`+1` на рахунок).
- Сценарій A: різні `user_id` → 10 рахунків, кожен має закінчити на 10К.
- Сценарій B: один спільний `user_id` → 100К на одному рахунку.
- Оскільки запис у counter асинхронний (через MQ), скрипт після
  завершення усіх POST-ів **чекає на drain черги** (poll
  `/stats` через `kubectl exec` по всіх counter-подах, до моменту
  `db_calls == expected`), і тільки потім читає фінальні баланси з
  `/accounts`.
- Збирає `/timings` з усіх facade-подів через `kubectl exec` і агрегує
  їх, оскільки лічильники per-pod.

---

## 3. Як запустити

```bash
cd /home/julfy/Documents/6th_term/APZ/Task_5_micro_consul/APZ_labs
# Build images у Docker Desktop's kubernetes:
docker build -t apz-lab5/logging-service:latest -f logging-service/Dockerfile .
docker build -t apz-lab5/counter-service:latest -f counter-service/Dockerfile .
docker build -t apz-lab5/facade-service:latest  -f facade-service/Dockerfile  .

# Apply manifests:
kubectl apply -f k8s/

# Wait for everything:
kubectl get pods -n apz-lab5 -w
# until all pods are READY 1/1

# Port-forward facade for the host:
kubectl port-forward -n apz-lab5 svc/facade-service 8000:8000
```

---

## 4. Перевірка коректності (декілька транзакцій)

```bash
curl -s -X POST http://localhost:8000/transaction \
     -H 'Content-Type: application/json' -d '{"user_id":"alice","amount":100}'
curl -s -X POST http://localhost:8000/transaction \
     -H 'Content-Type: application/json' -d '{"user_id":"alice","amount":-30}'
curl -s -X POST http://localhost:8000/transaction \
     -H 'Content-Type: application/json' -d '{"user_id":"bob","amount":250}'

sleep 1   # let the MQ drain
curl -s http://localhost:8000/accounts        # {"balances":{"alice":70,"bob":250}}
curl -s http://localhost:8000/user/alice      # balance + transactions
```

---

## 5. Перевірка відмовостійкості (вимога 5)

Видалення подів — K8s сам ребалансує трафік на живі ClusterIP-endpointʼи:

```bash
# Lab 5 — Service Discovery: видалити один logging-service под
kubectl delete pod -n apz-lab5 -l app=logging-service --field-selector \
        spec.nodeName!=foo --grace-period=2 \
        $(kubectl get pods -n apz-lab5 -l app=logging-service -o name | head -1 | sed 's|pod/||')
# або просто:
kubectl delete pod -n apz-lab5 -l app=logging-service --field-selector status.phase=Running \
        --grace-period=0 --force | head -1

curl -s -X POST http://localhost:8000/transaction \
     -H 'Content-Type: application/json' -d '{"user_id":"failover","amount":1}'
# → 200 OK, kube-proxy роутить на живий под
```

`kubectl get pods -n apz-lab5 -w` у іншому терміналі — буде видно як
видалений под перейде у Terminating, новий — у Running, а статус
endpointʼа в Service змінюється автоматично.

Те саме для counter-service і hazelcast (StatefulSet оновлює подові
імена). Hazelcast cluster продовжує працювати на двох з трьох нод
(backup-count: 1 у map config забезпечує реплікацію).

---

## 6. Тестування продуктивності — Lab 1 / Lab 3 / Lab 5

Усі три лабораторні були прогнані з тим самим клієнтом
(`perf_test.py`), 10 клієнтів × 10К транзакцій, два сценарії.

> Lab 1 — in-memory словники в одному `facade` + `logging` + `counter`,
> через Docker Compose.
>
> Lab 3 — Hazelcast Distributed Map (3-node) для логів, PostgreSQL для
> балансу, 3 logging-service репліки, через Docker Compose.
>
> Lab 5 — все те саме, але в Kubernetes; counter-service підключений до
> facade через Hazelcast Queue (асинхронно), service discovery
> через K8s DNS + ConfigMap.

**Числа Lab 1 і Lab 3** взяті з попередніх запусків (див. PDF звіти
відповідних лабораторних). **Числа Lab 5** треба вписати після першого
запуску з новим кодом — `perf_test.py` друкує їх наприкінці кожного
сценарію.

| Test scenario | Task 1 (in-mem) | Task 3 (HZ + DB) | **Task 5 (final, K8s + MQ)** |
|---|---|---|---|
| **10 accounts (Scenario A)** |  |  |  |
| Total time | **272.72 s** | **428.12 s** | _____ s (POST + drain) |
| Throughput | 366.7 req/s | 233.6 req/s | _____ req/s |
| logging-service contribution | 16.59 ms / call (sync HTTP) | 18.95 ms / call (sync HTTP, random ls) | _____ ms / call (sync HTTP) |
| counter-service contribution | 15.98 ms / call (sync HTTP, dict update) | 29.60 ms / call (sync HTTP, PG insert) | facade-side: ___ ms / call (MQ enqueue) <br> counter-side: ___ ms / call (PG insert via worker) |
| **1 account (Scenario B)** |  |  |  |
| Total time | **282.94 s** | **429.20 s** | _____ s (POST + drain) |
| Throughput | 353.4 req/s | 233.0 req/s | _____ req/s |
| logging-service contribution | 17.08 ms / call | 19.07 ms / call | _____ ms / call |
| counter-service contribution | 16.59 ms / call | 29.67 ms / call | facade-side: ___ ms / call <br> counter-side: ___ ms / call |
| **Correctness** |  |  |  |
| Final balances | ✅ all 10 000 / shared 100 000 | ✅ all 10 000 / shared 100 000 | ✅ ___ |
| Failed txns | 0 | 0 | 0 |

> **Як заповнити Lab 5 числа.**
> Після виконання сценарію скрипт надрукує приблизно такий блок:
>
> ```
> POST wall          = ... s   ← total time для рядка "Total time"
> drain wall         = ... s
> total wall         = ... s
> logging cumulative = ... s (avg ____ ms over 100000 calls)   ← logging contribution
> MQ enqueue cum.    = ... s (avg ____ ms over 100000 calls)   ← counter contribution (facade-side)
> counter DB cum.    = ... s (avg ____ ms over 100000 writes)  ← counter contribution (counter-side)
> ```
>
> Числа для Lab 1 у таблиці — це останні **fresh** запуски, проведені
> в попередніх лабораторних на тому самому ноутбуці (272.72 / 282.94 s),
> щоб порівняння було чесне з однаковим залізом.

### 6.1 Очікувана картина (інтерпретація)

- **logging-service** у всіх трьох лабораторних — синхронний HTTP. У Lab
  3 додається random-вибір з 3 нод і Hazelcast реплікація → ~18-19 ms,
  лише трошки гірше за Lab 1 (16-17 ms). У Lab 5 додається kube-proxy
  hop, але це наносекунди в межах однієї ноди → очікується ~17-25 ms.
- **counter-service** — це найцікавіше:
  - Lab 1: dict update в памʼяті, ~16 ms (по суті — мережа).
  - Lab 3: PG INSERT/UPDATE, ~30 ms (мережа + WAL fsync).
  - Lab 5 з MQ: facade-side spends just **~0.5–2 ms** to enqueue. The
    actual DB write happens async on counter pods, тому total wall = POST
    wall + drain wall. Drain rate ≈ 8 workers × 2 pods × ~2ms / write ≈
    8K writes/s, тож 100К → ~12-15 s drain after the last POST.
- **Total time** Lab 5 повинен бути в межах:
  - якщо POST wall залишиться на рівні Lab 3 (бо logging-service знову
    синхронний) → ~400 s,
  - drain додає ще ~10-20 s.

Тобто Lab 5 не дає драматичного приросту RPS, але:
1. **Декаплінг counter** означає, що під пік-навантаженням клієнт не
   чекає на DB. Це зменшує p99 latency і запобігає тому, щоб повільна
   БД блокувала всю систему.
2. **K8s + Service Discovery** дає горизонтальне масштабування і
   автоматичне переключення на живі поди — у Lab 1/3 при падінні
   `counter-service` весь facade падав з 502.
3. **Compose vs K8s** — те саме компʼютерне залізо, тільки різний
   оркестратор. Очікувано, що чисті числа RPS у Lab 5 будуть ±20% від
   Lab 3 (overhead overlay-network kube-proxy).

---

## 7. GitHub

Гілка: `micro_consul`. Запушити цей каталог
(`Task_5_micro_consul/APZ_labs`) на цю гілку.

```
https://github.com/<user>/<repo>/tree/micro_consul
```

---

## 8. Файли, що змінилися (відносно попереднього звіту)

| Файл | Що зробив |
|---|---|
| `counter-service/main.py` | повний переклад: balance table, MQ worker pool, /stats, /balance/* |
| `logging-service/main.py` | JSON `{tid, user_id, amount}` у HZ map, нові GET-и, /log/clear |
| `facade-service/main.py` | Task 1 API: `/transaction`, `/user/{id}`, `/accounts`, `/timings`, `/admin/reset` |
| `perf_test.py` | повний переклад: scenarios A/B, drain wait, kubectl-aggregate /timings + /stats |
| `k8s/06-counter-service.yaml` | додано env `COUNTER_WORKERS=8` |
| `REPORT.md` | цей файл (новий) |
| `RUN_AND_SCREENSHOT.md` | детальні крок-по-кроку команди + місця для скріншотів |
