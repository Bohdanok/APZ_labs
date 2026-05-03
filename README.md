# Lab 5 — Microservices on Kubernetes (15 points)

> Бранч: **`micro_consul`**
> GitHub: *(< вставити URL >)*
> Студент: *(< прізвище та група >)*

---

## 1. Що зроблено

Архітектура з лаб 3 і 4 (facade / logging / counter + 3-нодовий Hazelcast + Postgres + Hazelcast-черга як MQ) перенесена в **Kubernetes**. Кастомний `config-server` і Consul видалені — замість них:

| Роль | Як виконує K8s |
|------|----------------|
| Service Register | Кожен Pod автоматично з'являється в `Endpoints` свого Service за міткою `app=...` (Deployment + readinessProbe) — нічого вручну реєструвати не треба, це канонічний спосіб K8s |
| Service Discovery | facade-service звертається до DNS-імен `logging-service` та `counter-service`, kube-proxy балансує між Ready-Pod-ами |
| Config Server | `ConfigMap app-config` (HZ_NODES, HZ_CLUSTER_NAME, HZ_MAP_NAME, MQ_QUEUE_NAME, DB_*, LOGGING_SERVICE_URL, COUNTER_SERVICE_URL) + `Secret db-secret` (DB_USER, DB_PASSWORD); pod-и підхоплюють їх через `envFrom` |

Все це — у єдиному namespace `apz-lab5`. Hazelcast, Postgres, MQ — теж окремі Pod-и в кластері (вимога K8s-варіанту).

---

## 2. Архітектурна схема (відповідає схемі з PDF)

```
                       Command (HTTP POST/GET msg)
                                  ▼
                        +----------------------+
                        | facade-service Svc   |  NodePort :30080
                        | (ClusterIP load-bal) |
                        +----------+-----------+
                                   |   replicas: 2
                       +-----------+-----------+
                       v                       v
             facade-pod-A                 facade-pod-B
                |   |                        |   |
   HTTP POST    |   |  MQ (HZ queue)         |   |
   {UUID,msg}   v   v  put(json)             v   v
        +---------------+         +----------+----------+
        | logging Svc   |         |  Hazelcast cluster   |
        | replicas: 3   |         |  (StatefulSet x3)    |
        +-------+-------+         |  hazelcast-0/1/2     |
                |                 |  Distributed Map     |
        +-------+-------+         |  + Distributed Queue |
        v       v       v         +----------+-----------+
       ls-A   ls-B   ls-C                    ▲
        \_______|_______/                    |
                |                            |
        Hazelcast Distributed Map            |  HTTP GET /message
        ("messages_map")                     |
                                             |
                              +--------------+----------+
                              | counter-service Svc     |
                              | replicas: 2 (queue.take)|
                              +-----+-----------+-------+
                                    v           v
                                  cs-A         cs-B
                                    \__________/
                                          |
                                  +-------+-------+
                                  | Postgres      |
                                  | StatefulSet x1|
                                  +---------------+
```

Усі дешеві стрілки (registration / discovery / KV-config) йдуть через kube-apiserver — у схемі прихованим compose-bus-ом.

---

## 3. Структура репозиторія

```
APZ_labs/
├── README.md                ← цей файл
├── requirements.txt         ← python-deps для всіх трьох сервісів
├── perf_test.py             ← тест продуктивності (1 / 10 акаунтів)
│
├── facade-service/
│   ├── Dockerfile
│   └── main.py              ← URL-и беруться з ENV (LOGGING_SERVICE_URL, COUNTER_SERVICE_URL)
│                              ← HZ_NODES / MQ_QUEUE_NAME — теж з ENV (з ConfigMap)
├── logging-service/
│   ├── Dockerfile
│   └── main.py              ← HZ_NODES / HZ_CLUSTER_NAME / HZ_MAP_NAME — з ConfigMap
├── counter-service/
│   ├── Dockerfile
│   └── main.py              ← HZ + MQ + DB cfg — з ConfigMap, DB_USER/PASS — з Secret
│
└── k8s/
    ├── 00-namespace.yaml
    ├── 01-configmap.yaml    ← app-config (KV) + hazelcast-config (член-ліст cluster-у)
    ├── 02-secret.yaml       ← db-secret
    ├── 03-postgres.yaml     ← StatefulSet + ClusterIP Service + PVC
    ├── 04-hazelcast.yaml    ← StatefulSet x3 + headless Service + ClusterIP Service
    ├── 05-logging-service.yaml  ← Deployment x3 + ClusterIP
    ├── 06-counter-service.yaml  ← Deployment x2 + ClusterIP
    └── 07-facade-service.yaml   ← Deployment x2 + NodePort 30080
```

У коді **немає жодної статичної адреси** іншого сервісу — `grep -RE 'localhost|127\.|hazelcast-[0-9]'` в `*-service/main.py` повертає 0 збігів. Всі адреси читаються з env, що мапиться з ConfigMap.

---

## 4. Як запускати

```bash
# 1. Підняти кластер
minikube start --driver=docker --cpus=4 --memory=4096

# 2. Зібрати образи всередині docker-демона minikube
eval $(minikube docker-env)
cd APZ_labs
docker build -t apz-lab5/facade-service:latest  -f facade-service/Dockerfile  .
docker build -t apz-lab5/logging-service:latest -f logging-service/Dockerfile .
docker build -t apz-lab5/counter-service:latest -f counter-service/Dockerfile .

# 3. Розкотити маніфести (порядок важливий лише при першому запуску)
kubectl apply -f k8s/

# 4. Дочекатися готовності всіх Pod-ів
kubectl -n apz-lab5 get pods -w

# 5. Отримати URL
echo "FACADE = http://$(minikube ip):30080"
```

Кінцевий стан після `apply`:

```
NAME                               READY   STATUS
counter-service-…-bmqmm            1/1     Running
counter-service-…-xdksc            1/1     Running
facade-service-…-5w6dh             1/1     Running
facade-service-…-dbbht             1/1     Running
hazelcast-0                        1/1     Running
hazelcast-1                        1/1     Running
hazelcast-2                        1/1     Running
logging-service-…-k24ls            1/1     Running
logging-service-…-nghp5            1/1     Running
logging-service-…-t2jvq            1/1     Running
postgres-0                         1/1     Running
```

---

## 5. Команди для перевірки + місця для скріншотів у звіті

> У всіх блоках нижче — конкретні команди і де саме клацати «PrtScr». Кожен скріншот покладіть у папку `screenshots/` репо і пiдпишiть як вказано.

### 📸 SCREENSHOT 1 — `screenshots/01-pods.png`

**Команда:**
```bash
kubectl -n apz-lab5 get pods -o wide
```

**Що має бути на скріні:** усі 11 Pod-ів зі статусом `1/1 Running` (3 hazelcast, 1 postgres, 3 logging, 2 counter, 2 facade). Це доказ вимоги (1) — «декілька екземплярів кожного сервісу запущені».

---

### 📸 SCREENSHOT 2 — `screenshots/02-services.png`

**Команда:**
```bash
kubectl -n apz-lab5 get services -o wide
kubectl -n apz-lab5 get endpoints
```

**Що має бути на скріні:**
* Service-и `facade-service` (NodePort), `logging-service` (ClusterIP), `counter-service` (ClusterIP), `hazelcast` + `hazelcast-headless`, `postgres`.
* В блоці endpoints — для `logging-service` 3 IP-адреси Pod-ів, для `counter-service` 2, для `facade-service` 2.
* Це доказ Service Discovery: за DNS-іменем `logging-service` ховаються 3 живих ендпоїнти, які kube-proxy балансує.

---

### 📸 SCREENSHOT 3 — `screenshots/03-configmap.png`

**Команда:**
```bash
kubectl -n apz-lab5 get configmap app-config -o yaml
kubectl -n apz-lab5 describe configmap app-config
```

**Що має бути на скріні:** усі ключі ConfigMap-у (HZ_NODES, HZ_CLUSTER_NAME, HZ_MAP_NAME, MQ_QUEUE_NAME, DB_HOST, DB_PORT, DB_NAME, LOGGING_SERVICE_URL, COUNTER_SERVICE_URL) і їх значення. Це доказ Config-Server-ролі K8s (вимоги 3 і 4).

---

### 📸 SCREENSHOT 4 — `screenshots/04-post-request.png`

**Команда:**
```bash
FACADE=http://$(minikube ip):30080
curl -s -X POST "$FACADE/facade" \
     -H 'Content-Type: application/json' \
     -d '{"msg":"hello-from-test"}' | jq
```

**Що має бути на скріні:** запит + JSON-відповідь:
```json
{
  "status": "ok",
  "uuid": "7f17bd89-…",
  "facade_instance": "facade-service-…-5w6dh",
  "logging_pod": "logging-service-…-nghp5",
  "log_ms": 115.72,
  "mq_ms": 3.79
}
```
Поля `facade_instance` і `logging_pod` показують, який саме Pod обслужив запит — підтверджує балансування.

Виконайте `curl … POST …` 5–6 разів, щоб різні pod-и засвітилися (`facade_instance` чергується між обома facade-pod-ами, `logging_pod` — між трьома logging-pod-ами).

---

### 📸 SCREENSHOT 5 — `screenshots/05-get-request.png`

**Команда:**
```bash
curl -s "$FACADE/facade"
```

**Що має бути на скріні:**
```
facade=facade-service-…-dbbht | logs=[hello-from-test, …] |
counter=[Total messages in DB: 6 (served by counter-service-…-xdksc)]
```
Тут видно, що facade зробив:
* GET `/log` до **logging-service** (читає Hazelcast distributed map)
* GET `/message` до **counter-service** (читає Postgres)

Кожен з них — через DNS-ім'я Service-а, без хардкоду адрес.

---

### 📸 SCREENSHOT 6 — `screenshots/06-pod-logs.png`

**Команди (по одній на скріншот, або в 4-х панелях):**
```bash
kubectl -n apz-lab5 logs deployment/facade-service  --all-containers --tail=15
kubectl -n apz-lab5 logs deployment/logging-service --all-containers --tail=15
kubectl -n apz-lab5 logs deployment/counter-service --all-containers --tail=15
```

**Що має бути на скріні:**
* у facade — рядки `[facade:fs-pod] uuid=… logging_pod=ls-pod log_dt=…ms mq_dt=…ms`
* у logging — `[logging:ls-pod] stored <uuid> -> <msg>`
* у counter — `[counter:cs-pod] persisted msg='…'`

Це доказ, що повідомлення проходить весь ланцюг (вимога «вміст консолі кожного сервісу»).

---

### 📸 SCREENSHOT 7 — `screenshots/07-failure-before.png` + `08-failure-after.png` + `09-failure-recovery.png`

**Команди:**
```bash
# 1) ДО -- 3 pod-и, 3 endpoint-и
kubectl -n apz-lab5 get pods -l app=logging-service -o wide --no-headers
kubectl -n apz-lab5 get endpoints logging-service
#   logging-service   10.244.0.4:8000,10.244.0.6:8000,10.244.0.7:8000

# 2) Видалити один pod
POD=$(kubectl -n apz-lab5 get pods -l app=logging-service -o name | head -1)
kubectl -n apz-lab5 delete $POD --grace-period=0 --force

# 3) ПІСЛЯ (миттєво) — endpoint видалено, новий pod в стані 0/1 Running
kubectl -n apz-lab5 get pods -l app=logging-service --no-headers
kubectl -n apz-lab5 get endpoints logging-service
#   logging-service   10.244.0.4:8000,10.244.0.6:8000

# 4) Запит при цьому проходить (балансування на здорові endpoint-и):
for i in 1 2 3; do
  curl -s -X POST "$FACADE/facade" -H 'Content-Type: application/json' \
       -d "{\"msg\":\"after_kill_$i\"}" | jq -c '.logging_pod'
done
#   "logging-service-…-t2jvq"   ← залишився живим
#   "logging-service-…-t2jvq"
#   "logging-service-…-nghp5"   ← залишився живим
```

**Що зробити:**
* `07-failure-before.png` — pod-листинг + endpoints до видалення.
* `08-failure-after.png`  — pod-листинг (старий зник, новий 0/1) + endpoints (зменшилися на 1) **відразу** після `kubectl delete`.
* `09-failure-recovery.png` — три curl-и з POST, у кожному `logging_pod` показує, що запити йдуть тільки до здорових pod-ів. **Це підтверджує вимогу (5).**

(Аналогічно можна продемонструвати для `counter-service` — `kubectl delete pod counter-service-…-xdksc`; GET `/facade` повертає `served by counter-service-…-bmqmm`.)

---

### 📸 SCREENSHOT 10 — `screenshots/10-scale.png` *(бонус, демонструє «масштабування через K8s»)*

**Команди:**
```bash
kubectl -n apz-lab5 scale deployment logging-service --replicas=5
sleep 8
kubectl -n apz-lab5 get pods -l app=logging-service
kubectl -n apz-lab5 get endpoints logging-service       # уже 5 ip-шок

kubectl -n apz-lab5 scale deployment logging-service --replicas=3
```

**Що має бути на скріні:** 5 pod-ів у списку, потім обидва скейли. Це додатковий бонусний момент — у звіті можна підкреслити, що `replicas` робить зайвою ручну реєстрацію.

---

### 📸 SCREENSHOT 11 — `screenshots/11-minikube-dashboard.png` *(опційно, замінює пункти 1–2 для краси)*

**Команда:**
```bash
minikube dashboard
```
відкриється UI у браузері. Відкрити Workloads → Pods, Services → Services, Config and Storage → Config Maps. Зробити скріншот вкладки `apz-lab5` → Pods.

---

## 6. Тестування продуктивності

### Запуск
```bash
FACADE_URL="http://$(minikube ip):30080" python perf_test.py
```

### Виміряні значення (поточний прогон, NUM_REQUESTS = 100)

| Test scenario | Task 1 (in-mem) *з попереднього звіту* | Task 3 (HZ + DB) *з попереднього звіту* | **Task 5 (K8s)** |
|---|---|---|---|
| **10 accounts** — Total time            | *< вставити >* | *< вставити >* | **0.671 s**  |
| logging-service contribution avg/p95    | n/a            | *< вставити >* | **37.77 / 59.13 ms** |
| counter-service contribution avg/p95    | n/a            | *< вставити >* | **0.68 / 0.92 ms**   |
| **1 account**  — Total time             | *< вставити >* | *< вставити >* | **1.350 s**  |
| logging-service contribution avg/p95    | n/a            | *< вставити >* | **10.42 / 14.72 ms** |
| counter-service contribution avg/p95    | n/a            | *< вставити >* | **0.66 / 0.77 ms**   |

### 📸 SCREENSHOT 12 — `screenshots/12-perf-test.png`

**Команда:**
```bash
FACADE_URL="http://$(minikube ip):30080" python perf_test.py
```

**Що має бути на скріні:** повний вивід `perf_test.py`, включно з обома сценаріями. Цифри з нього мають відповідати клітинкам у таблиці вище.

### Обговорення

* **counter-service contribution (~0.7 ms)** — це лише `queue.put()` на сторону Hazelcast, бо з лаб 4 цей виклик асинхронний (через MQ). Сам counter-service дренує чергу та пише в Postgres у фоні; затримка персиста не впливає на час відповіді.
* **logging-service contribution** домінує: це синхронний HTTP POST до `logging-service` через K8s Service. На 10 паралельних клієнтах час одного запиту росте з 10 ms до 38 ms — навантаження ділиться на 3 logging-pod-и.
* Загальний час 10-account-сценарію (0.67 s) **менший**, ніж 1-account (1.35 s) — це і є виграш від паралелізму та kube-proxy-балансування.
* Накладні витрати на DNS-resolve K8s-Service-у — < 1 ms, у межах похибки.

---

## 7. Перевірка відповідності всім вимогам PDF (5 пунктів)

| # | Вимога | Як виконано | Перевірка / скріншот |
|---|--------|------------|----------------------|
| 1 | Усі мікросервіси самостійно динамічно реєструються в K8s; декілька екземплярів | Deployment-и з `replicas: 3 / 2 / 2`; кожен Pod автоматично попадає в `Endpoints` Service-а через label-селектор + readinessProbe | Scr 1, Scr 2, Scr 11 |
| 2 | facade-service читає IP-адреси (та порти) `logging-service`/`counter-service` з K8s, без статичних адрес | URL-и у ConfigMap `app-config`, підтягуються в Pod через `envFrom`. Балансування робить kube-proxy через DNS-ім'я Service-у. У коді — лише `os.environ["LOGGING_SERVICE_URL"]` | Scr 3, Scr 4, Scr 5; `grep` по `*-service/main.py` |
| 3 | Налаштування Hazelcast-клієнтів збережені як key/value у K8s і зчитуються logging-service | ConfigMap `app-config`: `HZ_NODES`, `HZ_CLUSTER_NAME`, `HZ_MAP_NAME` → `envFrom` у logging-service | Scr 3, Scr 6 (logs показують `config -> HZ_NODES=...`) |
| 4 | Налаштування MQ збережені як key/value у K8s і зчитуються facade- та counter-service | Той самий ConfigMap: `MQ_QUEUE_NAME` (+ HZ_NODES — фізично черга живе у HZ) → `envFrom` у facade і counter | Scr 3, Scr 6 |
| 5 | Відключення екземпляру → статус у K8s, виклики переспрямовуються | `kubectl delete pod` → endpoint видалено; запити йдуть лише до Ready-pod-ів; K8s тут же піднімає replacement | Scr 7, 8, 9 |

**Бонус K8s-варіанту (за PDF: «всі сервіси та супутні системи мають запускатись у K8s як окремі Pod-и»):**
* Hazelcast — StatefulSet × 3 ✅
* Postgres — StatefulSet × 1 з PVC ✅
* MQ — це Hazelcast Distributed Queue, тобто живе у тих самих 3 Pod-ах ✅
* «Можливість масштабувати через Kubernetes» — `kubectl scale deployment …` (Scr 10) ✅
* yaml-файли для всіх компонентів у `k8s/` ✅

---

## 8. Зупинка

```bash
kubectl delete -f k8s/
minikube stop      # зберігає кластер
# або
minikube delete    # повна очистка
```
