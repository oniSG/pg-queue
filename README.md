# pg-queue

A Rust testbed for using Postgres as a work queue.

The example domain is a "segment recalculation" job: an HTTP API creates
`segments` and enqueues recalculation jobs; a fleet of workers pulls jobs with
`SELECT … FOR UPDATE SKIP LOCKED`, does the work, and records every attempt.

Everything runs on Kubernetes (k3d) via Tilt.

---

## Quickstart

Prerequisites — Docker running, plus:

```sh
brew install tilt-dev/tap/tilt k3d
```

Create the k3d cluster with a companion Docker registry. Tilt pushes images to
`localhost:5111`; the cluster's containerd pulls from the same registry over
k3d's Docker network.

```sh
k3d cluster create pg-queue --registry-create pgregistry:0.0.0.0:5111
```

Bring the stack up:

```sh
tilt up
```

You get:

| What | Where |
|---|---|
| Tilt UI | <http://localhost:10350> |
| Scalar API UI | <http://localhost:3000/scalar> |
| OpenAPI JSON | <http://localhost:3000/openapi.json> |
| Postgres | `psql -h localhost -p 5432 -U pgqueue -d pgqueue` (password `pgqueue`) |

Smoke-test the full loop:

```sh
SID=$(curl -sf -X POST http://localhost:3000/segments \
  -H 'content-type: application/json' -d '{"name":"eu-users"}' \
  | grep -oE '"id":"[^"]+"' | cut -d'"' -f4)

for _ in $(seq 1 10); do
  curl -sf -X POST http://localhost:3000/segment-recalculate-queue \
    -H 'content-type: application/json' -d "{\"segment_id\":\"$SID\"}"
done

# a few seconds later every job is `succeeded`, work spread across the 3 pods:
kubectl exec deploy/postgres -- psql -U pgqueue -d pgqueue -c \
  "SELECT status, COUNT(*) FROM segment_recalculate_queue GROUP BY status"

kubectl logs -l app=worker --tail=100 --prefix
```

Teardown:

```sh
tilt down                      # stop workloads, keep cluster
k3d cluster delete pg-queue    # nuke cluster + registry
```

---

## Stack

| Layer | Choice | Notes |
|---|---|---|
| Language | Rust | Local cargo needs ≥ 1.85 (a transitive dep requires `edition2024`). The Dockerfile uses `rust:1-bookworm`. |
| HTTP | axum 0.8 | |
| Database | sqlx 0.8 + Postgres 16 | Runtime queries (no `query!` macro, no `.sqlx` offline). |
| Async | Tokio | |
| Docs | utoipa + utoipa-scalar | OpenAPI 3.1 auto-generated from handler attributes; Scalar UI at `/scalar`. |
| Logs | tracing + tracing-subscriber | `RUST_LOG` controls filtering. |
| Image | Multi-stage Dockerfile | One image, two entrypoints (`server`, `worker`). BuildKit cache mounts for cargo registry + `target/`. |
| Orchestration | k3d + Tilt | k3d = k3s in Docker; Tilt owns builds, deploys, port-forwards, hot reload. |

---

## Services

Three deployments in the `default` namespace:

| Deployment | Replicas | Role |
|---|:-:|---|
| `postgres` | 1 | Postgres 16. Holds `segments`, `segment_recalculate_queue`, `queue_executions`. In-cluster address `postgres:5432`. |
| `server`   | 1 | Axum HTTP API. Sole writer of `segments`; enqueues into `segment_recalculate_queue`. Serves Scalar UI + OpenAPI JSON. |
| `worker`   | 3 | Consumers. Each replica polls, atomically claims one pending job via `SKIP LOCKED`, does the work, records success or failure. |

`server` and `worker` share the same image (`pg-queue-app`) — they differ only
in the `command:` on the Deployment. One `docker build` → both roll.

---

## Architecture

```
                        ┌────────────────────┐
                        │   Scalar UI        │  :3000/scalar
   curl / browser ─────▶│   OpenAPI JSON     │  :3000/openapi.json
                        │   HTTP API         │
                        └──────────┬─────────┘
                                   │
                                   ▼
                        ┌────────────────────┐
                        │      server        │  axum, :3000
                        │    (1 replica)     │
                        └──────────┬─────────┘
                                   │  INSERT segments
                                   │  INSERT srq
                                   ▼
                        ┌────────────────────┐
                        │      postgres      │  :5432 (port-forwarded to host)
                        │                    │
                        │  segments                                    │
                        │  segment_recalculate_queue  ← queue rows     │
                        │  queue_executions           ← attempt log    │
                        │  enum queue_job_status                       │
                        └──────────┬─────────┘
                                   │  SELECT … FOR UPDATE SKIP LOCKED
                        ┌──────────┼──────────┐
                        ▼          ▼          ▼
                    worker-1   worker-2   worker-3     Deployment worker, replicas: 3
```

Everything but your shell runs inside the k3d node container
`k3d-pg-queue-server-0` via k3s + containerd. Docker Desktop only shows k3d's
infrastructure containers (`k3d-pg-queue-server-0`, `-serverlb`, `-tools`,
`pgregistry`); the app pods live one layer down — see them with `kubectl get pods`
or `docker exec -it k3d-pg-queue-server-0 crictl ps`.

---

## How the queue works

### Schema

```
enum queue_job_status = { pending, running, succeeded, failed }

segments                             -- domain rows
  id (uuid) PK
  name
  recalculated_at  (nullable — set by worker on success)
  created_at, updated_at

segment_recalculate_queue            -- the queue
  id (bigserial) PK
  segment_id       → segments.id  ON DELETE CASCADE
  status           queue_job_status  DEFAULT 'pending'
  scheduled_for    timestamptz       DEFAULT NOW()
  created_at, updated_at

  partial index (scheduled_for) WHERE status = 'pending'   -- the hot path

queue_executions                     -- one row per attempt (audit log)
  id (bigserial) PK
  queue_id       → segment_recalculate_queue.id  ON DELETE CASCADE
  status         queue_job_status  DEFAULT 'running'
                   CHECK (status IN ('running','succeeded','failed'))
  started_at, ended_at
  error          jsonb              -- populated on failure
```

There is no `attempts` or `last_error` column on the queue row. Both are
derivable from `queue_executions`:

- `attempts`   = `SELECT COUNT(*)  FROM queue_executions WHERE queue_id = ?`
- `last_error` = newest `error` where `queue_id = ? AND status = 'failed'`

### Lifecycle

```
                       enqueue
                          │
                          ▼
                   ┌─────────────┐
    ┌── retry ────▶│   pending   │
    │  (attempts   └──────┬──────┘
    │   < 3;              │  atomic claim, one txn:
    │   scheduled_        │    UPDATE srq → 'running'
    │   for += 5s)        │    WHERE id = (SELECT … FOR UPDATE SKIP LOCKED LIMIT 1)
    │                     │    RETURNING id, segment_id
    │                     │    + INSERT queue_executions (status='running')
    │                     ▼
    │              ┌─────────────┐
    │              │   running   │──── do_work()  (100–500ms, ~15% fail)
    │              └──┬────────┬─┘
    │             ok ✓│        │✗ err
    │                 ▼        ▼
    │          ┌───────────┐  ┌──────────────────┐
    │          │ succeeded │  │  queue_execution │
    │          └───────────┘  │   → 'failed'     │
    │                ▲        │   error = jsonb  │
    │                │        └────────┬─────────┘
    │      also: UPDATE               │
    │      segments.recalculated_     │
    │      at = NOW()                 │  attempts ≥ 3
    │                                 ▼
    └────────── attempts < 3 ─┐  ┌───────────┐
                              │  │  failed   │  (terminal)
                              └─▶└───────────┘
```

Every finalization — success, retry, or terminal fail — is a single
transaction that touches `queue_executions`, `segment_recalculate_queue`, and
(on success) `segments`.

### Why the pattern is safe

- **No double-claims.** `FOR UPDATE SKIP LOCKED` makes Worker B's `SELECT`
  skip the row Worker A is holding a row-lock on. Verified in tests with 3
  workers vs many jobs: total execution rows = total claims across pods, with
  no overlap.
- **No orphan `running` rows on worker crash.** The claim, the
  `queue_executions` insert, and every finalization are single transactions —
  a crash mid-attempt rolls back and the row goes back to `pending`.
- **Retries with backoff.** The partial index `WHERE status = 'pending'` keeps
  the dequeue query O(index lookup) even as `queue_executions` grows.
- **Single source of truth for retry count.** `attempts` is derived from
  `queue_executions`, not duplicated on the queue row — no chance of drift.

### Tunables

Constants at the top of `src/bin/worker.rs`:

| Constant | Default | Meaning |
|---|---|---|
| `POLL_INTERVAL` | 1s | Sleep when the queue is empty. |
| `MAX_ATTEMPTS`  | 3   | Attempts before a job is marked terminal `failed`. |
| `RETRY_BACKOFF` | 5s  | Added to `scheduled_for` on retry. |
| `WORK_MIN` / `WORK_MAX` | 100ms / 500ms | Simulated work range. |
| `FAIL_RATE` | 0.15 | Simulated failure probability. |

---

## API

Full spec at `/openapi.json`, rendered as Scalar at `/scalar`. Summary:

| Method | Path | Purpose |
|---|---|---|
| GET | `/health` | Liveness. |
| GET | `/segments` | List segments. |
| POST | `/segments` | Create a segment. |
| GET | `/segments/{id}` | Fetch one. |
| PATCH | `/segments/{id}` | Rename. |
| DELETE | `/segments/{id}` | Delete (cascades queue + executions). |
| POST | `/segment-recalculate-queue` | Enqueue a recalculation for a `segment_id`. |

---

## Local dev without Kubernetes

The Rust iteration loop against the (Tilt-managed) Postgres — no image
rebuilds, just cargo:

```sh
cp .env.example .env       # first time only
tilt up                    # so postgres is running + port-forwarded on 5432
cargo run --bin server     # or --bin worker (open more shells for more workers)
```

`dotenvy` picks up `.env` automatically. Alternatively, pass it inline:

```sh
DATABASE_URL=postgres://pgqueue:pgqueue@localhost:5432/pgqueue \
  cargo run --bin worker
```

Both binaries call `sqlx::migrate!()` on startup — safe to run concurrently
(sqlx uses `pg_advisory_lock` to serialize).

---

## Layout

```
pg-queue/
├── src/
│   ├── lib.rs                        shared: QueueJobStatus enum, pool/migrations/tracing helpers
│   └── bin/
│       ├── server.rs                 axum HTTP API + Scalar mount
│       └── worker.rs                 queue worker loop
├── migrations/                       sqlx migrations, applied at startup
│   ├── 20260706000001_create_segments.sql
│   ├── 20260706000002_create_segment_recalculate_queue.sql
│   └── 20260706000003_create_queue_executions.sql
├── k8s/
│   ├── postgres.yaml                 Deployment + Service, pg_isready probes
│   ├── server.yaml                   Deployment + Service, /health probes
│   └── worker.yaml                   Deployment, replicas: 3
├── Dockerfile                        multi-stage; single image with both binaries
├── .dockerignore
├── Tiltfile                          docker_build + k8s_yaml + default_registry
├── Cargo.toml
├── Cargo.lock
├── .env.example
└── README.md
```

---

## Common gotchas

- **`tilt up` fails with `push access denied … docker.io/library/pg-queue-app`.**
  You forgot `--registry-create` when creating the cluster. Recreate it:
  ```sh
  k3d cluster delete pg-queue
  k3d cluster create pg-queue --registry-create pgregistry:0.0.0.0:5111
  ```
- **`cargo build` locally errors on `edition2024`.** Cargo < 1.85. Update Rust.
- **Only one `worker` row in the Tilt UI, but 3 replicas.** Tilt shows one row
  per `k8s_resource`, not per pod. See real pods with
  `kubectl get pods -l app=worker`.
