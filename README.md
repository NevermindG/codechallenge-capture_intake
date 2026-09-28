# Capture Intake Service — 309 Technology technical assessment

Small Go service for the capture intake exercise. The implementation focuses on the graded correctness paths: request validation and bounded resources, database-enforced idempotency under concurrency, a transactional outbox, restart-safe worker claiming, bounded exponential retry with jitter, exact decimal handling, structured logs, readiness and graceful shutdown.

## Stack

- Go 1.26+
- PostgreSQL 16+
- `net/http` + chi v5
- pgx v5, hand-written SQL only
- `log/slog` JSON logs
- Docker Compose
- Standard-library `testing`, `httptest`, and handwritten fakes

The dependency list is intentionally short: chi is used for routing, pgx for PostgreSQL access, and everything else is standard library.

## Run

The intended first-run experience is one command:

```bash
docker compose up --build
```

That starts PostgreSQL, the capture service, and a stub downstream. The API waits for PostgreSQL, runs embedded migrations, then starts HTTP and the outbox worker.

API: `http://localhost:8080`

Stub downstream: `http://localhost:8081`

### Example capture

The manifest contract is intentionally small and is defined by this implementation:

```json
{
  "capturedAt": "2026-09-27T17:00:00Z",
  "amount": "123.450000",
  "currency": "PEN"
}
```

`amount` is a JSON string by design. It is parsed into an exact decimal type and is never converted through `float32` or `float64`.

Create a capture using a real image file:

```bash
curl -i \
  -H 'Idempotency-Key: capture-001' \
  -F 'manifest={"capturedAt":"2026-09-27T17:00:00Z","amount":"123.450000","currency":"PEN"};type=application/json' \
  -F 'frame=@testdata/frame.png;type=image/png' \
  http://localhost:8080/v1/captures
```

A first accepted request returns `201`. Repeating the same `Idempotency-Key` returns `200` with `duplicate: true` and the original capture. A different key always creates a different capture even when the body is identical.

Read it back:

```bash
curl -i http://localhost:8080/v1/captures/capture-001
```

Watch deliveries:

```bash
curl http://localhost:8081/stats
```

## What happens to a capture

The HTTP request performs validation first. The application then enters a database transaction and acquires a transaction-scoped PostgreSQL advisory lock derived from the idempotency key. While holding that lock it checks for an existing capture. If none exists, it writes the image to the deterministic final path, then inserts the capture row and its outbox row in the same PostgreSQL transaction.

This ordering matters because the image is outside PostgreSQL. The final image exists before the DB commit, so a process death immediately after the DB commit cannot create a committed capture whose image was never written. If the transaction later rolls back, a file may be left behind; a later retry for the same key can safely replace that uncommitted orphan because it first proves that no committed capture exists for the key. The README deliberately calls this trade-off out instead of pretending filesystem + PostgreSQL is one atomic transaction.

## Architecture

The tree is grouped by domain rather than by a global `controllers/services/repositories` technical split:

```text
cmd/
  api/                     composition root / dependency wiring
  downstream/              local stub downstream service

internal/
  capture/
    domain/                capture, manifest, UUID, decimal, domain errors
    application/
      intake/              acceptance use case + ports
      outbox/              worker use case + ports + retry policy
    adapters/
      http/                HTTP transport and contract mapping
      postgres/            pgx repository + embedded SQL migrations
      filesystem/          mounted-volume image store
      downstream/          HTTP downstream client
  platform/
    config/                environment configuration
    logging/               JSON logging setup

testdata/
  frame.png                small image fixture for manual and test usage
```

### Hexagonal boundaries

The application layer owns the ports. The core use cases do not import chi, HTTP types, pgx, or filesystem packages.

- `intake.Service` depends on `CaptureUnitOfWork`, `ImageStore`, `Clock`, and `UUIDGenerator`.
- `outbox.Worker` depends on `Repository`, `Downstream`, `Clock`, and `Jitter`.
- `postgres.Repository`, `filesystem.Store`, and `downstream.HTTPClient` implement those ports.
- `cmd/api` is the composition root and wires concrete adapters manually; there is no DI container.

This keeps the business flow testable without mocking frameworks and makes the delivery mechanism replaceable later without rewriting the intake use case.

## Requirement-by-requirement implementation

### 01 — Service shell and layout

Configuration comes from environment variables with defaults for operational knobs and explicit failures for required values (`DATABASE_URL`, `DOWNSTREAM_URL`). Invalid numeric or duration overrides also fail at startup rather than silently falling back.

JSON logs are emitted with `slog`. Request IDs are accepted from `X-Request-ID` or generated and returned in the response. The intake handler passes the request ID through the request context into the application call. Logs record metadata such as request ID, delivery key, attempt, delay, status, and errors; they never log the manifest body or frame bytes.

`/healthz` is liveness. `/readyz` pings PostgreSQL and returns `503` when the database cannot be reached, so readiness reflects the actual dependency.

`http.Server.Shutdown` closes listeners first and waits for active HTTP requests to finish. The worker receives the shutdown signal between delivery iterations: it stops claiming new rows, while the current delivery is allowed to finish inside its bounded lease context.

### 02 — Schema, migrations and SQL

Migrations are plain `.sql` files embedded with `go:embed`, sorted lexicographically, and recorded in `schema_migrations`. A process-wide startup lock prevents multiple replicas from applying a migration simultaneously. Re-running startup sees the recorded version and skips it.

SQL is hand-written through pgx. There is no ORM and no query builder.

The money path is:

`JSON string -> domain.Decimal (math/big.Int + explicit scale) -> SQL text parameter cast to NUMERIC -> PostgreSQL NUMERIC(38,12) -> decimal text -> domain.Decimal -> JSON string`.

There is no float type anywhere in that path.

Database constraints include:

- unique `captures.idempotency_key`
- unique `captures.image_key`
- `outbox.capture_id` unique, so a capture has one outbox row
- unique `outbox.delivery_key`
- currency is exactly three uppercase ASCII letters
- non-empty idempotency/image/delivery keys
- outbox status is one of `pending`, `processing`, `delivered`, `dead`
- attempts are non-negative
- money is non-negative and stored as `NUMERIC(38,12)`

### 03 — Intake endpoint

`POST /v1/captures` requires `multipart/form-data` with a `manifest` part and a `frame` part, plus `Idempotency-Key`.

The handler uses `http.MaxBytesReader` before reading the multipart body. It additionally caps the image part to the configured image limit. The manifest is bounded separately and is decoded with `DisallowUnknownFields`.

Required manifest fields are:

- `capturedAt`: RFC3339 timestamp
- `amount`: decimal string
- `currency`: three uppercase ASCII letters

The frame is checked for a non-empty body and an `image/*` content signature using `http.DetectContentType`.

### 04 — Idempotency under concurrency

The permanent invariant is the database unique constraint on `captures.idempotency_key`. The operation also uses a transaction-scoped `pg_advisory_xact_lock` computed from the key, with `READ COMMITTED` isolation.

Why both?

- The unique constraint is the durable invariant even if a future code path forgets the lock.
- The advisory lock makes the application flow deterministic: only one request for the same key can perform the check -> image write -> capture insert -> outbox insert sequence at a time.
- The lock is PostgreSQL-backed, so two replicas behind a load balancer coordinate through the same database. An in-process mutex or map cannot provide this property.

When the second request acquires the lock it sees the committed row, returns the original capture, and never writes another image or outbox row.

The image filename is a SHA-256-derived deterministic name from the idempotency key, so the same key maps to the same final storage object without exposing the raw key in the filesystem.

The integration test starts two real HTTP requests concurrently against the same PostgreSQL instance and asserts one capture row, one outbox row, one image, one original result, and one duplicate result.

### 05 — Outbox and worker

The capture and outbox row are committed together in the same PostgreSQL transaction. This is the durability boundary that prevents the "DB committed, process died before enqueue" gap.

The worker runs independently from the request path. It claims rows using:

`SELECT ... FOR UPDATE SKIP LOCKED` + lease fields.

Claiming changes the row to `processing`, increments `attempts`, and sets `locked_until`. A second worker therefore skips the already claimed row. Expired processing leases are eligible again after a restart.

The delivery call is made outside the claim transaction. On success the row becomes `delivered`. On failure it returns to `pending` with `next_attempt_at`. After the configured maximum attempt count it becomes `dead` and keeps `last_error` for inspection.

Retry delay is exponential with a ceiling and injected jitter. The clock is an injected interface, so the schedule is unit-tested without sleeping.

Delivery is explicitly at-least-once. The worker sends a stable `X-Delivery-Key` such as `capture:<capture UUID>`. If the process dies after the downstream accepts the request but before `delivered` is persisted, the row can be delivered again later. That is the expected at-least-once ambiguity; the downstream contract must deduplicate on the stable key. The provided stub does exactly that.

The lease is configured longer than the downstream client timeout, which prevents a normal request timeout from overlapping with a second claim for the same row.

Manual dead-letter re-drive is intentionally a DB operation rather than an admin API, because an admin surface is outside the required scope. Example:

```sql
UPDATE outbox
SET status='pending', attempts=0, next_attempt_at=now(), locked_until=NULL, last_error=NULL
WHERE id='...'
  AND status='dead';
```

### 06 — Tests

Tests use only the standard library testing stack plus `net/http/httptest`. Fakes are hand-written behind the application-owned interfaces.

The unit suite covers:

- exact decimal parsing and JSON round-trip
- JSON number rejection for money
- idempotency result behavior in the application service
- exponential retry + jitter + ceiling
- successful worker delivery
- retry scheduling without sleeping
- terminal `dead` transition
- required configuration / invalid configuration
- HTTP error contract for missing idempotency key
- HTTP 422 contract for invalid manifest
- successful HTTP capture and request ID response

The PostgreSQL integration test is intentionally separate and uses the real database from Compose. It proves the concurrent idempotency race and checks the persisted decimal representation.

Run the normal unit suite:

```bash
go test ./...
```

Run the PostgreSQL integration test after Compose is running:

```bash
DATABASE_URL='postgres://capture:capture@localhost:5432/captures?sslmode=disable' \
  go test -tags=integration ./internal/capture/adapters/postgres ./internal/capture/application/outbox -count=1
```

Run vet:

```bash
go vet ./...
```

### Error contract

Every error is encoded as:

```json
{
  "message": "...",
  "code": 422003,
  "errorClass": "ManifestInvalid",
  "httpCode": 422
}
```

Defined codes:

| HTTP | Code | errorClass | When |
|---|---:|---|---|
| 400 | 400001 | IdempotencyKeyMissing | Header missing |
| 400 | 400002 | IdempotencyKeyInvalid | Key shape/length invalid |
| 400 | 400003 | MultipartMalformed | Invalid multipart body |
| 404 | 404001 | CaptureNotFound | GET key does not exist |
| 413 | 413001 | RequestTooLarge | Whole request over limit |
| 413 | 413002 | ImageTooLarge | Frame over image limit |
| 422 | 422001 | ManifestMissing | Manifest part missing |
| 422 | 422002 | ImageInvalid | Empty or non-image frame |
| 422 | 422003 | ManifestInvalid | Manifest JSON/schema invalid |
| 422 | 422004 | FrameMissing | Frame part missing |
| 503 | 503001 | CaptureStorageUnavailable | DB or image persistence unavailable |

The first three digits of every code match the HTTP status.

### POST response semantics

`201` means this request created the capture.

`200` with `duplicate: true` means the key already existed and the original stored result is returned. Both are client-success responses.

The implementation never returns `409` for an idempotency race.

## Operational demo of recovery

To demonstrate the delivery retry path without changing code, start Compose with a delayed downstream:

```bash
docker compose down -v
STUB_DELAY_MS=5000 docker compose up --build
```

Create a capture and watch the API logs. Once the delivery begins, kill the API container with SIGKILL:

```bash
docker compose kill -s SIGKILL api
```

Bring it back:

```bash
docker compose up -d api
```

The processing row becomes claimable after its lease and the worker delivers it again. The stable delivery key allows the downstream to deduplicate if the first attempt had already been accepted.

To demonstrate retries, set `STUB_FAIL_FIRST_N` in `compose.yaml` to a small value such as `2`; the worker will schedule retries using the exponential policy.
