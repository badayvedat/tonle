# tonle

`tonle` is a small Redis-backed event stream service with a Python client.

It exposes stream operations over HTTP, supports long-poll consumers and Server-Sent Events
(SSE), includes batching helpers so producers can avoid one-request-per-event overhead, and
ships with basic production hooks such as auth, health/readiness checks, payload limits,
trim-gap detection, and Prometheus-style metrics.

## Requirements

- Python 3.13+
- Redis

## Install

With `uv`:

```bash
uv sync
```

For development tools:

```bash
uv sync --group dev
```

## Python Client

### Sync

```python
from tonle import Stream, StreamTrimmedError

with Stream("demo") as stream:
    stream.put({"n": 1})
    stream.put_many([{"n": 2}, {"n": 3}])

    try:
        for event in stream.events():
            print(event["id"], event["data"])
            break
    except StreamTrimmedError as exc:
        print("resume cursor was trimmed", exc.last_id, exc.first_available_id)
```

Buffered producer:

```python
from tonle import Stream

with Stream("demo") as stream, stream.buffered(max_batch_size=100) as writer:
    for n in range(1000):
        writer.put({"n": n})
```

Authenticated listener:

```python
from datetime import datetime, timedelta, timezone

from tonle import Stream, create_read_ticket

stream_id = "tenant123:job:456"
ticket = create_read_ticket(
    "replace-with-your-shared-secret",
    stream_id,
    expires_at=datetime.now(timezone.utc) + timedelta(minutes=5),
    subject="customer-123",
)

with Stream(stream_id, base_url="https://streams.example.com", token=ticket) as stream:
    for event in stream.events():
        print(event["id"], event["data"])
        break
```

### Async

```python
from tonle import AsyncStream

async with AsyncStream("demo") as stream:
    await stream.put({"n": 1})
    async for event in stream.events():
        print(event["id"], event["data"])
        break
```

### Production Client Behavior

The client accepts either one shared `timeout` value or separate
`connect_timeout`, `read_timeout`, `write_timeout`, and `pool_timeout` values:

```python
from tonle import Stream

stream = Stream(
    "tenant123:job:456",
    base_url="https://streams.example.com",
    token="backend-minted-token-or-read-ticket",
    connect_timeout=5.0,
    read_timeout=65.0,
    write_timeout=10.0,
    pool_timeout=5.0,
    read_retries=2,
    retry_backoff=0.25,
)
```

`events()` uses SSE by default. Use `events(transport="long-poll")` only for clients or
networks that cannot hold SSE connections. Reads retry transient transport failures and
`5xx` responses before surfacing an exception. For SSE, the retry budget applies to each
reconnect attempt, not the whole lifetime of the listener. `put()`, `put_many()`, and
`delete()` do not retry automatically because writes may not be safe to replay unless your
application supplies its own idempotency.

Specific HTTP errors are raised as typed exceptions: `StreamUnauthorizedError` for `401`,
`StreamForbiddenError` for `403`, `StreamPayloadTooLargeError` for `413`,
`StreamRateLimitedError` for `429`, and `StreamServerError` for `5xx`. Other HTTP errors
raise `StreamHTTPError`. Transient network failures are raised as `StreamTransportError`.

## Run the Server

By default the server listens on `0.0.0.0:8000` and connects to `redis://localhost:6379`.

```bash
uv run tonle
```

Useful environment variables:

- `REDIS_URL`: Redis connection string
- `TONLE_HOST`: bind host
- `TONLE_PORT`: bind port
- `TONLE_RELOAD`: set to `1`/`true`/`yes` to enable reload during development
- `TONLE_AUTH_TOKENS`: JSON array of bearer-token definitions
- `TONLE_READ_TICKET_SECRET`: shared secret for signed exact-stream read tickets
- `TONLE_READ_TICKET_PREVIOUS_SECRETS`: JSON array of previous read-ticket secrets accepted during rotation
- `TONLE_READ_TICKET_LEEWAY_SECONDS`: optional clock-skew leeway for read-ticket expiry checks
- `TONLE_REQUIRE_AUTH`: set to `1`/`true`/`yes` to fail startup when no auth mode is configured
- `TONLE_MAX_REQUEST_BYTES`: max request body size in bytes
- `TONLE_REQUEST_BODY_READ_TIMEOUT_SECONDS`: max seconds to wait for the next request body chunk
- `TONLE_MAX_EVENT_BYTES`: max encoded event size in bytes
- `TONLE_SSE_POLL_INTERVAL`: max seconds between SSE disconnect checks / keep-alives
- `TONLE_MAX_ACTIVE_SSE_CONNECTIONS`: max active SSE responses per process
- `TONLE_MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM`: max active SSE responses per stream per process
- `TONLE_MAX_ACTIVE_POLLS`: max active long-poll requests per process
- `TONLE_MAX_ACTIVE_POLLS_PER_STREAM`: max active long-poll requests per stream per process
- `TONLE_RATE_LIMIT_WINDOW_SECONDS`: fixed-window rate-limit duration in seconds
- `TONLE_RATE_LIMIT_REQUESTS_PER_WINDOW`: max stream-route requests per principal/IP per window; `0` disables
- `TONLE_RATE_LIMIT_WRITES_PER_WINDOW`: max write/delete requests per principal/IP per window; `0` disables
- `TONLE_RATE_LIMIT_AUTH_FAILURES_PER_WINDOW`: max auth failures per client IP per window; `0` disables
- `TONLE_TRUST_FORWARDED_HEADERS`: set to `1`/`true`/`yes` only behind a trusted proxy that sanitizes `X-Forwarded-For`
- `TONLE_TENANT_QUOTAS`: optional JSON object for Redis-backed tenant request/write/byte quotas
- `STREAM_TTL`: Redis key TTL in seconds
- `STREAM_MAXLEN`: approximate Redis stream max length

## Auth

Auth is optional. If both `TONLE_AUTH_TOKENS` and `TONLE_READ_TICKET_SECRET` are unset, the
API is open.

Set `TONLE_REQUIRE_AUTH=true` in production to fail startup when neither auth mode is
configured. This is a configuration guard, not route-level authorization; for example,
`/metrics` remains public only when no auth mode is configured. When auth is configured,
`/metrics` requires a token with `metrics:read`.

When either auth mode is enabled, every request must send:

```http
Authorization: Bearer <token>
```

`TONLE_AUTH_TOKENS` is intended for static internal service tokens. Token definitions are
provided as JSON. Plaintext `token` values must be at least 32 characters; `token_sha256`
values must be SHA-256 hashes of high-entropy tokens.

```json
[
  {
    "name": "reader",
    "token": "reader-static-token-000000000000001",
    "scopes": ["streams:read"]
  },
  {
    "name": "writer",
    "token_sha256": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    "scopes": ["streams:write"],
    "prefixes": ["tenant-a:"]
  }
]
```

Supported scopes:

- `streams:read`
- `streams:write`
- `streams:delete`
- `metrics:read`

If `prefixes` is set, the token may only access stream IDs that start with one of those
prefixes. Prefix entries must end with a stream-ID delimiter (`:`, `_`, or `-`) so
`tenant-a:` cannot accidentally authorize `tenant-ab:...`. Prefixes apply to stream routes
only; they do not narrow the global `/metrics` view for tokens with `metrics:read`.

`TONLE_READ_TICKET_SECRET` enables exact-stream signed read tickets for customer listeners.
The signing secret must be at least 32 characters. These tickets are bearer tokens signed
with HMAC-SHA256 and must contain:

- `scope`: must be `streams:read`
- `stream_id`: exact stream ID the ticket may read
- `exp`: Unix timestamp expiry

Use `create_read_ticket(...)` from the Python package in your app/control plane to mint a
ticket, then pass that ticket to the Python client with `Stream(..., token=ticket)` or
`AsyncStream(..., token=ticket)`.

Read tickets are only valid for read routes and only for their exact `stream_id`. They are a
better fit for many short-lived customer listeners than adding one static env token per
customer.

For planned read-ticket secret rotation, update ticket minting to use the new secret, set
`TONLE_READ_TICKET_SECRET` to that new active secret, and keep the old secret in
`TONLE_READ_TICKET_PREVIOUS_SECRETS` as a JSON array until the maximum ticket lifetime plus
`TONLE_READ_TICKET_LEEWAY_SECONDS` has passed. Then remove the old secret and redeploy.

Route policy:

- `POST /streams/{stream_id}/events`: `streams:write`
- `POST /streams/{stream_id}/events/batch`: `streams:write`
- `GET /streams/{stream_id}/events`: `streams:read`
- `GET /streams/{stream_id}/events/sse`: `streams:read`
- `GET /streams/{stream_id}`: `streams:read`
- `DELETE /streams/{stream_id}`: `streams:delete`

Operational endpoints:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`: open only when auth is disabled; otherwise requires `metrics:read`

## HTTP API

### Push one event

```http
POST /streams/{stream_id}/events
Content-Type: application/json

{"data":{"message":"hello"}}
```

Response:

```json
{"id":"1714000000-0"}
```

### Push many events

```http
POST /streams/{stream_id}/events/batch
Content-Type: application/json

{"events":[{"message":"a"},{"message":"b"}]}
```

Response:

```json
{"ids":["1714000000-0","1714000000-1"]}
```

### Poll for events

```http
GET /streams/{stream_id}/events?last_id=0&limit=100&timeout=30
```

Response:

```json
{
  "events": [
    {"id":"1714000000-0","data":{"message":"hello"}}
  ],
  "last_id":"1714000000-0"
}
```

If no events are available before the timeout, the server returns `204 No Content`.

If the requested `last_id` has already been trimmed from the stream, the server returns
`409 Conflict`:

```json
{
  "detail": "requested last_id has been trimmed from the stream",
  "last_id": "1714000000-0",
  "first_available_id": "1714000100-0"
}
```

### Stream via SSE

```http
GET /streams/{stream_id}/events/sse?timeout=30
Last-Event-ID: 1714000000-0
```

If an SSE consumer falls behind and its cursor has been trimmed, the server emits a final
`gap` event before closing the stream. The Python client turns both poll-time `409` responses
and SSE `gap` events into `StreamTrimmedError`.

SSE responses include keep-alive comments while no events are available. The Python client
automatically reconnects after an SSE response ends and resumes with the last event ID it
received.

For customer-facing consumers, treat `StreamTrimmedError` as a recovery signal:

1. fetch the authoritative current snapshot for the resource from your main app
2. replace local state with that snapshot
3. open a fresh stream with a new read ticket if the previous one expired

### Retention Contract

Events are retained for up to `STREAM_TTL` seconds or approximately `STREAM_MAXLEN`
entries per stream, whichever limit is reached first. Redis is the recent-event transport,
not the durable source of truth.

Consumers that need to recover from a trim gap must fetch the authoritative current
snapshot from the main application, replace local state with that snapshot, and reconnect
from a fresh cursor or read ticket. If your product needs audit logs, exact historical
delivery, or durable replay, store events outside `tonle` in a durable event store.

### Stream info

```http
GET /streams/{stream_id}
```

### Delete a stream

```http
DELETE /streams/{stream_id}
```

### Health and readiness

```http
GET /healthz
GET /readyz
```

`/healthz` reports process liveness. `/readyz` also checks Redis connectivity and returns
`503` if the backend is unavailable.

Stream routes can return `429 Too Many Requests` with `Retry-After` when active connection
limits or rate limits are reached.

### Metrics

```http
GET /metrics
```

The service exposes Prometheus-style counters and gauges for request volume, request latency,
auth denials, payload rejections, trim-gap detections, Redis/SSE failures, readiness failures,
active SSE connections, active long-poll requests, connection-limit rejections, and
rate-limit rejections. If `TONLE_TENANT_QUOTAS` is configured, `/metrics` also includes
tenant quota rejections without tenant labels.

Production monitoring starter files are included:

- [monitoring/grafana-dashboard.json](monitoring/grafana-dashboard.json)
- [monitoring/prometheus-alerts.yml](monitoring/prometheus-alerts.yml)
- [RUNBOOKS.md](RUNBOOKS.md)

### Logs

Operational log messages contain a structured JSON object. If no application logging handler
exists, `tonle` configures basic logging at `INFO` with `%(message)s`, so operational events
are emitted as bare JSON lines. Logs include an event name, route, method, status code when
available, principal name when available, stream label when available, and error class for
failures.

SSE trim-gap logs use `event="trim_gap"` with `status_code=200` because the SSE response has
already started; poll trim gaps use the same event name with HTTP `409`.

Logs intentionally exclude bearer tokens, read-ticket contents, Redis credentials, and
request payloads.

## Gateway Example

See [examples/README.md](examples/README.md) for a small control-plane `gateway` server that:

- accepts internal worker job updates
- stores the latest authoritative snapshot
- issues short-lived exact-stream read tickets
- lets a customer Python client listen directly to `tonle`

## Testing

Install the dev group and run the full test suite in parallel:

```bash
uv sync --group dev
uv run pytest -n auto
```

The suite includes:

- unit tests for API validation, Redis store behavior, and client helpers
- integration tests that exercise the real ASGI app against a live Redis instance

The integration tests will try to start a temporary `redis-server` automatically. If the
binary is unavailable or local process binding is restricted, those tests skip cleanly.
