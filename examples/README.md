# Examples

## Gateway

`gateway.py` is a small control-plane example for the customer-listener flow:

1. an internal worker publishes job updates to the gateway
2. the gateway stores the latest authoritative job snapshot
3. the gateway forwards the update to `tonle`
4. a customer asks the gateway for a short-lived stream ticket
5. the customer listens directly to `tonle` with that ticket

### Start `tonle`

Run `tonle` with a writer token and the same read-ticket secret the gateway will use:

```bash
export TONLE_AUTH_TOKENS='[{"name":"writer","token":"writer-static-token-000000000000001","scopes":["streams:write"],"prefixes":["tenant:"]}]'
export TONLE_READ_TICKET_SECRET='dev-read-ticket-secret-00000000000001'
uv run tonle
```

### Start the gateway

```bash
export GATEWAY_TONLE_INTERNAL_WRITE_TOKEN='writer-static-token-000000000000001'
export TONLE_READ_TICKET_SECRET='dev-read-ticket-secret-00000000000001'
./.venv/bin/python examples/gateway.py
```

### Publish a worker update

```bash
./.venv/bin/python examples/publish_job_update.py 10 running "started"
./.venv/bin/python examples/publish_job_update.py 50 running "halfway"
./.venv/bin/python examples/publish_job_update.py 100 done "finished"
```

### Listen as a customer

In another shell:

```bash
./.venv/bin/python examples/listen_job.py
```

The listener first fetches the latest snapshot from the gateway, then requests a short-lived
read ticket, then connects directly to `tonle` with the returned `base_url`, `stream_id`, and
bearer token.

## Load / Soak

Use `load_soak.py` to exercise direct fan-out against `tonle`.

SSE example:

```bash
./.venv/bin/python examples/load_soak.py \
  --mode sse \
  --base-url http://127.0.0.1:8000 \
  --streams 20 \
  --listeners-per-stream 10 \
  --events-per-stream 200 \
  --batch-size 10 \
  --stream-prefix load:
```

Polling example:

```bash
./.venv/bin/python examples/load_soak.py \
  --mode poll \
  --base-url http://127.0.0.1:8000 \
  --streams 10 \
  --listeners-per-stream 5 \
  --events-per-stream 100
```

The script prints a JSON summary with:

- expected vs actual publish/receive counts
- publish and receive throughput
- sampled `p50` / `p95` / `p99` latency
- trim-gap and listener error counts
