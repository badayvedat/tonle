# Production Runbooks

These runbooks assume a Fly.io deployment, Prometheus scraping `/metrics`, and static
tokens stored as hashes in `TONLE_AUTH_TOKENS`. Replace placeholder app, Redis, and owner
names with your production values before going live.

## Alert Owners

Initial owner: `platform-oncall`

Prometheus alert rules live in [monitoring/prometheus-alerts.yml](monitoring/prometheus-alerts.yml).
The Grafana dashboard lives in [monitoring/grafana-dashboard.json](monitoring/grafana-dashboard.json).

## Redis Outage

Symptoms:

- `/readyz` returns `503`.
- `TonleRedisUnavailable`, `TonleRedisErrors`, or `TonleSustained5xx` fires.
- Logs contain `event="readiness_failed"` or `event="redis_error"`.

Impact:

- Writes, reads, stream deletion, and stream info requests may fail.
- Existing SSE listeners may receive keep-alives but no new events while Redis is unavailable.
- Recent events may be unavailable until Redis recovers.

First checks:

- `fly status -a <app>`
- `fly checks list -a <app>`
- `fly logs -a <app>`
- Redis provider status and Redis metrics for connection failures, memory pressure, and
  failover events.
- Dashboard panels: Redis Health, Error Rate, Request Rate.

Mitigation:

- If Redis is down or failing over, wait for recovery and keep `tonle` instances running so
  readiness recovers automatically.
- If only one machine is affected, restart it with `fly machine restart <machine-id> -a <app>`.
- If Redis credentials changed, update `REDIS_URL` with `fly secrets set REDIS_URL=...` and
  redeploy or restart machines.
- If clients are falling behind after recovery, instruct upstream applications to refresh
  authoritative snapshots and reconnect with fresh cursors or tickets.

Rollback:

- Roll back the latest app release if Redis failures started immediately after a deploy:
  `fly releases -a <app>` then `fly deploy --image <previous-image> -a <app>`.
- If a bad Redis secret was deployed, restore the previous `REDIS_URL` secret and restart
  the app.

## Token Compromise

Symptoms:

- Unexpected traffic from a known principal.
- High `tonle_auth_denials_total` or suspicious successful access logs.
- A static token is posted, logged by another service, committed, or reported leaked.

Impact:

- Leaked writer tokens can publish events for their allowed prefixes.
- Leaked reader tokens can read events for their allowed prefixes.
- Leaked admin tokens can delete streams for their allowed prefixes.
- Leaked metrics tokens expose operational traffic and tenant activity signals.

First checks:

- Identify token name, scopes, prefixes, owner, and callers from the token registry.
- Search logs by `principal`, `route`, `method`, and `stream_label`.
- Check dashboard panels: Request Rate, Operational Rejections, Error Rate.
- Confirm no untrusted client received a static token.

Mitigation:

- Generate a replacement high-entropy token.
- Add its SHA-256 hash as a new `TONLE_AUTH_TOKENS` entry with the same intended scopes and
  prefixes.
- Move trusted callers to the replacement token.
- Remove the compromised token entry and redeploy. For emergency revocation, remove it
  immediately and accept caller failures until they are updated.
- Rotate any upstream secret stores that copied the plaintext token.

Rollback:

- Do not restore a compromised token.
- If the new token entry has incorrect scopes or prefixes, deploy a corrected
  `TONLE_AUTH_TOKENS` value.
- If callers break after emergency revocation, issue a new scoped token instead of
  re-enabling the old token.

## High SSE Connection Count

Symptoms:

- `TonleSseConnectionSaturation` or `TonleSseConnectionLimitRejections` fires.
- `tonle_sse_connections_active` is near `TONLE_MAX_ACTIVE_SSE_CONNECTIONS`.
- Clients report `429 Too Many Requests` on SSE routes.

Impact:

- New SSE listeners may be rejected.
- Existing listeners may still receive events, but reconnect storms can increase load.
- Fly request concurrency may saturate before application limits reject connections.

First checks:

- Dashboard panels: Active Listeners, Operational Rejections, Request Rate.
- Logs for `event="connection_limit_rejected"` and `kind="sse"`.
- Recent deploys, traffic spikes, client reconnect behavior, and gateway changes.
- Current app settings for `TONLE_MAX_ACTIVE_SSE_CONNECTIONS` and
  `TONLE_MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM`.

Mitigation:

- Identify whether growth is expected load, one hot stream, or a reconnect storm.
- For a hot stream, move users to backend fan-out or a gateway broker if possible.
- Temporarily raise SSE limits only if Redis and CPU headroom are healthy.
- Scale Fly machines cautiously after running the load/soak test; process-local limits
  multiply by machine count.
- Reduce client reconnect aggression if callers are retrying immediately.

Rollback:

- Restore previous SSE limit settings if raising limits causes higher error rates or Redis
  failures.
- Roll back the latest client or gateway deploy if it caused a reconnect storm.

## High Trim-Gap Rate

Symptoms:

- `TonleHighTrimGapRate` fires.
- Logs contain `event="trim_gap"`.
- Clients receive `StreamTrimmedError` or SSE `event: error` with `type: stream_trimmed`.

Impact:

- Falling-behind consumers cannot replay all missed events from Redis.
- Clients must fetch authoritative snapshots from the main application and reconnect.

First checks:

- Dashboard panels: Operational Rejections, Request Rate, Redis Health.
- Current `STREAM_TTL` and `STREAM_MAXLEN`.
- Recent increases in event rate, payload size, listener count, or consumer downtime.
- Logs grouped by `stream_label` to identify hot tenants or streams.

Mitigation:

- Confirm clients follow trim-gap recovery by fetching a fresh snapshot and reconnecting.
- Increase `STREAM_TTL` or `STREAM_MAXLEN` if Redis memory headroom allows it.
- Reduce event volume or batch noisy producers.
- For audit or exact historical replay requirements, move that use case to a durable event
  store outside Redis.

Rollback:

- Restore previous retention settings if increased retention creates Redis memory pressure.
- Roll back a noisy producer deploy if trim gaps started after a release.

## Fly Deploy Rollback

Symptoms:

- 5xx rate, readiness failures, or client errors begin immediately after a Fly deploy.
- Logs show new exception classes or startup configuration failures.
- `/healthz` passes but `/readyz` or stream routes fail.

Impact:

- New requests may fail or return incorrect errors.
- Existing SSE listeners may disconnect and reconnect to unhealthy machines.

First checks:

- `fly releases -a <app>`
- `fly status -a <app>`
- `fly checks list -a <app>`
- `fly logs -a <app>`
- Dashboard panels: Error Rate, Redis Health, Active Listeners.

Mitigation:

- If the release is clearly bad, redeploy the previous image from `fly releases`.
- If the issue is bad environment configuration, correct secrets or env vars and redeploy.
- Keep customer-facing applications ready to refresh snapshots after reconnects.

Rollback:

- `fly releases -a <app>`
- `fly deploy --image <previous-image> -a <app>`
- Confirm `/healthz`, `/readyz`, `/metrics`, one write, and one read.
- Leave the bad release documented with cause, timestamps, and follow-up issue.

## Read-Ticket Secret Rotation

Symptoms:

- Planned rotation window.
- Suspected read-ticket signing secret exposure.
- Clients unexpectedly receive `401 invalid bearer token` after a secret change.

Impact:

- Tickets signed by removed secrets stop working immediately.
- Public listeners using short-lived tickets may reconnect and request fresh tickets from
  the control plane.

First checks:

- Confirm the active signing secret used by the control plane.
- Confirm `TONLE_READ_TICKET_SECRET` and `TONLE_READ_TICKET_PREVIOUS_SECRETS` in the app
  deployment plan.
- Check ticket maximum lifetime and `TONLE_READ_TICKET_LEEWAY_SECONDS`.
- Check logs for auth denials on read routes.

Mitigation:

- Planned rotation: set the new active secret in the control plane and `tonle`, then place
  the old secret in `TONLE_READ_TICKET_PREVIOUS_SECRETS`.
- Keep previous secrets only until maximum ticket lifetime plus clock leeway has elapsed.
- Emergency rotation: remove the exposed secret from all verification config immediately,
  deploy, and force clients to request fresh tickets.

Rollback:

- If planned rotation breaks valid clients, temporarily add the previous secret back to
  `TONLE_READ_TICKET_PREVIOUS_SECRETS` while fixing the control plane signer.
- Do not restore a secret that is known to be exposed.
- After rollback, verify old and new tickets behave as intended with a read-only stream.
