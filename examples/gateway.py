"""Example control-plane gateway for customer-facing stream access.

This example keeps Redis and raw stream access behind two service layers:

1. workers call this gateway to publish job updates
2. the gateway stores the latest authoritative job snapshot
3. the gateway publishes the same update into tonle
4. customers ask the gateway for a short-lived read ticket
5. customers connect directly to tonle with that ticket

This is intentionally small and uses in-memory job state so the integration points are easy
to see. Replace the customer auth and snapshot storage with your real application logic.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any

import orjson
import uvicorn
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from tonle import AsyncStream, create_read_ticket

TONLE_INTERNAL_BASE_URL = os.getenv("GATEWAY_TONLE_INTERNAL_BASE_URL", "http://127.0.0.1:8000")
TONLE_PUBLIC_BASE_URL = os.getenv("GATEWAY_TONLE_PUBLIC_BASE_URL", TONLE_INTERNAL_BASE_URL)
TONLE_INTERNAL_WRITE_TOKEN = os.getenv("GATEWAY_TONLE_INTERNAL_WRITE_TOKEN")
TONLE_READ_TICKET_SECRET = os.getenv("TONLE_READ_TICKET_SECRET", "dev-read-ticket-secret-00000000000001")
GATEWAY_INTERNAL_TOKEN = os.getenv("GATEWAY_INTERNAL_TOKEN", "gateway-internal-secret-000000000001")
GATEWAY_TICKET_TTL_SECONDS = int(os.getenv("GATEWAY_TICKET_TTL_SECONDS", "300"))
GATEWAY_HOST = os.getenv("GATEWAY_HOST", "127.0.0.1")
GATEWAY_PORT = int(os.getenv("GATEWAY_PORT", "8010"))

# Demo-only authoritative state store.
JOB_SNAPSHOTS: dict[tuple[str, str], dict[str, Any]] = {}


def _json(data: Any, status: int = 200) -> Response:
    return Response(orjson.dumps(data), status_code=status, media_type="application/json")


def _error(detail: str, status: int) -> Response:
    return _json({"detail": detail}, status=status)


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _stream_id(tenant_id: str, job_id: str) -> str:
    return f"tenant:{tenant_id}:job:{job_id}"


def _snapshot_key(tenant_id: str, job_id: str) -> tuple[str, str]:
    return tenant_id, job_id


def _require_internal_token(request: Request) -> None:
    expected = f"Bearer {GATEWAY_INTERNAL_TOKEN}"
    if request.headers.get("authorization") != expected:
        raise PermissionError("missing or invalid internal bearer token")


def _require_customer_access(request: Request, tenant_id: str) -> str:
    # Demo-only stand-in for your real app auth.
    provided_tenant = request.headers.get("x-demo-tenant")
    if not provided_tenant:
        raise PermissionError("missing X-Demo-Tenant header")
    if provided_tenant != tenant_id:
        raise PermissionError("forbidden")
    return request.headers.get("x-demo-customer-id", "demo-customer")


def _merge_snapshot(existing: dict[str, Any] | None, tenant_id: str, job_id: str, update: dict[str, Any]) -> dict[str, Any]:
    snapshot = dict(existing or {})
    snapshot.update(update)
    snapshot["tenant_id"] = tenant_id
    snapshot["job_id"] = job_id
    snapshot["stream_id"] = _stream_id(tenant_id, job_id)
    snapshot["updated_at"] = _utcnow().isoformat()
    if "status" not in snapshot:
        snapshot["status"] = "queued"
    return snapshot


async def _publish_to_tonle(stream_id: str, event: dict[str, Any]) -> None:
    async with AsyncStream(
        stream_id,
        base_url=TONLE_INTERNAL_BASE_URL,
        token=TONLE_INTERNAL_WRITE_TOKEN,
    ) as stream:
        await stream.put(event)


async def get_job_snapshot(request: Request) -> Response:
    tenant_id = request.path_params["tenant_id"]
    job_id = request.path_params["job_id"]
    try:
        _require_customer_access(request, tenant_id)
    except PermissionError as exc:
        message = str(exc)
        status = 403 if message == "forbidden" else 401
        return _error(message, status)

    snapshot = JOB_SNAPSHOTS.get(_snapshot_key(tenant_id, job_id))
    if snapshot is None:
        return _error("job not found", 404)
    return _json(snapshot)


async def issue_stream_ticket(request: Request) -> Response:
    tenant_id = request.path_params["tenant_id"]
    job_id = request.path_params["job_id"]
    try:
        customer_id = _require_customer_access(request, tenant_id)
    except PermissionError as exc:
        message = str(exc)
        status = 403 if message == "forbidden" else 401
        return _error(message, status)

    snapshot = JOB_SNAPSHOTS.get(_snapshot_key(tenant_id, job_id))
    if snapshot is None:
        return _error("job not found", 404)

    stream_id = _stream_id(tenant_id, job_id)
    expires_at = _utcnow() + timedelta(seconds=GATEWAY_TICKET_TTL_SECONDS)
    ticket = create_read_ticket(
        TONLE_READ_TICKET_SECRET,
        stream_id,
        expires_at=expires_at,
        subject=customer_id,
    )
    return _json(
        {
            "stream_id": stream_id,
            "base_url": TONLE_PUBLIC_BASE_URL,
            "token": ticket,
            "expires_at": expires_at.isoformat(),
        }
    )


async def publish_job_event(request: Request) -> Response:
    tenant_id = request.path_params["tenant_id"]
    job_id = request.path_params["job_id"]
    try:
        _require_internal_token(request)
    except PermissionError as exc:
        return _error(str(exc), 401)

    try:
        body = await request.json()
    except Exception:
        return _error("request body must be valid JSON", 400)
    if not isinstance(body, dict):
        return _error("request body must be a JSON object", 400)

    existing = JOB_SNAPSHOTS.get(_snapshot_key(tenant_id, job_id))
    snapshot = _merge_snapshot(existing, tenant_id, job_id, body)
    JOB_SNAPSHOTS[_snapshot_key(tenant_id, job_id)] = snapshot

    event = {
        "kind": "job.snapshot",
        "tenant_id": tenant_id,
        "job_id": job_id,
        "snapshot": snapshot,
    }
    await _publish_to_tonle(_stream_id(tenant_id, job_id), event)
    return _json({"published": True, "snapshot": snapshot}, status=202)


app = Starlette(
    routes=[
        Route("/tenants/{tenant_id}/jobs/{job_id}", get_job_snapshot, methods=["GET"]),
        Route("/tenants/{tenant_id}/jobs/{job_id}/stream-ticket", issue_stream_ticket, methods=["POST"]),
        Route("/internal/tenants/{tenant_id}/jobs/{job_id}/events", publish_job_event, methods=["POST"]),
    ]
)


def main() -> None:
    uvicorn.run(app, host=GATEWAY_HOST, port=GATEWAY_PORT)


if __name__ == "__main__":
    main()
