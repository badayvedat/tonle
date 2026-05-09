import asyncio
import logging
import re
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import orjson
import redis.asyncio as aioredis
import uvicorn
from redis.exceptions import RedisError
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import Response, StreamingResponse
from starlette.routing import Route

from .auth import AuthError, authorize_global_request, authorize_request, get_authenticator
from .config import (
    HOST,
    MAX_ACTIVE_POLLS,
    MAX_ACTIVE_POLLS_PER_STREAM,
    MAX_ACTIVE_SSE_CONNECTIONS,
    MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM,
    MAX_EVENT_BYTES,
    MAX_REQUEST_BYTES,
    REQUEST_BODY_READ_TIMEOUT_SECONDS,
    PORT,
    RATE_LIMIT_AUTH_FAILURES_PER_WINDOW,
    RATE_LIMIT_REQUESTS_PER_WINDOW,
    RATE_LIMIT_WINDOW_SECONDS,
    RATE_LIMIT_WRITES_PER_WINDOW,
    REDIS_URL,
    RELOAD,
    SSE_POLL_INTERVAL,
    STREAM_MAXLEN,
    STREAM_TTL,
    TRUST_FORWARDED_HEADERS,
)
from .metrics import MetricsMiddleware, app_metrics
from .quotas import (
    ConnectionLease,
    RedisConnectionLeaseLimiter,
    RedisTenantQuotaLimiter,
    TenantQuotaExceeded,
    get_tenant_quota_config,
    tenant_from_stream_id,
)
from .store import InvalidStreamCursor, Store, TrimmedStreamCursor, validate_stream_cursor

logger = logging.getLogger(__name__)
store: Store
_MAX_LIMIT = 1000
_MAX_TIMEOUT = 60
_MAX_BATCH_EVENTS = 1000
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9:_-]{1,128}$")


class ConnectionLimitExceeded(Exception):
    def __init__(self, detail: str, kind: str):
        super().__init__(detail)
        self.detail = detail
        self.kind = kind


class RateLimitExceeded(Exception):
    def __init__(self, detail: str, retry_after: int):
        super().__init__(detail)
        self.detail = detail
        self.retry_after = retry_after


# These process-local limiters are mutated without awaits under the normal single-threaded
# asyncio server model. Use a lock if the app is ever served from multiple threads.
class ConnectionLimiter:
    def __init__(self, *, max_total: int, max_per_stream: int, kind: str):
        self.max_total = max_total
        self.max_per_stream = max_per_stream
        self.kind = kind
        self._total = 0
        self._by_stream: dict[str, int] = {}

    def acquire(self, stream_id: str) -> None:
        if self._total >= self.max_total:
            raise ConnectionLimitExceeded(f"too many active {self.kind} connections", self.kind)
        stream_total = self._by_stream.get(stream_id, 0)
        if stream_total >= self.max_per_stream:
            raise ConnectionLimitExceeded(f"too many active {self.kind} connections for stream", self.kind)
        self._total += 1
        self._by_stream[stream_id] = stream_total + 1

    def release(self, stream_id: str) -> None:
        if self._total > 0:
            self._total -= 1
        stream_total = self._by_stream.get(stream_id, 0)
        if stream_total <= 1:
            self._by_stream.pop(stream_id, None)
        else:
            self._by_stream[stream_id] = stream_total - 1

    def reset(self) -> None:
        self._total = 0
        self._by_stream = {}

    @property
    def active_total(self) -> int:
        return self._total


class FixedWindowRateLimiter:
    def __init__(self, *, max_events: int, window_seconds: int, kind: str, clock=time.monotonic):
        self.max_events = max_events
        self.window_seconds = window_seconds
        self.kind = kind
        self._clock = clock
        self._windows: dict[str, tuple[float, int]] = {}

    def check(self, key: str) -> None:
        if self.max_events <= 0:
            return
        now = self._clock()
        # O(N) pruning is acceptable for the expected per-process key count. If this sees
        # large public-IP churn, amortize pruning or move rate limits to Redis.
        self._prune(now)
        window_start, count = self._windows.get(key, (now, 0))
        elapsed = now - window_start
        if count >= self.max_events:
            retry_after = max(1, int(self.window_seconds - elapsed))
            raise RateLimitExceeded(f"too many {self.kind} requests", retry_after)
        self._windows[key] = (window_start, count + 1)

    def _prune(self, now: float) -> None:
        stale_keys = [
            key
            for key, (window_start, _) in self._windows.items()
            if now - window_start >= self.window_seconds
        ]
        for key in stale_keys:
            del self._windows[key]

    def reset(self) -> None:
        self._windows = {}

    @property
    def key_count(self) -> int:
        return len(self._windows)


sse_limiter = ConnectionLimiter(
    max_total=MAX_ACTIVE_SSE_CONNECTIONS,
    max_per_stream=MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM,
    kind="sse",
)
poll_limiter = ConnectionLimiter(
    max_total=MAX_ACTIVE_POLLS,
    max_per_stream=MAX_ACTIVE_POLLS_PER_STREAM,
    kind="long-poll",
)
request_rate_limiter = FixedWindowRateLimiter(
    max_events=RATE_LIMIT_REQUESTS_PER_WINDOW,
    window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    kind="stream",
)
write_rate_limiter = FixedWindowRateLimiter(
    max_events=RATE_LIMIT_WRITES_PER_WINDOW,
    window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    kind="write",
)
auth_failure_rate_limiter = FixedWindowRateLimiter(
    max_events=RATE_LIMIT_AUTH_FAILURES_PER_WINDOW,
    window_seconds=RATE_LIMIT_WINDOW_SECONDS,
    kind="auth-failure",
)


class PayloadTooLargeError(ValueError):
    pass


class RequestBodyTimeoutError(ValueError):
    pass


def _structured_log(level: int, event: str, **fields: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": logging.getLevelName(level),
        "event": event,
        **fields,
    }
    logger.log(level, orjson.dumps(record).decode())


def configure_logging() -> None:
    logger.setLevel(logging.INFO)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(message)s")


def _stream_label(stream_id: str | None) -> str | None:
    if stream_id is None:
        return None
    return tenant_from_stream_id(stream_id)


def _request_log_fields(request: Request | None = None, stream_id: str | None = None) -> dict[str, Any]:
    if request is None:
        return {"stream": stream_id, "stream_label": _stream_label(stream_id)}
    principal = getattr(request.state, "principal", None)
    return {
        "method": request.method,
        "route": request.url.path,
        "principal": getattr(principal, "name", None),
        "stream": stream_id,
        "stream_label": _stream_label(stream_id),
        "client": _client_identity(request),
    }


def _json(data: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    return Response(
        orjson.dumps(data),
        status_code=status,
        media_type="application/json",
        headers=headers,
    )


def _error(detail: str, status: int = 400, headers: dict[str, str] | None = None) -> Response:
    return _json({"detail": detail}, status=status, headers=headers)


def _auth_error(exc: AuthError, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_auth_denial()
    _structured_log(
        logging.WARNING,
        "auth_denied",
        **_request_log_fields(request, stream_id),
        status_code=exc.status_code,
        error=exc.detail,
    )
    return _error(exc.detail, status=exc.status_code, headers=exc.headers)


def _payload_too_large(exc: PayloadTooLargeError, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_payload_too_large()
    _structured_log(
        logging.WARNING,
        "payload_rejected",
        **_request_log_fields(request, stream_id),
        status_code=413,
        error=str(exc),
    )
    return _error(str(exc), status=413)


def _request_body_timeout(exc: RequestBodyTimeoutError, request: Request | None = None, stream_id: str | None = None) -> Response:
    _structured_log(
        logging.WARNING,
        "request_body_timeout",
        **_request_log_fields(request, stream_id),
        status_code=408,
        error=str(exc),
    )
    return _error(str(exc), status=408)


def _connection_limit_error(exc: ConnectionLimitExceeded, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_connection_limit_rejection(exc.kind)
    _structured_log(
        logging.WARNING,
        "connection_limit_rejected",
        **_request_log_fields(request, stream_id),
        status_code=429,
        kind=exc.kind,
        error=exc.detail,
    )
    return _error(exc.detail, status=429, headers={"Retry-After": "1"})


def _rate_limit_error(exc: RateLimitExceeded, kind: str, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_rate_limit_rejection(kind)
    _structured_log(
        logging.WARNING,
        "rate_limit_rejected",
        **_request_log_fields(request, stream_id),
        status_code=429,
        kind=kind,
        retry_after=exc.retry_after,
        error=exc.detail,
    )
    return _error(exc.detail, status=429, headers={"Retry-After": str(exc.retry_after)})


def _tenant_quota_error(exc: TenantQuotaExceeded, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_tenant_quota_rejection(exc.quota)
    headers = {"Retry-After": str(exc.retry_after)} if exc.retry_after is not None else None
    _structured_log(
        logging.WARNING,
        "tenant_quota_rejected",
        **_request_log_fields(request, stream_id),
        status_code=exc.status_code,
        quota=exc.quota,
        retry_after=exc.retry_after,
        error=exc.detail,
    )
    return _error(exc.detail, status=exc.status_code, headers=headers)


def _trim_gap(exc: TrimmedStreamCursor, request: Request | None = None, stream_id: str | None = None) -> Response:
    app_metrics.record_trim_gap()
    _structured_log(
        logging.INFO,
        "trim_gap",
        **_request_log_fields(request, stream_id),
        status_code=409,
        last_id=exc.last_id,
        first_available_id=exc.first_available_id,
    )
    return _json(
        {
            "detail": "requested last_id has been trimmed from the stream",
            "last_id": exc.last_id,
            "first_available_id": exc.first_available_id,
        },
        status=409,
    )


def _sse_message(event_id: str, data: dict, *, event_type: str = "message") -> bytes:
    parts = []
    if event_type != "message":
        parts.append(b"event: " + event_type.encode() + b"\n")
    parts.append(b"id: " + event_id.encode() + b"\n")
    parts.append(b"data: " + orjson.dumps(data) + b"\n\n")
    return b"".join(parts)


def _read_stream_id(request: Request) -> str:
    stream_id = request.path_params["stream_id"]
    if not _STREAM_ID_RE.fullmatch(stream_id):
        raise ValueError("stream_id must be 1-128 chars of [A-Za-z0-9:_-]")
    return stream_id


def _client_identity(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for") if TRUST_FORWARDED_HEADERS else None
    if forwarded_for:
        return f"ip:{forwarded_for.split(',')[0].strip()}"
    if request.client is not None:
        return f"ip:{request.client.host}"
    return "ip:unknown"


def _principal_identity(request: Request) -> str:
    principal = getattr(request.state, "principal", None)
    if principal is None:
        # Open-mode deployments have no principal, so IP becomes the rate-limit key.
        return _client_identity(request)
    return f"principal:{principal.name}"


def _check_stream_rate_limit(request: Request) -> None:
    request_rate_limiter.check(_principal_identity(request))


def _check_write_rate_limit(request: Request) -> None:
    write_rate_limiter.check(_principal_identity(request))


def _check_auth_failure_rate_limit(request: Request) -> None:
    auth_failure_rate_limiter.check(_client_identity(request))


async def _check_tenant_quota(
    request: Request,
    stream_id: str,
    quota: str,
    *,
    amount: int = 1,
    policy_limit: str,
) -> None:
    config = get_tenant_quota_config()
    if config is None:
        return
    tenant = tenant_from_stream_id(stream_id)
    policy = config.policy_for(tenant)
    if policy is None:
        return
    limit = getattr(policy, policy_limit)
    try:
        await RedisTenantQuotaLimiter(store.redis).check(
            tenant=tenant,
            quota=quota,
            amount=amount,
            limit=limit,
            window_seconds=policy.window_seconds,
        )
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="tenant_quota_check",
            quota=quota,
        )
        raise


async def _check_tenant_request_quota(request: Request, stream_id: str) -> None:
    await _check_tenant_quota(
        request,
        stream_id,
        "requests",
        policy_limit="requests_per_window",
    )


async def _check_tenant_write_quota(request: Request, stream_id: str) -> None:
    await _check_tenant_quota(
        request,
        stream_id,
        "writes",
        policy_limit="writes_per_window",
    )


async def _check_tenant_write_bytes_quota(request: Request, stream_id: str, byte_count: int) -> None:
    await _check_tenant_quota(
        request,
        stream_id,
        "write-bytes",
        amount=byte_count,
        policy_limit="write_bytes_per_window",
    )


async def _check_tenant_window_quotas(
    request: Request,
    stream_id: str,
    *,
    request_count: int = 0,
    write_count: int = 0,
    write_byte_count: int = 0,
) -> None:
    config = get_tenant_quota_config()
    if config is None:
        return
    tenant = tenant_from_stream_id(stream_id)
    policy = config.policy_for(tenant)
    if policy is None:
        return
    checks = [
        ("requests", request_count, policy.requests_per_window),
        ("writes", write_count, policy.writes_per_window),
        ("write-bytes", write_byte_count, policy.write_bytes_per_window),
    ]
    try:
        await RedisTenantQuotaLimiter(store.redis).check_many(
            tenant=tenant,
            checks=checks,
            window_seconds=policy.window_seconds,
        )
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="tenant_quota_check",
        )
        raise


async def _acquire_connection_lease(stream_id: str, kind: str, request: Request) -> ConnectionLease | None:
    config = get_tenant_quota_config()
    if config is None:
        return None
    tenant = tenant_from_stream_id(stream_id)
    policy = config.policy_for(tenant)
    if policy is None:
        return None
    principal = _principal_identity(request)
    try:
        return await RedisConnectionLeaseLimiter(store.redis).acquire(
            kind=kind,
            tenant=tenant,
            stream_id=stream_id,
            principal=principal,
            policy=policy,
        )
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="connection_lease_acquire",
            kind=kind,
        )
        raise


async def _refresh_connection_lease(lease: ConnectionLease | None, request: Request, stream_id: str, kind: str) -> None:
    if lease is None:
        return
    limiter = RedisConnectionLeaseLimiter(store.redis)
    for attempt in range(2):
        try:
            await limiter.refresh(lease)
            return
        except RedisError as exc:
            app_metrics.record_redis_error()
            _structured_log(
                logging.ERROR,
                "redis_error",
                **_request_log_fields(request, stream_id),
                error_class=exc.__class__.__name__,
                operation="connection_lease_refresh",
                kind=kind,
                attempt=attempt + 1,
            )
            if attempt == 1:
                raise
            await asyncio.sleep(0.1)


async def _release_connection_lease(lease: ConnectionLease | None, request: Request, stream_id: str, kind: str) -> None:
    if lease is None:
        return
    try:
        await RedisConnectionLeaseLimiter(store.redis).release(lease)
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="connection_lease_release",
            kind=kind,
        )


async def _read_json_object(request: Request) -> dict:
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            content_length_value = int(content_length)
        except ValueError:
            content_length_value = None
        if content_length_value is not None and content_length_value > MAX_REQUEST_BYTES:
            raise PayloadTooLargeError(f"request body must be at most {MAX_REQUEST_BYTES} bytes")
    body_chunks = []
    body_size = 0
    stream = request.stream().__aiter__()
    while True:
        try:
            chunk = await asyncio.wait_for(
                anext(stream),
                timeout=REQUEST_BODY_READ_TIMEOUT_SECONDS,
            )
        except StopAsyncIteration:
            break
        except TimeoutError as exc:
            raise RequestBodyTimeoutError("request body read timed out") from exc
        body_size += len(chunk)
        if body_size > MAX_REQUEST_BYTES:
            raise PayloadTooLargeError(f"request body must be at most {MAX_REQUEST_BYTES} bytes")
        body_chunks.append(chunk)
    raw_body = b"".join(body_chunks)
    try:
        body = orjson.loads(raw_body)
    except orjson.JSONDecodeError as exc:
        raise ValueError("request body must be valid JSON") from exc
    if not isinstance(body, dict):
        raise ValueError("request body must be a JSON object")
    return body


def _parse_int_param(raw_value: str, name: str, minimum: int, maximum: int) -> int:
    try:
        value = int(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if not minimum <= value <= maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum}")
    return value


def _read_event(body: dict) -> dict:
    data = body.get("data")
    if not isinstance(data, dict):
        raise ValueError("data must be an object")
    if len(orjson.dumps(data)) > MAX_EVENT_BYTES:
        raise PayloadTooLargeError(f"event payload must be at most {MAX_EVENT_BYTES} bytes")
    return data


def _encoded_event_bytes(event: dict) -> int:
    return len(orjson.dumps(event))


def _read_events(body: dict) -> list[dict]:
    events = body.get("events")
    if not isinstance(events, list):
        raise ValueError("events must be an array of objects")
    if len(events) > _MAX_BATCH_EVENTS:
        raise ValueError(f"events must contain at most {_MAX_BATCH_EVENTS} items")
    if not events:
        raise ValueError("events must contain at least 1 item")
    for event in events:
        if not isinstance(event, dict):
            raise ValueError("events must be an array of objects")
        if len(orjson.dumps(event)) > MAX_EVENT_BYTES:
            raise PayloadTooLargeError(f"event payload must be at most {MAX_EVENT_BYTES} bytes")
    return events


def _check_tenant_payload_quotas(stream_id: str, events: list[dict]) -> None:
    config = get_tenant_quota_config()
    if config is None:
        return
    policy = config.policy_for(tenant_from_stream_id(stream_id))
    if policy is None:
        return
    if policy.max_batch_events > 0 and len(events) > policy.max_batch_events:
        raise TenantQuotaExceeded(
            "tenant batch event quota exceeded",
            quota="max-batch-events",
        )
    if policy.max_event_bytes > 0:
        for event in events:
            if _encoded_event_bytes(event) > policy.max_event_bytes:
                raise TenantQuotaExceeded(
                    "tenant event size quota exceeded",
                    quota="max-event-bytes",
                    status_code=413,
                )


def _parse_poll_args(request: Request, *, allow_sse_resume: bool = False, min_timeout: int = 0) -> tuple[str, int, int]:
    last_id = request.query_params.get("last_id", "0")
    if allow_sse_resume:
        last_id = request.headers.get("last-event-id", last_id)
    limit = _parse_int_param(request.query_params.get("limit", "100"), "limit", 1, _MAX_LIMIT)
    timeout = _parse_int_param(request.query_params.get("timeout", "30"), "timeout", min_timeout, _MAX_TIMEOUT)
    return last_id, limit, timeout


async def push(request: Request) -> Response:
    stream_id = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:write", stream_id)
        _check_stream_rate_limit(request)
        _check_write_rate_limit(request)
        body = await _read_json_object(request)
        data = _read_event(body)
        _check_tenant_payload_quotas(stream_id, [data])
        await _check_tenant_window_quotas(
            request,
            stream_id,
            request_count=1,
            write_count=1,
            write_byte_count=_encoded_event_bytes(data),
        )
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "write", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except PayloadTooLargeError as exc:
        return _payload_too_large(exc, request, stream_id)
    except RequestBodyTimeoutError as exc:
        return _request_body_timeout(exc, request, stream_id)
    except ValueError as exc:
        return _error(str(exc))
    try:
        entry_id = await store.add(stream_id, data)
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="add_event",
        )
        raise
    return _json({"id": entry_id}, status=201)


async def push_many(request: Request) -> Response:
    stream_id = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:write", stream_id)
        _check_stream_rate_limit(request)
        _check_write_rate_limit(request)
        body = await _read_json_object(request)
        events = _read_events(body)
        _check_tenant_payload_quotas(stream_id, events)
        await _check_tenant_window_quotas(
            request,
            stream_id,
            request_count=1,
            write_count=1,
            write_byte_count=sum(_encoded_event_bytes(event) for event in events),
        )
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "write", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except PayloadTooLargeError as exc:
        return _payload_too_large(exc, request, stream_id)
    except RequestBodyTimeoutError as exc:
        return _request_body_timeout(exc, request, stream_id)
    except ValueError as exc:
        return _error(str(exc))
    try:
        ids = await store.add_many(stream_id, events)
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="add_many_events",
        )
        raise
    return _json({"ids": ids}, status=201)


async def poll(request: Request) -> Response:
    stream_id = None
    connection_lease = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:read", stream_id)
        _check_stream_rate_limit(request)
        await _check_tenant_request_quota(request, stream_id)
        last_id, limit, timeout = _parse_poll_args(request)
        connection_lease = await _acquire_connection_lease(stream_id, "long-poll", request)
        poll_limiter.acquire(stream_id)
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "stream", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except ConnectionLimitExceeded as exc:
        await _release_connection_lease(connection_lease, request, stream_id, "long-poll")
        return _connection_limit_error(exc, request, stream_id)
    except ValueError as exc:
        return _error(str(exc))
    try:
        app_metrics.poll_open()
        entries = await store.read(stream_id, last_id, limit, timeout * 1000)
    except TrimmedStreamCursor as exc:
        return _trim_gap(exc, request, stream_id)
    except InvalidStreamCursor as exc:
        return _error(f"invalid last_id: {exc}")
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="poll_read",
            last_id=last_id,
        )
        raise
    finally:
        app_metrics.poll_close()
        poll_limiter.release(stream_id)
        await _release_connection_lease(connection_lease, request, stream_id, "long-poll")
    if not entries:
        return Response(status_code=204)
    return _json({
        "events": [{"id": eid, "data": data} for eid, data in entries],
        "last_id": entries[-1][0],
    })


async def sse(request: Request) -> Response:
    stream_id = None
    connection_lease = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:read", stream_id)
        _check_stream_rate_limit(request)
        await _check_tenant_request_quota(request, stream_id)
        last_id, limit, timeout = _parse_poll_args(
            request,
            allow_sse_resume=True,
            min_timeout=1,
        )
        validate_stream_cursor(last_id)
        # Acquire before creating StreamingResponse so over-limit SSE requests return a
        # normal HTTP 429 instead of a streamed error body after status 200 has started.
        connection_lease = await _acquire_connection_lease(stream_id, "sse", request)
        sse_limiter.acquire(stream_id)
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "stream", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except ConnectionLimitExceeded as exc:
        await _release_connection_lease(connection_lease, request, stream_id, "sse")
        return _connection_limit_error(exc, request, stream_id)
    except InvalidStreamCursor as exc:
        return _error(f"invalid last_id: {exc}")
    except ValueError as exc:
        return _error(str(exc))

    async def event_stream():
        current_last_id = last_id
        disconnected = False
        read_timeout_s = min(timeout, max(1, SSE_POLL_INTERVAL))
        next_lease_refresh_at = time.monotonic() + max(
            1,
            (connection_lease.ttl_seconds // 2) if connection_lease is not None else SSE_POLL_INTERVAL,
        )
        app_metrics.sse_open()
        try:
            while True:
                try:
                    if await request.is_disconnected():
                        disconnected = True
                        return
                    if connection_lease is not None and time.monotonic() >= next_lease_refresh_at:
                        await _refresh_connection_lease(connection_lease, request, stream_id, "sse")
                        next_lease_refresh_at = time.monotonic() + max(1, connection_lease.ttl_seconds // 2)
                    entries = await store.read(stream_id, current_last_id, limit, read_timeout_s * 1000)
                except TrimmedStreamCursor as exc:
                    app_metrics.record_trim_gap()
                    _structured_log(
                        logging.INFO,
                        "trim_gap",
                        **_request_log_fields(request, stream_id),
                        status_code=200,
                        last_id=exc.last_id,
                        first_available_id=exc.first_available_id,
                    )
                    yield _sse_message(
                        exc.first_available_id,
                        {
                            "detail": "requested last_id has been trimmed from the stream",
                            "last_id": exc.last_id,
                            "first_available_id": exc.first_available_id,
                        },
                        event_type="gap",
                    )
                    return
                except asyncio.CancelledError:
                    disconnected = True
                    raise
                except RedisError as exc:
                    app_metrics.record_redis_error()
                    _structured_log(
                        logging.ERROR,
                        "redis_error",
                        **_request_log_fields(request, stream_id),
                        error_class=exc.__class__.__name__,
                        operation="sse_read",
                        last_id=current_last_id,
                    )
                    raise
                try:
                    if await request.is_disconnected():
                        disconnected = True
                        return
                    if not entries:
                        yield b": keep-alive\n\n"
                        continue
                    for entry_id, data in entries:
                        if await request.is_disconnected():
                            disconnected = True
                            return
                        current_last_id = entry_id
                        yield _sse_message(entry_id, data)
                except asyncio.CancelledError:
                    disconnected = True
                    raise
        finally:
            app_metrics.sse_close(disconnected=disconnected)
            if disconnected:
                _structured_log(
                    logging.INFO,
                    "sse_disconnected",
                    **_request_log_fields(request, stream_id),
                    status_code=499,
                )
            sse_limiter.release(stream_id)
            await _release_connection_lease(connection_lease, request, stream_id, "sse")

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


async def healthz(request: Request) -> Response:
    return _json({"status": "ok"})


async def readyz(request: Request) -> Response:
    try:
        if await store.ping():
            return _json({"status": "ok"})
    except Exception as exc:
        _structured_log(
            logging.ERROR,
            "readiness_failed",
            **_request_log_fields(request),
            status_code=503,
            error_class=exc.__class__.__name__,
        )
    app_metrics.record_readiness_failure()
    return _error("redis unavailable", status=503)


async def metrics(request: Request) -> Response:
    try:
        authorize_global_request(request, "metrics:read")
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request)
        return _auth_error(exc, request)
    return Response(
        app_metrics.render_prometheus(),
        headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
    )


async def info(request: Request) -> Response:
    stream_id = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:read", stream_id)
        _check_stream_rate_limit(request)
        await _check_tenant_request_quota(request, stream_id)
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "stream", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except ValueError as exc:
        return _error(str(exc))
    try:
        length = await store.length(stream_id)
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="stream_length",
        )
        raise
    return _json({"id": stream_id, "length": length})


async def delete(request: Request) -> Response:
    stream_id = None
    try:
        stream_id = _read_stream_id(request)
        authorize_request(request, "streams:delete", stream_id)
        _check_stream_rate_limit(request)
        _check_write_rate_limit(request)
        await _check_tenant_window_quotas(
            request,
            stream_id,
            request_count=1,
            write_count=1,
        )
    except AuthError as exc:
        try:
            _check_auth_failure_rate_limit(request)
        except RateLimitExceeded as rate_exc:
            return _rate_limit_error(rate_exc, "auth-failure", request, stream_id)
        return _auth_error(exc, request, stream_id)
    except RateLimitExceeded as exc:
        return _rate_limit_error(exc, "write", request, stream_id)
    except TenantQuotaExceeded as exc:
        return _tenant_quota_error(exc, request, stream_id)
    except ValueError as exc:
        return _error(str(exc))
    try:
        await store.delete(stream_id)
    except RedisError as exc:
        app_metrics.record_redis_error()
        _structured_log(
            logging.ERROR,
            "redis_error",
            **_request_log_fields(request, stream_id),
            error_class=exc.__class__.__name__,
            operation="delete_stream",
        )
        raise
    return Response(status_code=204)


@asynccontextmanager
async def lifespan(app: Starlette):
    global store
    configure_logging()
    authenticator = get_authenticator()
    _structured_log(
        logging.INFO,
        "startup_config",
        auth_configured=authenticator is not None,
        host=HOST,
        port=PORT,
        stream_ttl=STREAM_TTL,
        stream_maxlen=STREAM_MAXLEN,
        redis_url_configured=bool(REDIS_URL),
    )
    redis = aioredis.from_url(REDIS_URL, decode_responses=False)
    store = Store(redis)
    yield
    await redis.aclose()


app = Starlette(
    routes=[
        Route("/healthz", healthz, methods=["GET"]),
        Route("/readyz", readyz, methods=["GET"]),
        Route("/metrics", metrics, methods=["GET"]),
        Route("/streams/{stream_id}/events/batch", push_many, methods=["POST"]),
        Route("/streams/{stream_id}/events", push, methods=["POST"]),
        Route("/streams/{stream_id}/events", poll, methods=["GET"]),
        Route("/streams/{stream_id}/events/sse", sse, methods=["GET"]),
        Route("/streams/{stream_id}", info, methods=["GET"]),
        Route("/streams/{stream_id}", delete, methods=["DELETE"]),
    ],
    lifespan=lifespan,
    middleware=[Middleware(MetricsMiddleware)],
)


def main():
    configure_logging()
    uvicorn.run("tonle.api:app", host=HOST, port=PORT, reload=RELOAD)
