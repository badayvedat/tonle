import hashlib
import importlib
import logging
import os
from unittest.mock import AsyncMock

import orjson
import pytest
from redis.exceptions import RedisError
from starlette.requests import Request

import tonle.api as api
import tonle.config as config
from tonle.auth import clear_auth_cache, create_read_ticket, get_authenticator
from tonle.config import MAX_EVENT_BYTES, MAX_REQUEST_BYTES
from tonle.quotas import (
    ConnectionLease,
    clear_tenant_quota_cache,
    get_tenant_quota_config,
    tenant_from_stream_id,
)
from tonle.store import InvalidStreamCursor, TrimmedStreamCursor


class InvalidCursorStore:
    async def read(self, *args, **kwargs):
        raise InvalidStreamCursor("abc")


class FailingReadStore:
    async def read(self, *args, **kwargs):
        raise RedisError("redis down")


class TrimmedCursorStore:
    async def read(self, *args, **kwargs):
        raise TrimmedStreamCursor("1-0", "5-0")


class ReadyStore:
    def __init__(self, healthy: bool):
        self.healthy = healthy

    async def ping(self):
        if self.healthy:
            return True
        raise RuntimeError("redis unavailable")


class FakeRedis:
    def __init__(self):
        self.values = {}
        self.expirations = {}
        self.sorted_sets = {}

    async def incrby(self, key, amount):
        self.values[key] = self.values.get(key, 0) + amount
        return self.values[key]

    async def expire(self, key, seconds):
        self.expirations[key] = seconds
        return True

    async def eval(self, script, numkeys, *args):
        if "key_count = #KEYS" in script and "INCRBY" in script:
            keys = args[:numkeys]
            ttl_seconds, *remaining = args[numkeys:]
            amounts = remaining[:numkeys]
            limits = remaining[numkeys:]
            return await self._eval_incr_many_with_expire(keys, ttl_seconds, amounts, limits)
        if "INCRBY" in script:
            key, amount, ttl = args
            return await self._eval_incr_with_expire(key, amount, ttl)
        if "ZREMRANGEBYSCORE" in script and "ZCARD" in script:
            keys = args[:numkeys]
            lease_id, now_ms, expires_at_ms, ttl_seconds, *limits = args[numkeys:]
            return await self._eval_connection_acquire(keys, lease_id, now_ms, expires_at_ms, ttl_seconds, limits)
        if "ZSCORE" in script:
            keys = args[:numkeys]
            lease_id, expires_at_ms, ttl_seconds = args[numkeys:]
            return await self._eval_connection_refresh(keys, lease_id, expires_at_ms, ttl_seconds)
        if "ZREM" in script:
            keys = args[:numkeys]
            lease_id = args[numkeys]
            return await self._eval_connection_release(keys, lease_id)
        raise AssertionError("unexpected redis eval script")

    async def _eval_incr_with_expire(self, key, amount, ttl):
        value = await self.incrby(key, int(amount))
        if value == int(amount):
            await self.expire(key, int(ttl))
        return value

    async def _eval_incr_many_with_expire(self, keys, ttl_seconds, amounts, limits):
        for index, key in enumerate(keys):
            amount = int(amounts[index])
            limit = int(limits[index])
            if limit > 0 and amount > 0:
                value = await self.incrby(key, amount)
                if value == amount:
                    await self.expire(key, int(ttl_seconds))
                if value > limit:
                    return index + 1
        return 0

    async def _eval_connection_acquire(self, keys, lease_id, now_ms, expires_at_ms, ttl_seconds, limits):
        now_ms = int(now_ms)
        expires_at_ms = int(expires_at_ms)
        for key in keys:
            members = self.sorted_sets.setdefault(key, {})
            for member, score in list(members.items()):
                if score <= now_ms:
                    del members[member]
        for index, key in enumerate(keys):
            limit = int(limits[index])
            if limit > 0 and len(self.sorted_sets.setdefault(key, {})) >= limit:
                return index + 1
        for key in keys:
            self.sorted_sets.setdefault(key, {})[lease_id] = expires_at_ms
            await self.expire(key, int(ttl_seconds))
        return 0

    async def _eval_connection_refresh(self, keys, lease_id, expires_at_ms, ttl_seconds):
        for key in keys:
            members = self.sorted_sets.setdefault(key, {})
            if lease_id in members:
                members[lease_id] = int(expires_at_ms)
                await self.expire(key, int(ttl_seconds))
        return 0

    async def _eval_connection_release(self, keys, lease_id):
        for key in keys:
            self.sorted_sets.setdefault(key, {}).pop(lease_id, None)
        return 0

    async def aclose(self):
        pass


class FailingQuotaRedis(FakeRedis):
    async def eval(self, script, numkeys, *args):
        raise RedisError("quota redis down")


class RecordingStore:
    def __init__(self, redis=None):
        self.add_calls = []
        self.add_many_calls = []
        self.delete_calls = []
        self.read_calls = []
        self.read_results = []
        self.redis = redis or FakeRedis()

    async def add(self, stream_id, data):
        self.add_calls.append((stream_id, data))
        return "1-0"

    async def add_many(self, stream_id, events):
        self.add_many_calls.append((stream_id, events))
        return ["1-0"] * len(events)

    async def read(self, *args, **kwargs):
        self.read_calls.append((args, kwargs))
        if self.read_results:
            return self.read_results.pop(0)
        return []

    async def length(self, stream_id):
        return 0

    async def delete(self, stream_id):
        self.delete_calls.append(stream_id)


def make_request(
    method: str,
    path: str,
    *,
    query_string: bytes = b"",
    body: bytes = b"",
    disconnect: bool = False,
    path_params: dict | None = None,
    headers: dict[str, str] | None = None,
) -> Request:
    initial_sent = False
    disconnect_sent = False

    async def receive():
        nonlocal initial_sent, disconnect_sent
        if not initial_sent:
            initial_sent = True
            return {"type": "http.request", "body": body, "more_body": False}
        if disconnect and not disconnect_sent:
            disconnect_sent = True
            return {"type": "http.disconnect"}
        return {"type": "http.request", "body": b"", "more_body": False}

    raw_headers = [(b"content-type", b"application/json")]
    if headers:
        raw_headers.extend(
            (name.lower().encode(), value.encode())
            for name, value in headers.items()
        )

    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": query_string,
            "path_params": path_params or {},
            "headers": raw_headers,
        },
        receive=receive,
    )


def make_streaming_request(
    method: str,
    path: str,
    *,
    chunks: list[bytes],
    path_params: dict | None = None,
    headers: dict[str, str] | None = None,
) -> Request:
    chunk_iter = iter(chunks)

    async def receive():
        try:
            chunk = next(chunk_iter)
        except StopIteration:
            return {"type": "http.request", "body": b"", "more_body": False}
        return {"type": "http.request", "body": chunk, "more_body": True}

    raw_headers = [(b"content-type", b"application/json")]
    if headers:
        raw_headers.extend(
            (name.lower().encode(), value.encode())
            for name, value in headers.items()
        )

    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": b"",
            "path_params": path_params or {},
            "headers": raw_headers,
        },
        receive=receive,
    )


def make_slow_streaming_request(
    method: str,
    path: str,
    *,
    chunk: bytes,
    delay_seconds: float,
    path_params: dict | None = None,
) -> Request:
    sent = False

    async def receive():
        nonlocal sent
        if not sent:
            sent = True
            await api.asyncio.sleep(delay_seconds)
            return {"type": "http.request", "body": chunk, "more_body": False}
        return {"type": "http.request", "body": b"", "more_body": False}

    return Request(
        {
            "type": "http",
            "method": method,
            "path": path,
            "query_string": b"",
            "path_params": path_params or {},
            "headers": [(b"content-type", b"application/json")],
        },
        receive=receive,
    )


@pytest.fixture(autouse=True)
def reset_state():
    api.app_metrics.reset()
    api.sse_limiter.reset()
    api.poll_limiter.reset()
    api.request_rate_limiter.reset()
    api.write_rate_limiter.reset()
    api.auth_failure_rate_limiter.reset()
    clear_tenant_quota_cache()
    yield
    api.app_metrics.reset()
    api.sse_limiter.reset()
    api.poll_limiter.reset()
    api.request_rate_limiter.reset()
    api.write_rate_limiter.reset()
    api.auth_failure_rate_limiter.reset()
    clear_auth_cache()
    clear_tenant_quota_cache()


def test_fixed_window_rate_limiter_allows_after_window_reset():
    now = 0.0
    limiter = api.FixedWindowRateLimiter(
        max_events=1,
        window_seconds=10,
        kind="stream",
        clock=lambda: now,
    )

    limiter.check("principal:reader")
    with pytest.raises(api.RateLimitExceeded):
        limiter.check("principal:reader")

    now = 10.0
    limiter.check("principal:reader")


def test_fixed_window_rate_limiter_prunes_stale_keys():
    now = 0.0
    limiter = api.FixedWindowRateLimiter(
        max_events=1,
        window_seconds=10,
        kind="stream",
        clock=lambda: now,
    )

    limiter.check("principal:a")
    limiter.check("principal:b")
    assert limiter.key_count == 2

    now = 10.0
    limiter.check("principal:c")

    assert limiter.key_count == 1


def test_tenant_quota_config_parses_default_and_override(monkeypatch):
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps(
            {
                "default": {"requests_per_window": 10},
                "tenanta": {
                    "window_seconds": 30,
                    "writes_per_window": 5,
                    "write_bytes_per_window": 100,
                    "max_event_bytes": 50,
                    "max_batch_events": 2,
                },
            }
        ).decode(),
    )
    clear_tenant_quota_cache()

    config = get_tenant_quota_config()

    assert config.policy_for("tenant-b").requests_per_window == 10
    assert config.policy_for("tenanta").window_seconds == 30
    assert config.policy_for("tenanta").requests_per_window == 10
    assert config.policy_for("tenanta").writes_per_window == 5
    assert config.policy_for("tenanta").write_bytes_per_window == 100
    assert config.policy_for("tenanta").max_event_bytes == 50
    assert config.policy_for("tenanta").max_batch_events == 2


def test_tenant_quota_config_merges_tenant_over_default(monkeypatch):
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps(
            {
                "default": {
                    "requests_per_window": 10,
                    "writes_per_window": 5,
                    "max_event_bytes": 50,
                    "max_batch_events": 2,
                },
                "tenanta": {"requests_per_window": 20},
            }
        ).decode(),
    )
    clear_tenant_quota_cache()

    policy = get_tenant_quota_config().policy_for("tenanta")

    assert policy.requests_per_window == 20
    assert policy.writes_per_window == 5
    assert policy.max_event_bytes == 50
    assert policy.max_batch_events == 2


def test_tenant_from_stream_id_uses_leftmost_separator():
    assert tenant_from_stream_id("tenanta:job:123") == "tenanta"
    assert tenant_from_stream_id("tenant_b_job_123") == "tenant"
    assert tenant_from_stream_id("tenant-c-job-123") == "tenant"
    assert tenant_from_stream_id("foo-bar_baz") == "foo"
    assert tenant_from_stream_id("foo_bar-baz") == "foo"
    assert tenant_from_stream_id("foo:bar-baz") == "foo"
    assert tenant_from_stream_id("globalstream") == "globalstream"


def test_auth_token_prefixes_must_end_with_stream_id_delimiter(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [
                {
                    "name": "writer",
                    "token": "test-static-token-0000000000000001",
                    "scopes": ["streams:write"],
                    "prefixes": ["tenanta"],
                }
            ]
        ).decode(),
    )
    clear_auth_cache()

    with pytest.raises(ValueError, match="prefixes entries must be non-empty"):
        get_authenticator()


def test_auth_token_rejects_short_plaintext_token(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "short", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()

    with pytest.raises(ValueError, match="token must be at least 32 characters"):
        get_authenticator()


def test_auth_token_rejects_invalid_principal_name(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [
                {
                    "name": "bad principal",
                    "token": "test-static-token-0000000000000001",
                    "scopes": ["streams:write"],
                }
            ]
        ).decode(),
    )
    clear_auth_cache()

    with pytest.raises(ValueError, match="name must be 1-128 chars"):
        get_authenticator()


def test_create_read_ticket_rejects_short_secret():
    with pytest.raises(ValueError, match="secret must be at least 32 characters"):
        create_read_ticket("short", "tenanta:demo", expires_at=4102444800)


def test_create_read_ticket_rejects_invalid_subject():
    with pytest.raises(ValueError, match="subject must be 1-128 chars"):
        create_read_ticket(
            "read-ticket-secret-000000000000001",
            "tenanta:demo",
            expires_at=4102444800,
            subject="bad principal",
        )


def test_config_rejects_invalid_numeric_ranges(monkeypatch):
    monkeypatch.setenv("TONLE_PORT", "0")
    with pytest.raises(ValueError, match="TONLE_PORT must be at least 1"):
        importlib.reload(config)
    monkeypatch.delenv("TONLE_PORT", raising=False)
    importlib.reload(config)


def test_client_identity_ignores_forwarded_headers_by_default(monkeypatch):
    monkeypatch.setattr(api, "TRUST_FORWARDED_HEADERS", False)
    request = make_request(
        "GET",
        "/metrics",
        headers={"X-Forwarded-For": "203.0.113.99"},
    )

    assert api._client_identity(request) == "ip:unknown"


def test_client_identity_can_trust_generic_forwarded_header(monkeypatch):
    monkeypatch.setattr(api, "TRUST_FORWARDED_HEADERS", True)
    request = make_request(
        "GET",
        "/metrics",
        headers={"X-Forwarded-For": "203.0.113.99, 10.0.0.1"},
    )

    assert api._client_identity(request) == "ip:203.0.113.99"


def test_tenant_quota_config_rejects_zero_window(monkeypatch):
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"default": {"window_seconds": 0, "requests_per_window": 10}}).decode(),
    )
    clear_tenant_quota_cache()

    with pytest.raises(ValueError, match="window_seconds must be a positive integer"):
        get_tenant_quota_config()


def test_tenant_quota_config_rejects_invalid_json(monkeypatch):
    monkeypatch.setenv("TONLE_TENANT_QUOTAS", "{")
    clear_tenant_quota_cache()

    with pytest.raises(ValueError, match="TONLE_TENANT_QUOTAS must be valid JSON"):
        get_tenant_quota_config()


def test_tenant_quota_config_rejects_null_values(monkeypatch):
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"default": {"requests_per_window": None}}).decode(),
    )
    clear_tenant_quota_cache()

    with pytest.raises(ValueError, match="requests_per_window must be a non-negative integer"):
        get_tenant_quota_config()


def test_configure_logging_emits_info_structured_logs_without_existing_handlers(capsys):
    root_logger = logging.getLogger()
    original_root_handlers = root_logger.handlers[:]
    original_root_level = root_logger.level
    original_api_level = api.logger.level
    root_logger.handlers.clear()
    api.logger.setLevel(logging.NOTSET)
    try:
        api.configure_logging()
        api._structured_log(logging.INFO, "startup_config_test", redis_url_configured=True)
        captured = capsys.readouterr()
    finally:
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.handlers[:] = original_root_handlers
        root_logger.setLevel(original_root_level)
        api.logger.setLevel(original_api_level)

    assert '"event":"startup_config_test"' in captured.err
    assert '"level":"INFO"' in captured.err
    assert captured.err.strip().startswith("{")


async def test_poll_returns_400_for_invalid_last_id(monkeypatch):
    monkeypatch.setattr(api, "store", InvalidCursorStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"last_id=abc&timeout=0",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "invalid last_id: abc"}


async def test_poll_returns_400_for_non_integer_limit():
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"limit=abc",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "limit must be an integer"}


async def test_poll_returns_400_for_out_of_range_timeout():
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"timeout=-1",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "timeout must be between 0 and 60"}


async def test_push_rejects_non_object_data(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": ["bad"]}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "data must be an object"}
    assert store.add_calls == []


async def test_push_rejects_invalid_json():
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=b"{",
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "request body must be valid JSON"}


async def test_push_rejects_when_tenant_request_quota_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"requests_per_window": 1}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    second = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 201
    assert second.status_code == 429
    assert orjson.loads(second.body) == {"detail": "tenant requests quota exceeded"}
    assert "retry-after" in second.headers
    assert store.add_calls == [("tenanta:demo", {"n": 1})]
    assert 'tonle_tenant_quota_rejections_total{quota="requests"} 1' in api.app_metrics.render_prometheus()


async def test_push_rejects_when_tenant_write_quota_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"writes_per_window": 1}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    second = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 201
    assert second.status_code == 429
    assert orjson.loads(second.body) == {"detail": "tenant writes quota exceeded"}
    assert store.add_calls == [("tenanta:demo", {"n": 1})]
    assert 'tonle_tenant_quota_rejections_total{quota="writes"} 1' in api.app_metrics.render_prometheus()


async def test_tenant_rate_quota_rejections_remain_counted(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"requests_per_window": 1}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.info(
        make_request(
            "GET",
            "/streams/tenanta:demo",
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    second = await api.info(
        make_request(
            "GET",
            "/streams/tenanta:demo",
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    third = await api.info(
        make_request(
            "GET",
            "/streams/tenanta:demo",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 200
    assert second.status_code == 429
    assert third.status_code == 429
    assert len(store.redis.values) == 1
    assert next(iter(store.redis.values.values())) == 3
    assert 'tonle_tenant_quota_rejections_total{quota="requests"} 2' in api.app_metrics.render_prometheus()


async def test_push_rejects_when_tenant_write_bytes_quota_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"write_bytes_per_window": 7}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    second = await api.push(
        make_request(
            "POST",
            "/streams/tenanta:demo/events",
            body=orjson.dumps({"data": {"n": 2}}),
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 201
    assert second.status_code == 429
    assert orjson.loads(second.body) == {"detail": "tenant write-bytes quota exceeded"}
    assert store.add_calls == [("tenanta:demo", {"n": 1})]
    assert 'tonle_tenant_quota_rejections_total{quota="write-bytes"} 1' in api.app_metrics.render_prometheus()


async def test_push_many_rejects_when_tenant_batch_quota_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_batch_events": 1}}).decode(),
    )
    clear_tenant_quota_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events/batch",
        body=orjson.dumps({"events": [{"n": 1}, {"n": 2}]}),
        path_params={"stream_id": "tenanta:demo"},
    )

    response = await api.push_many(request)

    assert response.status_code == 429
    assert orjson.loads(response.body) == {"detail": "tenant batch event quota exceeded"}
    assert "retry-after" not in response.headers
    assert store.add_many_calls == []
    assert store.redis.values == {}


async def test_push_rejects_tenant_max_event_bytes_without_retry_after(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_event_bytes": 3}}).decode(),
    )
    clear_tenant_quota_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenanta:demo"},
    )

    response = await api.push(request)

    assert response.status_code == 413
    assert orjson.loads(response.body) == {"detail": "tenant event size quota exceeded"}
    assert "retry-after" not in response.headers
    assert store.add_calls == []
    assert store.redis.values == {}


async def test_tenant_quota_redis_errors_are_observable(monkeypatch, caplog):
    store = RecordingStore(redis=FailingQuotaRedis())
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"requests_per_window": 1}}).decode(),
    )
    clear_tenant_quota_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenanta:demo"},
    )

    with caplog.at_level(logging.ERROR, logger="tonle.api"):
        with pytest.raises(RedisError):
            await api.push(request)

    assert '"event":"redis_error"' in caplog.text
    assert '"operation":"tenant_quota_check"' in caplog.text
    assert '"method":"POST"' in caplog.text
    assert '"route":"/streams/tenanta:demo/events"' in caplog.text
    assert '"client":"ip:unknown"' in caplog.text
    assert "tonle_redis_errors_total 1" in api.app_metrics.render_prometheus()


async def test_poll_rejects_when_distributed_long_poll_tenant_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_long_polls": 1}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.poll(
        make_request(
            "GET",
            "/streams/tenanta:demo/events",
            query_string=b"timeout=0",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 204
    assert all(not members for members in store.redis.sorted_sets.values())

    lease_keys = (
        "tonle:conn:long-poll:global",
        "tonle:conn:long-poll:tenant:tenanta",
        "tonle:conn:long-poll:stream:tenanta:demo",
        "tonle:conn:long-poll:principal:ip:unknown",
    )
    await store.redis._eval_connection_acquire(
        lease_keys,
        "held-lease",
        0,
        4102444800000,
        35,
        (0, 1, 0, 0),
    )

    second = await api.poll(
        make_request(
            "GET",
            "/streams/tenanta:demo/events",
            query_string=b"timeout=0",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert second.status_code == 429
    assert second.headers["retry-after"] == "30"
    assert orjson.loads(second.body) == {"detail": "tenant long-poll-connections-tenant quota exceeded"}
    assert 'tonle_tenant_quota_rejections_total{quota="long-poll-connections-tenant"} 1' in api.app_metrics.render_prometheus()


async def test_poll_releases_distributed_lease_when_process_local_limit_rejects(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_long_polls": 10}}).decode(),
    )
    limiter = api.ConnectionLimiter(max_total=100, max_per_stream=1, kind="long-poll")
    limiter.acquire("tenanta:demo")
    monkeypatch.setattr(api, "poll_limiter", limiter)
    clear_tenant_quota_cache()

    response = await api.poll(
        make_request(
            "GET",
            "/streams/tenanta:demo/events",
            query_string=b"timeout=0",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert response.status_code == 429
    assert all(not members for members in store.redis.sorted_sets.values())


async def test_sse_rejects_when_distributed_sse_stream_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_sse_connections_per_stream": 1}}).decode(),
    )
    clear_tenant_quota_cache()
    lease_keys = (
        "tonle:conn:sse:global",
        "tonle:conn:sse:tenant:tenanta",
        "tonle:conn:sse:stream:tenanta:demo",
        "tonle:conn:sse:principal:ip:unknown",
    )
    await store.redis._eval_connection_acquire(
        lease_keys,
        "held-lease",
        0,
        4102444800000,
        35,
        (0, 0, 1, 0),
    )

    response = await api.sse(
        make_request(
            "GET",
            "/streams/tenanta:demo/events/sse",
            query_string=b"timeout=1",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert response.status_code == 429
    assert response.headers["retry-after"] == "30"
    assert orjson.loads(response.body) == {"detail": "tenant sse-connections-stream quota exceeded"}
    assert 'tonle_tenant_quota_rejections_total{quota="sse-connections-stream"} 1' in api.app_metrics.render_prometheus()


async def test_sse_releases_distributed_lease_when_process_local_limit_rejects(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"max_sse_connections": 10}}).decode(),
    )
    limiter = api.ConnectionLimiter(max_total=100, max_per_stream=1, kind="sse")
    limiter.acquire("tenanta:demo")
    monkeypatch.setattr(api, "sse_limiter", limiter)
    clear_tenant_quota_cache()

    response = await api.sse(
        make_request(
            "GET",
            "/streams/tenanta:demo/events/sse",
            query_string=b"timeout=1",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert response.status_code == 429
    assert all(not members for members in store.redis.sorted_sets.values())


async def test_delete_rejects_when_tenant_write_quota_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_TENANT_QUOTAS",
        orjson.dumps({"tenanta": {"writes_per_window": 1}}).decode(),
    )
    clear_tenant_quota_cache()

    first = await api.delete(
        make_request(
            "DELETE",
            "/streams/tenanta:demo",
            path_params={"stream_id": "tenanta:demo"},
        )
    )
    second = await api.delete(
        make_request(
            "DELETE",
            "/streams/tenanta:demo",
            path_params={"stream_id": "tenanta:demo"},
        )
    )

    assert first.status_code == 204
    assert second.status_code == 429
    assert orjson.loads(second.body) == {"detail": "tenant writes quota exceeded"}
    assert store.delete_calls == ["tenanta:demo"]


async def test_push_rejects_oversized_request_body(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=b"x" * (MAX_REQUEST_BYTES + 1),
        path_params={"stream_id": "demo"},
        headers={"Content-Length": str(MAX_REQUEST_BYTES + 1)},
    )

    response = await api.push(request)

    assert response.status_code == 413
    assert orjson.loads(response.body) == {"detail": f"request body must be at most {MAX_REQUEST_BYTES} bytes"}
    assert "tonle_payload_too_large_total 1" in api.app_metrics.render_prometheus()
    assert store.add_calls == []


async def test_push_rejects_oversized_streaming_request_body_without_content_length(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(api, "MAX_REQUEST_BYTES", 10)
    request = make_streaming_request(
        "POST",
        "/streams/demo/events",
        chunks=[b'{"data":', b'{"x":"too-large"}}'],
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 413
    assert orjson.loads(response.body) == {"detail": "request body must be at most 10 bytes"}
    assert "tonle_payload_too_large_total 1" in api.app_metrics.render_prometheus()
    assert store.add_calls == []


async def test_push_rejects_slow_request_body(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(api, "REQUEST_BODY_READ_TIMEOUT_SECONDS", 0.001)
    request = make_slow_streaming_request(
        "POST",
        "/streams/demo/events",
        chunk=orjson.dumps({"data": {"n": 1}}),
        delay_seconds=0.01,
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 408
    assert orjson.loads(response.body) == {"detail": "request body read timed out"}
    assert store.add_calls == []


async def test_push_rejects_oversized_event_payload(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"blob": "x" * MAX_EVENT_BYTES}}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 413
    assert orjson.loads(response.body) == {"detail": f"event payload must be at most {MAX_EVENT_BYTES} bytes"}
    assert store.add_calls == []


async def test_push_rejects_invalid_stream_id(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/bad/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "../bad"},
    )

    response = await api.push(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "stream_id must be 1-128 chars of [A-Za-z0-9:_-]"}
    assert store.add_calls == []


async def test_push_many_rejects_non_object_events(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events/batch",
        body=orjson.dumps({"events": [{"n": 1}, "bad"]}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push_many(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "events must be an array of objects"}
    assert store.add_many_calls == []


async def test_push_many_rejects_oversized_batches():
    request = make_request(
        "POST",
        "/streams/demo/events/batch",
        body=orjson.dumps({"events": [{}] * 1001}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push_many(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "events must contain at most 1000 items"}


async def test_push_many_rejects_empty_batches(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events/batch",
        body=orjson.dumps({"events": []}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push_many(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "events must contain at least 1 item"}
    assert store.add_many_calls == []


async def test_push_many_rejects_oversized_event_payload(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "POST",
        "/streams/demo/events/batch",
        body=orjson.dumps({"events": [{"blob": "x" * MAX_EVENT_BYTES}]}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push_many(request)

    assert response.status_code == 413
    assert orjson.loads(response.body) == {"detail": f"event payload must be at most {MAX_EVENT_BYTES} bytes"}
    assert store.add_many_calls == []


async def test_poll_rejects_invalid_stream_id_before_store_call(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "GET",
        "/streams/bad/events",
        path_params={"stream_id": "bad/value"},
    )

    response = await api.poll(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "stream_id must be 1-128 chars of [A-Za-z0-9:_-]"}
    assert store.read_calls == []


async def test_poll_rejects_when_total_connection_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "poll_limiter",
        api.ConnectionLimiter(max_total=0, max_per_stream=100, kind="long-poll"),
    )
    request = make_request(
        "GET",
        "/streams/demo/events",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"
    assert orjson.loads(response.body) == {"detail": "too many active long-poll connections"}
    assert store.read_calls == []
    assert 'tonle_connection_limit_rejections_total{kind="long-poll"} 1' in api.app_metrics.render_prometheus()


async def test_poll_rejects_when_stream_connection_limit_is_reached(monkeypatch):
    store = RecordingStore()
    limiter = api.ConnectionLimiter(max_total=100, max_per_stream=1, kind="long-poll")
    limiter.acquire("demo")
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(api, "poll_limiter", limiter)
    request = make_request(
        "GET",
        "/streams/demo/events",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 429
    assert orjson.loads(response.body) == {"detail": "too many active long-poll connections for stream"}
    assert store.read_calls == []


async def test_poll_releases_connection_counter_after_completion(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"timeout=0",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 204
    assert api.poll_limiter.active_total == 0
    assert "tonle_polls_active 0" in api.app_metrics.render_prometheus()


async def test_poll_rejects_when_request_rate_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "request_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="stream"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "reader", "token": "test-static-token-0000000000000001", "scopes": ["streams:read"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"timeout=0",
        path_params={"stream_id": "demo"},
        headers={"Authorization": "Bearer test-static-token-0000000000000001"},
    )

    first = await api.poll(request)
    second = await api.poll(request)

    assert first.status_code == 204
    assert second.status_code == 429
    assert 1 <= int(second.headers["retry-after"]) <= 60
    assert orjson.loads(second.body) == {"detail": "too many stream requests"}
    assert len(store.read_calls) == 1
    assert 'tonle_rate_limit_rejections_total{kind="stream"} 1' in api.app_metrics.render_prometheus()


async def test_request_rate_limit_is_isolated_by_principal(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "request_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="stream"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [
                {"name": "reader-a", "token": "reader-a-static-token-000000000001", "scopes": ["streams:read"]},
                {"name": "reader-b", "token": "reader-b-static-token-000000000001", "scopes": ["streams:read"]},
            ]
        ).decode(),
    )
    clear_auth_cache()

    first = await api.poll(
        make_request(
            "GET",
            "/streams/demo/events",
            query_string=b"timeout=0",
            path_params={"stream_id": "demo"},
            headers={"Authorization": "Bearer reader-a-static-token-000000000001"},
        )
    )
    second = await api.poll(
        make_request(
            "GET",
            "/streams/demo/events",
            query_string=b"timeout=0",
            path_params={"stream_id": "demo"},
            headers={"Authorization": "Bearer reader-b-static-token-000000000001"},
        )
    )

    assert first.status_code == 204
    assert second.status_code == 204


async def test_sse_returns_400_for_zero_timeout():
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        query_string=b"timeout=0",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)

    assert response.status_code == 400
    assert orjson.loads(response.body) == {"detail": "timeout must be between 1 and 60"}


async def test_sse_rejects_when_total_connection_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "sse_limiter",
        api.ConnectionLimiter(max_total=0, max_per_stream=100, kind="sse"),
    )
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)

    assert response.status_code == 429
    assert response.headers["retry-after"] == "1"
    assert orjson.loads(response.body) == {"detail": "too many active sse connections"}
    assert store.read_calls == []
    assert 'tonle_connection_limit_rejections_total{kind="sse"} 1' in api.app_metrics.render_prometheus()


async def test_sse_rejects_when_stream_connection_limit_is_reached(monkeypatch):
    store = RecordingStore()
    limiter = api.ConnectionLimiter(max_total=100, max_per_stream=1, kind="sse")
    limiter.acquire("demo")
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(api, "sse_limiter", limiter)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)

    assert response.status_code == 429
    assert orjson.loads(response.body) == {"detail": "too many active sse connections for stream"}
    assert store.read_calls == []


async def test_sse_streams_event_messages(monkeypatch):
    store = RecordingStore()
    store.read_results = [[("1-0", {"n": 1})]]
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk)
        break

    assert response.media_type == "text/event-stream"
    assert chunks == [b'id: 1-0\ndata: {"n":1}\n\n']


async def test_poll_returns_409_for_trimmed_cursor(monkeypatch):
    monkeypatch.setattr(api, "store", TrimmedCursorStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"last_id=1-0&timeout=0",
        path_params={"stream_id": "demo"},
    )

    response = await api.poll(request)

    assert response.status_code == 409
    assert orjson.loads(response.body) == {
        "detail": "requested last_id has been trimmed from the stream",
        "last_id": "1-0",
        "first_available_id": "5-0",
    }
    assert "tonle_trim_gaps_total 1" in api.app_metrics.render_prometheus()


async def test_poll_logs_backend_failures(monkeypatch, caplog):
    monkeypatch.setattr(api, "store", FailingReadStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events",
        query_string=b"last_id=0&timeout=0",
        path_params={"stream_id": "demo"},
    )

    with caplog.at_level(logging.ERROR, logger="tonle.api"):
        with pytest.raises(RedisError):
            await api.poll(request)

    assert '"event":"redis_error"' in caplog.text
    assert '"operation":"poll_read"' in caplog.text
    assert '"stream":"demo"' in caplog.text
    assert "tonle_redis_errors_total 1" in api.app_metrics.render_prometheus()


async def test_sse_streams_gap_event(monkeypatch):
    monkeypatch.setattr(api, "store", TrimmedCursorStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)
    chunk = await anext(response.body_iterator)

    assert chunk == (
        b'event: gap\nid: 5-0\ndata: '
        b'{"detail":"requested last_id has been trimmed from the stream","last_id":"1-0","first_available_id":"5-0"}\n\n'
    )
    assert "tonle_trim_gaps_total 1" in api.app_metrics.render_prometheus()


async def test_sse_releases_connection_counter_after_completion(monkeypatch):
    monkeypatch.setattr(api, "store", TrimmedCursorStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    response = await api.sse(request)
    chunks = [chunk async for chunk in response.body_iterator]

    assert len(chunks) == 1
    assert api.sse_limiter.active_total == 0
    assert "tonle_sse_connections_active 0" in api.app_metrics.render_prometheus()


async def test_sse_logs_backend_failures(monkeypatch, caplog):
    monkeypatch.setattr(api, "store", FailingReadStore(), raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    with caplog.at_level(logging.ERROR, logger="tonle.api"):
        response = await api.sse(request)
        with pytest.raises(RedisError):
            await anext(response.body_iterator)

    assert '"event":"redis_error"' in caplog.text
    assert '"stream":"demo"' in caplog.text
    assert '"last_id":"0"' in caplog.text
    assert "tonle_redis_errors_total 1" in api.app_metrics.render_prometheus()


async def test_sse_connection_lease_refresh_retries_once(monkeypatch, caplog):
    class FlakyRefreshRedis(FakeRedis):
        def __init__(self):
            super().__init__()
            self.refresh_failures = 0

        async def eval(self, script, numkeys, *args):
            if "ZSCORE" in script and self.refresh_failures == 0:
                self.refresh_failures += 1
                raise RedisError("temporary refresh failure")
            return await super().eval(script, numkeys, *args)

    redis = FlakyRefreshRedis()
    lease = ConnectionLease(
        lease_id="lease-1",
        keys=("tonle:conn:sse:tenant:demo",),
        ttl_seconds=30,
    )
    redis.sorted_sets[lease.keys[0]] = {lease.lease_id: 1}
    monkeypatch.setattr(api, "store", RecordingStore(redis=redis), raising=False)
    monkeypatch.setattr(api.asyncio, "sleep", AsyncMock())
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
    )

    with caplog.at_level(logging.ERROR, logger="tonle.api"):
        await api._refresh_connection_lease(lease, request, "demo", "sse")

    assert redis.refresh_failures == 1
    assert '"operation":"connection_lease_refresh"' in caplog.text
    assert "tonle_redis_errors_total 1" in api.app_metrics.render_prometheus()


async def test_sse_stops_on_disconnect(monkeypatch):
    store = RecordingStore()
    store.read_results = [[("1-0", {"n": 1})]]
    monkeypatch.setattr(api, "store", store, raising=False)
    request = make_request(
        "GET",
        "/streams/demo/events/sse",
        path_params={"stream_id": "demo"},
        disconnect=True,
    )

    response = await api.sse(request)

    with pytest.raises(StopAsyncIteration):
        await anext(response.body_iterator)

    assert "tonle_sse_disconnects_total 1" in api.app_metrics.render_prometheus()


async def test_healthz_reports_ok():
    response = await api.healthz(make_request("GET", "/healthz"))

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"status": "ok"}


async def test_readyz_reports_ok(monkeypatch):
    monkeypatch.setattr(api, "store", ReadyStore(healthy=True), raising=False)

    response = await api.readyz(make_request("GET", "/readyz"))

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"status": "ok"}


async def test_readyz_reports_redis_failure(monkeypatch, caplog):
    monkeypatch.setattr(api, "store", ReadyStore(healthy=False), raising=False)

    with caplog.at_level(logging.ERROR, logger="tonle.api"):
        response = await api.readyz(make_request("GET", "/readyz"))

    assert response.status_code == 503
    assert orjson.loads(response.body) == {"detail": "redis unavailable"}
    assert '"event":"readiness_failed"' in caplog.text
    assert '"error_class":"RuntimeError"' in caplog.text
    assert "tonle_readiness_failures_total 1" in api.app_metrics.render_prometheus()


async def test_metrics_exposes_prometheus_text():
    api.app_metrics.record_request("/healthz", "GET", 200, 0.01)

    response = await api.metrics(make_request("GET", "/metrics"))

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
    body = response.body.decode()
    assert 'tonle_requests_total{method="GET",route="/healthz",status="200"} 1' in body


async def test_metrics_middleware_uses_bounded_unmatched_route_label():
    async def not_found_app(scope, receive, send):
        await send({"type": "http.response.start", "status": 404, "headers": []})
        await send({"type": "http.response.body", "body": b""})

    middleware = api.MetricsMiddleware(not_found_app)
    sent = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    await middleware(
        {
            "type": "http",
            "method": "GET",
            "path": "/random/path/123",
        },
        receive,
        send,
    )

    metrics = api.app_metrics.render_prometheus()
    assert 'tonle_requests_total{method="GET",route="unmatched",status="404"} 1' in metrics
    assert "/random/path/123" not in metrics


async def test_metrics_requires_bearer_token_when_auth_enabled(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "metrics", "token": "metrics-static-token-00000000000001", "scopes": ["metrics:read"]}]
        ).decode(),
    )
    clear_auth_cache()

    response = await api.metrics(make_request("GET", "/metrics"))

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert orjson.loads(response.body) == {"detail": "missing bearer token"}


async def test_metrics_rejects_stream_token_without_metrics_scope(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "reader", "token": "reader-static-token-000000000000001", "scopes": ["streams:read"]}]
        ).decode(),
    )
    clear_auth_cache()

    response = await api.metrics(
        make_request(
            "GET",
            "/metrics",
            headers={"Authorization": "Bearer reader-static-token-000000000000001"},
        )
    )

    assert response.status_code == 403
    assert orjson.loads(response.body) == {"detail": "forbidden"}


async def test_metrics_auth_failures_are_rate_limited_by_client_ip(monkeypatch):
    monkeypatch.setattr(
        api,
        "auth_failure_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="auth-failure"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "metrics", "token": "metrics-static-token-00000000000001", "scopes": ["metrics:read"]}]
        ).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "GET",
        "/metrics",
        headers={"X-Forwarded-For": "203.0.113.10"},
    )

    first = await api.metrics(request)
    second = await api.metrics(request)

    assert first.status_code == 401
    assert second.status_code == 429
    assert 1 <= int(second.headers["retry-after"]) <= 60
    assert orjson.loads(second.body) == {"detail": "too many auth-failure requests"}


async def test_metrics_accepts_metrics_scope(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "metrics", "token": "metrics-static-token-00000000000001", "scopes": ["metrics:read"]}]
        ).decode(),
    )
    clear_auth_cache()

    response = await api.metrics(
        make_request(
            "GET",
            "/metrics",
            headers={"Authorization": "Bearer metrics-static-token-00000000000001"},
        )
    )

    assert response.status_code == 200
    assert response.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
    assert "tonle_requests_total" in response.body.decode()


async def test_metrics_scope_can_be_combined_with_prefix_limited_stream_scopes(monkeypatch):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [
                {
                    "name": "ops",
                    "token": "ops-static-token-00000000000000001",
                    "scopes": ["streams:read", "metrics:read"],
                    "prefixes": ["tenanta:"],
                }
            ]
        ).decode(),
    )
    clear_auth_cache()

    response = await api.metrics(
        make_request(
            "GET",
            "/metrics",
            headers={"Authorization": "Bearer ops-static-token-00000000000000001"},
        )
    )

    assert response.status_code == 200


async def test_push_requires_bearer_token_when_auth_enabled(monkeypatch, caplog):
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
    )

    with caplog.at_level(logging.WARNING, logger="tonle.api"):
        response = await api.push(request)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert orjson.loads(response.body) == {"detail": "missing bearer token"}
    assert '"event":"auth_denied"' in caplog.text
    assert '"stream":"demo"' in caplog.text


async def test_push_rejects_oversized_bearer_token_before_verification(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "read-ticket-secret-000000000000001")
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": f"Bearer {'x' * 4097}"},
    )

    response = await api.push(request)

    assert response.status_code == 401
    assert orjson.loads(response.body) == {"detail": "invalid bearer token"}
    assert store.add_calls == []


async def test_push_rejects_when_write_rate_limit_is_reached(monkeypatch, caplog):
    store = RecordingStore()
    secret_token = "secret-token-that-must-not-be-logged"
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "write_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="write"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": secret_token, "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": f"Bearer {secret_token}"},
    )

    with caplog.at_level(logging.WARNING, logger="tonle.api"):
        first = await api.push(request)
        second = await api.push(request)

    assert first.status_code == 201
    assert second.status_code == 429
    assert 1 <= int(second.headers["retry-after"]) <= 60
    assert orjson.loads(second.body) == {"detail": "too many write requests"}
    assert store.add_calls == [("demo", {"n": 1})]
    assert 'tonle_rate_limit_rejections_total{kind="write"} 1' in api.app_metrics.render_prometheus()
    assert '"event":"rate_limit_rejected"' in caplog.text
    assert '"principal":"writer"' in caplog.text
    assert secret_token not in caplog.text
    assert "Bearer" not in caplog.text


async def test_push_many_rejects_when_write_rate_limit_is_reached(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setattr(
        api,
        "write_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="write"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events/batch",
        body=orjson.dumps({"events": [{"n": 1}]}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": "Bearer test-static-token-0000000000000001"},
    )

    first = await api.push_many(request)
    second = await api.push_many(request)

    assert first.status_code == 201
    assert second.status_code == 429
    assert orjson.loads(second.body) == {"detail": "too many write requests"}
    assert store.add_many_calls == [("demo", [{"n": 1}])]


async def test_auth_failures_are_rate_limited_by_client_ip(monkeypatch):
    monkeypatch.setattr(
        api,
        "auth_failure_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="auth-failure"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"X-Forwarded-For": "203.0.113.10"},
    )

    first = await api.push(request)
    second = await api.push(request)

    assert first.status_code == 401
    assert second.status_code == 429
    assert 1 <= int(second.headers["retry-after"]) <= 60
    assert orjson.loads(second.body) == {"detail": "too many auth-failure requests"}
    assert 'tonle_rate_limit_rejections_total{kind="auth-failure"} 1' in api.app_metrics.render_prometheus()


async def test_auth_failure_rate_limit_is_isolated_by_client_ip(monkeypatch):
    monkeypatch.setattr(api, "TRUST_FORWARDED_HEADERS", True)
    monkeypatch.setattr(
        api,
        "auth_failure_rate_limiter",
        api.FixedWindowRateLimiter(max_events=1, window_seconds=60, kind="auth-failure"),
    )
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()

    first = await api.push(
        make_request(
            "POST",
            "/streams/demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "demo"},
            headers={"X-Forwarded-For": "203.0.113.10"},
        )
    )
    second = await api.push(
        make_request(
            "POST",
            "/streams/demo/events",
            body=orjson.dumps({"data": {"n": 1}}),
            path_params={"stream_id": "demo"},
            headers={"X-Forwarded-For": "203.0.113.11"},
        )
    )

    assert first.status_code == 401
    assert second.status_code == 401


async def test_auth_denial_logs_do_not_include_bearer_token(monkeypatch, caplog):
    secret_token = "secret-token-that-must-not-be-logged"
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": "different-static-token-000000000001", "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": f"Bearer {secret_token}"},
    )

    with caplog.at_level(logging.WARNING, logger="tonle.api"):
        response = await api.push(request)

    assert response.status_code == 401
    assert '"event":"auth_denied"' in caplog.text
    assert secret_token not in caplog.text
    assert "Bearer" not in caplog.text


async def test_successful_auth_logs_do_not_include_bearer_token(monkeypatch, caplog):
    store = RecordingStore()
    secret_token = "secret-token-that-must-not-be-logged"
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "writer", "token": secret_token, "scopes": ["streams:write"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": f"Bearer {secret_token}"},
    )

    with caplog.at_level(logging.INFO, logger="tonle.api"):
        response = await api.push(request)

    assert response.status_code == 201
    assert secret_token not in caplog.text
    assert "Bearer" not in caplog.text


async def test_read_ticket_logs_do_not_include_ticket_contents(monkeypatch, caplog):
    secret = "read-ticket-secret-000000000000001"
    ticket = create_read_ticket(secret, "tenanta:demo", expires_at=4102444800)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", secret)
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    with caplog.at_level(logging.WARNING, logger="tonle.api"):
        response = await api.push(request)

    assert response.status_code == 403
    assert '"event":"auth_denied"' in caplog.text
    assert ticket not in caplog.text
    assert "Bearer" not in caplog.text


async def test_startup_config_log_does_not_include_redis_url_credentials(monkeypatch, caplog):
    redis_url = "redis://:redis-password@example.internal:6379/0"
    monkeypatch.setattr(api, "REDIS_URL", redis_url)
    monkeypatch.setattr(api.aioredis, "from_url", lambda *_, **__: FakeRedis())
    monkeypatch.delenv("TONLE_AUTH_TOKENS", raising=False)
    monkeypatch.delenv("TONLE_READ_TICKET_SECRET", raising=False)
    clear_auth_cache()

    with caplog.at_level(logging.INFO, logger="tonle.api"):
        async with api.lifespan(api.app):
            pass

    assert '"event":"startup_config"' in caplog.text
    assert '"redis_url_configured":true' in caplog.text
    assert redis_url not in caplog.text
    assert "redis-password" not in caplog.text


def test_require_auth_rejects_open_configuration(monkeypatch):
    monkeypatch.setenv("TONLE_REQUIRE_AUTH", "true")
    monkeypatch.delenv("TONLE_AUTH_TOKENS", raising=False)
    monkeypatch.delenv("TONLE_READ_TICKET_SECRET", raising=False)
    clear_auth_cache()

    with pytest.raises(ValueError, match="TONLE_REQUIRE_AUTH=true requires"):
        get_authenticator()


def test_require_auth_accepts_static_tokens(monkeypatch):
    monkeypatch.setenv("TONLE_REQUIRE_AUTH", "true")
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "reader", "token": "test-static-token-0000000000000001", "scopes": ["streams:read"]}]).decode(),
    )
    monkeypatch.delenv("TONLE_READ_TICKET_SECRET", raising=False)
    clear_auth_cache()

    assert get_authenticator() is not None


def test_require_auth_accepts_read_ticket_secret(monkeypatch):
    monkeypatch.setenv("TONLE_REQUIRE_AUTH", "true")
    monkeypatch.delenv("TONLE_AUTH_TOKENS", raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "read-ticket-secret-000000000000001")
    clear_auth_cache()

    assert get_authenticator() is not None


def test_previous_read_ticket_secrets_require_active_secret(monkeypatch):
    monkeypatch.delenv("TONLE_AUTH_TOKENS", raising=False)
    monkeypatch.delenv("TONLE_READ_TICKET_SECRET", raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_PREVIOUS_SECRETS", orjson.dumps(["old-read-ticket-secret-00000000001"]).decode())
    clear_auth_cache()

    with pytest.raises(ValueError, match="TONLE_READ_TICKET_PREVIOUS_SECRETS requires"):
        get_authenticator()


async def test_push_rejects_tokens_without_write_scope(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "reader", "token": "test-static-token-0000000000000001", "scopes": ["streams:read"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
        headers={"Authorization": "Bearer test-static-token-0000000000000001"},
    )

    response = await api.push(request)

    assert response.status_code == 403
    assert orjson.loads(response.body) == {"detail": "forbidden"}
    assert store.add_calls == []


async def test_push_rejects_streams_outside_allowed_prefix(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"], "prefixes": ["tenanta:"]}]
        ).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/tenant-b:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenant-b:demo"},
        headers={"Authorization": "Bearer test-static-token-0000000000000001"},
    )

    response = await api.push(request)

    assert response.status_code == 403
    assert orjson.loads(response.body) == {"detail": "forbidden"}
    assert store.add_calls == []


async def test_push_accepts_valid_scoped_token(monkeypatch):
    store = RecordingStore()
    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps(
            [{"name": "writer", "token": "test-static-token-0000000000000001", "scopes": ["streams:write"], "prefixes": ["tenanta:"]}]
        ).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": "Bearer test-static-token-0000000000000001"},
    )

    response = await api.push(request)

    assert response.status_code == 201
    assert orjson.loads(response.body) == {"id": "1-0"}
    assert store.add_calls == [("tenanta:demo", {"n": 1})]


async def test_info_accepts_sha256_token(monkeypatch):
    token = "test-static-token-0000000000000001"
    digest = hashlib.sha256(token.encode()).hexdigest()

    class InfoStore:
        async def length(self, stream_id):
            return 5

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv(
        "TONLE_AUTH_TOKENS",
        orjson.dumps([{"name": "reader", "token_sha256": digest, "scopes": ["streams:read"]}]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/demo",
        path_params={"stream_id": "demo"},
        headers={"Authorization": f"Bearer {token}"},
    )

    response = await api.info(request)

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"id": "demo", "length": 5}


async def test_info_accepts_valid_read_ticket(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    secret = "read-ticket-secret-000000000000001"
    ticket = create_read_ticket(secret, "tenanta:demo", expires_at=4102444800, subject="customer-1")

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", secret)
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:demo",
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"id": "tenanta:demo", "length": 5}


async def test_info_accepts_read_ticket_signed_with_previous_secret(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    ticket = create_read_ticket("old-read-ticket-secret-00000000001", "tenanta:demo", expires_at=4102444800)

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "new-read-ticket-secret-00000000001")
    monkeypatch.setenv(
        "TONLE_READ_TICKET_PREVIOUS_SECRETS",
        orjson.dumps(["old-read-ticket-secret-00000000001"]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:demo",
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"id": "tenanta:demo", "length": 5}


async def test_info_accepts_read_ticket_signed_with_active_secret_during_rotation(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    ticket = create_read_ticket("new-read-ticket-secret-00000000001", "tenanta:demo", expires_at=4102444800)

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "new-read-ticket-secret-00000000001")
    monkeypatch.setenv(
        "TONLE_READ_TICKET_PREVIOUS_SECRETS",
        orjson.dumps(["old-read-ticket-secret-00000000001"]).decode(),
    )
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:demo",
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 200
    assert orjson.loads(response.body) == {"id": "tenanta:demo", "length": 5}


def test_read_ticket_secrets_are_deduped_during_rotation(monkeypatch):
    monkeypatch.delenv("TONLE_AUTH_TOKENS", raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "read-ticket-secret-000000000000001")
    monkeypatch.setenv(
        "TONLE_READ_TICKET_PREVIOUS_SECRETS",
        orjson.dumps(["read-ticket-secret-000000000000001", "old-read-ticket-secret-00000000001"]).decode(),
    )
    clear_auth_cache()

    authenticator = get_authenticator()

    assert authenticator is not None
    assert authenticator.read_ticket_secrets == ("read-ticket-secret-000000000000001", "old-read-ticket-secret-00000000001")


async def test_info_rejects_read_ticket_after_previous_secret_is_removed(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    ticket = create_read_ticket("old-read-ticket-secret-00000000001", "tenanta:demo", expires_at=4102444800)

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "new-read-ticket-secret-00000000001")
    monkeypatch.delenv("TONLE_READ_TICKET_PREVIOUS_SECRETS", raising=False)
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:demo",
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 401
    assert orjson.loads(response.body) == {"detail": "invalid bearer token"}


async def test_info_rejects_read_ticket_for_wrong_stream(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    secret = "read-ticket-secret-000000000000001"
    ticket = create_read_ticket(secret, "tenanta:demo", expires_at=4102444800)

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", secret)
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:other",
        path_params={"stream_id": "tenanta:other"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 403
    assert orjson.loads(response.body) == {"detail": "forbidden"}


async def test_info_rejects_expired_read_ticket(monkeypatch):
    class InfoStore:
        async def length(self, stream_id):
            return 5

    secret = "read-ticket-secret-000000000000001"
    ticket = create_read_ticket(secret, "tenanta:demo", expires_at=1)

    monkeypatch.setattr(api, "store", InfoStore(), raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", secret)
    monkeypatch.setenv("TONLE_READ_TICKET_LEEWAY_SECONDS", "0")
    clear_auth_cache()
    request = make_request(
        "GET",
        "/streams/tenanta:demo",
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.info(request)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert orjson.loads(response.body) == {"detail": "expired bearer token"}


async def test_push_rejects_read_ticket_without_write_scope(monkeypatch):
    store = RecordingStore()
    secret = "read-ticket-secret-000000000000001"
    ticket = create_read_ticket(secret, "tenanta:demo", expires_at=4102444800)

    monkeypatch.setattr(api, "store", store, raising=False)
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", secret)
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/tenanta:demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "tenanta:demo"},
        headers={"Authorization": f"Bearer {ticket}"},
    )

    response = await api.push(request)

    assert response.status_code == 403
    assert orjson.loads(response.body) == {"detail": "forbidden"}
    assert store.add_calls == []


async def test_push_requires_bearer_token_when_only_read_tickets_are_enabled(monkeypatch):
    monkeypatch.setenv("TONLE_READ_TICKET_SECRET", "read-ticket-secret-000000000000001")
    clear_auth_cache()
    request = make_request(
        "POST",
        "/streams/demo/events",
        body=orjson.dumps({"data": {"n": 1}}),
        path_params={"stream_id": "demo"},
    )

    response = await api.push(request)

    assert response.status_code == 401
    assert response.headers["www-authenticate"] == "Bearer"
    assert orjson.loads(response.body) == {"detail": "missing bearer token"}
