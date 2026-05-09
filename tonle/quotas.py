import json
import math
import os
import time
import uuid
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

_TENANT_QUOTAS_ENV = "TONLE_TENANT_QUOTAS"
_DEFAULT_POLICY = "default"
_KEY_PREFIX = "tonle:quota"
_TTL_BUFFER_SECONDS = 5
_INCR_WITH_EXPIRE_SCRIPT = """
local value = redis.call("INCRBY", KEYS[1], ARGV[1])
if value == tonumber(ARGV[1]) then
  redis.call("EXPIRE", KEYS[1], ARGV[2])
end
return value
"""
_INCR_MANY_WITH_EXPIRE_SCRIPT = """
local key_count = #KEYS
local ttl_seconds = tonumber(ARGV[1])
for i = 1, key_count do
  local amount = tonumber(ARGV[1 + i])
  local limit = tonumber(ARGV[1 + key_count + i])
  if limit > 0 and amount > 0 then
    local value = redis.call("INCRBY", KEYS[i], amount)
    if value == amount then
      redis.call("EXPIRE", KEYS[i], ttl_seconds)
    end
    if value > limit then
      return i
    end
  end
end
return 0
"""
_CONNECTION_ACQUIRE_SCRIPT = """
local lease_id = ARGV[1]
local now_ms = tonumber(ARGV[2])
local expires_at_ms = tonumber(ARGV[3])
local ttl_seconds = tonumber(ARGV[4])
local key_count = #KEYS

for i = 1, key_count do
  redis.call("ZREMRANGEBYSCORE", KEYS[i], "-inf", now_ms)
end

for i = 1, key_count do
  local limit = tonumber(ARGV[4 + i])
  if limit > 0 and redis.call("ZCARD", KEYS[i]) >= limit then
    return i
  end
end

for i = 1, key_count do
  redis.call("ZADD", KEYS[i], expires_at_ms, lease_id)
  redis.call("EXPIRE", KEYS[i], ttl_seconds)
end
return 0
"""
_CONNECTION_REFRESH_SCRIPT = """
local lease_id = ARGV[1]
local expires_at_ms = tonumber(ARGV[2])
local ttl_seconds = tonumber(ARGV[3])
for i = 1, #KEYS do
  if redis.call("ZSCORE", KEYS[i], lease_id) then
    redis.call("ZADD", KEYS[i], expires_at_ms, lease_id)
    redis.call("EXPIRE", KEYS[i], ttl_seconds)
  end
end
return 0
"""
_CONNECTION_RELEASE_SCRIPT = """
local lease_id = ARGV[1]
for i = 1, #KEYS do
  redis.call("ZREM", KEYS[i], lease_id)
end
return 0
"""


class TenantQuotaExceeded(Exception):
    def __init__(
        self,
        detail: str,
        quota: str,
        *,
        status_code: int = 429,
        retry_after: int | None = None,
    ):
        super().__init__(detail)
        self.detail = detail
        self.quota = quota
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass(frozen=True)
class TenantQuotaPolicy:
    window_seconds: int = 60
    requests_per_window: int = 0
    writes_per_window: int = 0
    write_bytes_per_window: int = 0
    max_event_bytes: int = 0
    max_batch_events: int = 0
    max_sse_connections: int = 0
    max_sse_connections_per_stream: int = 0
    max_sse_connections_per_principal: int = 0
    max_long_polls: int = 0
    max_long_polls_per_stream: int = 0
    max_long_polls_per_principal: int = 0
    connection_lease_ttl_seconds: int = 30


@dataclass(frozen=True)
class ConnectionLease:
    lease_id: str
    keys: tuple[str, ...]
    ttl_seconds: int


@dataclass(frozen=True)
class TenantQuotaConfig:
    policies: dict[str, TenantQuotaPolicy]

    def policy_for(self, tenant: str) -> TenantQuotaPolicy | None:
        return self.policies.get(tenant) or self.policies.get(_DEFAULT_POLICY)


class RedisTenantQuotaLimiter:
    def __init__(self, redis, *, clock=time.time):
        self._redis = redis
        self._clock = clock

    async def check(self, *, tenant: str, quota: str, amount: int, limit: int, window_seconds: int) -> None:
        if limit <= 0 or amount <= 0:
            return
        now = self._clock()
        window = int(now // window_seconds)
        retry_after = max(1, int(math.ceil(((window + 1) * window_seconds) - now)))
        key = f"{_KEY_PREFIX}:{quota}:{tenant}:{window}"
        value = await self._redis.eval(
            _INCR_WITH_EXPIRE_SCRIPT,
            1,
            key,
            amount,
            window_seconds + _TTL_BUFFER_SECONDS,
        )
        if int(value) > limit:
            raise TenantQuotaExceeded(
                f"tenant {quota} quota exceeded",
                quota=quota,
                retry_after=retry_after,
            )

    async def check_many(
        self,
        *,
        tenant: str,
        checks: list[tuple[str, int, int]],
        window_seconds: int,
    ) -> None:
        active_checks = [
            (quota, amount, limit)
            for quota, amount, limit in checks
            if limit > 0 and amount > 0
        ]
        if not active_checks:
            return
        now = self._clock()
        window = int(now // window_seconds)
        retry_after = max(1, int(math.ceil(((window + 1) * window_seconds) - now)))
        keys = [
            f"{_KEY_PREFIX}:{quota}:{tenant}:{window}"
            for quota, _, _ in active_checks
        ]
        amounts = [amount for _, amount, _ in active_checks]
        limits = [limit for _, _, limit in active_checks]
        exceeded_index = int(
            await self._redis.eval(
                _INCR_MANY_WITH_EXPIRE_SCRIPT,
                len(keys),
                *keys,
                window_seconds + _TTL_BUFFER_SECONDS,
                *amounts,
                *limits,
            )
        )
        if exceeded_index:
            quota = active_checks[exceeded_index - 1][0]
            raise TenantQuotaExceeded(
                f"tenant {quota} quota exceeded",
                quota=quota,
                retry_after=retry_after,
            )


class RedisConnectionLeaseLimiter:
    def __init__(self, redis, *, clock=time.time):
        self._redis = redis
        self._clock = clock

    async def acquire(
        self,
        *,
        kind: str,
        tenant: str,
        stream_id: str,
        principal: str,
        policy: TenantQuotaPolicy,
    ) -> ConnectionLease | None:
        limits = _connection_limits(kind, policy)
        if all(limit <= 0 for limit in limits):
            return None
        ttl_seconds = policy.connection_lease_ttl_seconds
        lease = ConnectionLease(
            lease_id=f"{uuid.uuid4().hex}",
            keys=_connection_keys(kind, tenant, stream_id, principal),
            ttl_seconds=ttl_seconds,
        )
        now_ms = int(self._clock() * 1000)
        exceeded_index = int(
            await self._redis.eval(
                _CONNECTION_ACQUIRE_SCRIPT,
                len(lease.keys),
                *lease.keys,
                lease.lease_id,
                now_ms,
                now_ms + (ttl_seconds * 1000),
                ttl_seconds + _TTL_BUFFER_SECONDS,
                *limits,
            )
        )
        if exceeded_index:
            quota = _connection_quota_name(kind, exceeded_index)
            raise TenantQuotaExceeded(
                f"tenant {quota} quota exceeded",
                quota=quota,
                retry_after=ttl_seconds,
            )
        return lease

    async def refresh(self, lease: ConnectionLease | None) -> None:
        if lease is None:
            return
        expires_at_ms = int((self._clock() + lease.ttl_seconds) * 1000)
        await self._redis.eval(
            _CONNECTION_REFRESH_SCRIPT,
            len(lease.keys),
            *lease.keys,
            lease.lease_id,
            expires_at_ms,
            lease.ttl_seconds + _TTL_BUFFER_SECONDS,
        )

    async def release(self, lease: ConnectionLease | None) -> None:
        if lease is None:
            return
        await self._redis.eval(
            _CONNECTION_RELEASE_SCRIPT,
            len(lease.keys),
            *lease.keys,
            lease.lease_id,
        )


def tenant_from_stream_id(stream_id: str) -> str:
    separator_indexes = [
        index
        for index in (stream_id.find(":"), stream_id.find("_"), stream_id.find("-"))
        if index >= 0
    ]
    if separator_indexes:
        return stream_id[:min(separator_indexes)]
    return stream_id


def _read_int(item: dict[str, Any], name: str, default: int = 0) -> int:
    value = item.get(name, default)
    if not isinstance(value, int) or value < 0:
        raise ValueError(f"{_TENANT_QUOTAS_ENV}.{name} must be a non-negative integer")
    return value


def _connection_keys(kind: str, tenant: str, stream_id: str, principal: str) -> tuple[str, ...]:
    return (
        f"tonle:conn:{kind}:global",
        f"tonle:conn:{kind}:tenant:{tenant}",
        f"tonle:conn:{kind}:stream:{stream_id}",
        f"tonle:conn:{kind}:principal:{principal}",
    )


def _connection_limits(kind: str, policy: TenantQuotaPolicy) -> tuple[int, int, int, int]:
    if kind == "sse":
        return (
            0,
            policy.max_sse_connections,
            policy.max_sse_connections_per_stream,
            policy.max_sse_connections_per_principal,
        )
    if kind == "long-poll":
        return (
            0,
            policy.max_long_polls,
            policy.max_long_polls_per_stream,
            policy.max_long_polls_per_principal,
        )
    raise ValueError(f"unknown connection kind: {kind}")


def _connection_quota_name(kind: str, exceeded_index: int) -> str:
    scope = ("global", "tenant", "stream", "principal")[exceeded_index - 1]
    return f"{kind}-connections-{scope}"


def _parse_policy(name: str, item: object, *, base: TenantQuotaPolicy | None = None) -> TenantQuotaPolicy:
    if not isinstance(item, dict):
        raise ValueError(f"{_TENANT_QUOTAS_ENV}.{name} must be an object")
    base = base or TenantQuotaPolicy()
    window_seconds = _read_int(item, "window_seconds", base.window_seconds)
    if window_seconds <= 0:
        raise ValueError(f"{_TENANT_QUOTAS_ENV}.{name}.window_seconds must be a positive integer")
    connection_lease_ttl_seconds = _read_int(
        item,
        "connection_lease_ttl_seconds",
        base.connection_lease_ttl_seconds,
    )
    if connection_lease_ttl_seconds <= 0:
        raise ValueError(
            f"{_TENANT_QUOTAS_ENV}.{name}.connection_lease_ttl_seconds must be a positive integer"
        )
    return TenantQuotaPolicy(
        window_seconds=window_seconds,
        requests_per_window=_read_int(item, "requests_per_window", base.requests_per_window),
        writes_per_window=_read_int(item, "writes_per_window", base.writes_per_window),
        write_bytes_per_window=_read_int(
            item,
            "write_bytes_per_window",
            base.write_bytes_per_window,
        ),
        max_event_bytes=_read_int(item, "max_event_bytes", base.max_event_bytes),
        max_batch_events=_read_int(item, "max_batch_events", base.max_batch_events),
        max_sse_connections=_read_int(item, "max_sse_connections", base.max_sse_connections),
        max_sse_connections_per_stream=_read_int(
            item,
            "max_sse_connections_per_stream",
            base.max_sse_connections_per_stream,
        ),
        max_sse_connections_per_principal=_read_int(
            item,
            "max_sse_connections_per_principal",
            base.max_sse_connections_per_principal,
        ),
        max_long_polls=_read_int(item, "max_long_polls", base.max_long_polls),
        max_long_polls_per_stream=_read_int(
            item,
            "max_long_polls_per_stream",
            base.max_long_polls_per_stream,
        ),
        max_long_polls_per_principal=_read_int(
            item,
            "max_long_polls_per_principal",
            base.max_long_polls_per_principal,
        ),
        connection_lease_ttl_seconds=connection_lease_ttl_seconds,
    )


@lru_cache
def get_tenant_quota_config() -> TenantQuotaConfig | None:
    raw_config = os.getenv(_TENANT_QUOTAS_ENV)
    if raw_config is None or not raw_config.strip():
        return None
    try:
        parsed = json.loads(raw_config)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{_TENANT_QUOTAS_ENV} must be valid JSON") from exc
    if not isinstance(parsed, dict) or not parsed:
        raise ValueError(f"{_TENANT_QUOTAS_ENV} must be a non-empty JSON object")
    if any(not isinstance(name, str) or not name for name in parsed):
        raise ValueError(f"{_TENANT_QUOTAS_ENV} keys must be non-empty strings")
    default_policy = _parse_policy(_DEFAULT_POLICY, parsed[_DEFAULT_POLICY]) if _DEFAULT_POLICY in parsed else None
    policies = {}
    if default_policy is not None:
        policies[_DEFAULT_POLICY] = default_policy
    for name, item in parsed.items():
        if name == _DEFAULT_POLICY:
            continue
        policies[name] = _parse_policy(name, item, base=default_policy)
    return TenantQuotaConfig(policies=policies)


def clear_tenant_quota_cache() -> None:
    get_tenant_quota_config.cache_clear()
