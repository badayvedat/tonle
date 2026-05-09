import os


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.lower() in {"1", "true", "yes", "on"}


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int | None = None,
    maximum: int | None = None,
) -> int:
    raw = os.getenv(name)
    if raw is None:
        value = default
    else:
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(f"{name} must be an integer") from exc
    if minimum is not None and value < minimum:
        raise ValueError(f"{name} must be at least {minimum}")
    if maximum is not None and value > maximum:
        raise ValueError(f"{name} must be at most {maximum}")
    return value


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
STREAM_TTL = _env_int("STREAM_TTL", 600, minimum=1)    # 10m
STREAM_MAXLEN = _env_int("STREAM_MAXLEN", 10000, minimum=1)
MAX_REQUEST_BYTES = _env_int("TONLE_MAX_REQUEST_BYTES", 1048576, minimum=1)
REQUEST_BODY_READ_TIMEOUT_SECONDS = _env_int(
    "TONLE_REQUEST_BODY_READ_TIMEOUT_SECONDS",
    30,
    minimum=1,
)
MAX_EVENT_BYTES = _env_int("TONLE_MAX_EVENT_BYTES", 65536, minimum=1)
SSE_POLL_INTERVAL = _env_int("TONLE_SSE_POLL_INTERVAL", 5, minimum=1)
MAX_ACTIVE_SSE_CONNECTIONS = _env_int("TONLE_MAX_ACTIVE_SSE_CONNECTIONS", 1000, minimum=1)
MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM = _env_int("TONLE_MAX_ACTIVE_SSE_CONNECTIONS_PER_STREAM", 100, minimum=1)
MAX_ACTIVE_POLLS = _env_int("TONLE_MAX_ACTIVE_POLLS", 1000, minimum=1)
MAX_ACTIVE_POLLS_PER_STREAM = _env_int("TONLE_MAX_ACTIVE_POLLS_PER_STREAM", 100, minimum=1)
RATE_LIMIT_WINDOW_SECONDS = _env_int("TONLE_RATE_LIMIT_WINDOW_SECONDS", 60, minimum=1)
RATE_LIMIT_REQUESTS_PER_WINDOW = _env_int("TONLE_RATE_LIMIT_REQUESTS_PER_WINDOW", 0, minimum=0)
RATE_LIMIT_WRITES_PER_WINDOW = _env_int("TONLE_RATE_LIMIT_WRITES_PER_WINDOW", 0, minimum=0)
RATE_LIMIT_AUTH_FAILURES_PER_WINDOW = _env_int("TONLE_RATE_LIMIT_AUTH_FAILURES_PER_WINDOW", 0, minimum=0)
TRUST_FORWARDED_HEADERS = _env_bool("TONLE_TRUST_FORWARDED_HEADERS", default=False)
HOST = os.getenv("TONLE_HOST", "0.0.0.0")
PORT = _env_int("TONLE_PORT", 8000, minimum=1, maximum=65535)
RELOAD = _env_bool("TONLE_RELOAD", default=False)
