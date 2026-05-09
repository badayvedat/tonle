import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import secrets
import time
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any

from starlette.requests import Request

from .config import _env_bool

_TOKEN_ENV = "TONLE_AUTH_TOKENS"
_READ_TICKET_SECRET_ENV = "TONLE_READ_TICKET_SECRET"
_READ_TICKET_PREVIOUS_SECRETS_ENV = "TONLE_READ_TICKET_PREVIOUS_SECRETS"
_READ_TICKET_LEEWAY_ENV = "TONLE_READ_TICKET_LEEWAY_SECONDS"
_REQUIRE_AUTH_ENV = "TONLE_REQUIRE_AUTH"
_VALID_SCOPES = {"streams:read", "streams:write", "streams:delete", "metrics:read"}
_AUTH_HEADERS = {"WWW-Authenticate": "Bearer"}
_READ_TICKET_SCOPE = "streams:read"
_READ_TICKET_VERSION = "v1"
_PREFIX_DELIMITERS = (":", "_", "-")
_MAX_BEARER_TOKEN_LENGTH = 4096
_MIN_SECRET_LENGTH = 32
_PRINCIPAL_NAME_RE = re.compile(r"^[A-Za-z0-9:_@.-]{1,128}$")


class AuthError(Exception):
    def __init__(self, status_code: int, detail: str, headers: dict[str, str] | None = None):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.headers = headers or {}


@dataclass(frozen=True)
class Principal:
    name: str
    scopes: frozenset[str]
    prefixes: tuple[str, ...]
    stream_ids: tuple[str, ...] = ()


@dataclass(frozen=True)
class _TokenEntry:
    principal: Principal
    token: str | None
    token_sha256: str | None

    def matches(self, provided_token: str) -> bool:
        if self.token is not None:
            return secrets.compare_digest(self.token, provided_token)
        digest = hashlib.sha256(provided_token.encode()).hexdigest()
        return secrets.compare_digest(self.token_sha256, digest)


@dataclass(frozen=True)
class Authenticator:
    entries: tuple[_TokenEntry, ...]
    read_ticket_secrets: tuple[str, ...] = ()
    read_ticket_leeway_seconds: int = 0

    def authorize(self, authorization: str | None, required_scope: str, stream_id: str) -> Principal:
        principal = self.authorize_global(authorization, required_scope)
        if principal.stream_ids and stream_id not in principal.stream_ids:
            raise AuthError(403, "forbidden")
        if principal.prefixes and not any(stream_id.startswith(prefix) for prefix in principal.prefixes):
            raise AuthError(403, "forbidden")
        return principal

    def authorize_global(self, authorization: str | None, required_scope: str) -> Principal:
        if authorization is None:
            raise AuthError(401, "missing bearer token", headers=_AUTH_HEADERS)

        scheme, _, token = authorization.partition(" ")
        if scheme.lower() != "bearer" or not token:
            raise AuthError(401, "invalid authorization header", headers=_AUTH_HEADERS)
        if len(token) > _MAX_BEARER_TOKEN_LENGTH:
            raise AuthError(401, "invalid bearer token", headers=_AUTH_HEADERS)

        principal = self._match_principal(token)
        if principal is None:
            raise AuthError(401, "invalid bearer token", headers=_AUTH_HEADERS)
        if required_scope not in principal.scopes:
            raise AuthError(403, "forbidden")
        return principal

    def _match_principal(self, token: str) -> Principal | None:
        for entry in self.entries:
            if entry.matches(token):
                return entry.principal
        for secret in self.read_ticket_secrets:
            principal = _read_ticket_principal(
                token,
                secret=secret,
                leeway_seconds=self.read_ticket_leeway_seconds,
            )
            if principal is not None:
                return principal
        return None


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(data: str) -> bytes:
    padding = "=" * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def _read_ticket_payload_bytes(payload: dict[str, Any]) -> bytes:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()


def _read_ticket_signature(secret: str, payload_segment: str) -> str:
    digest = hmac.new(
        secret.encode(),
        f"{_READ_TICKET_VERSION}.{payload_segment}".encode(),
        hashlib.sha256,
    ).digest()
    return _b64url_encode(digest)


def _coerce_expiry(expires_at: int | float | datetime) -> int:
    if isinstance(expires_at, datetime):
        return int(expires_at.timestamp())
    return int(expires_at)


def create_read_ticket(
    secret: str,
    stream_id: str,
    *,
    expires_at: int | float | datetime,
    subject: str | None = None,
) -> str:
    if not isinstance(secret, str) or len(secret) < _MIN_SECRET_LENGTH:
        raise ValueError(f"secret must be at least {_MIN_SECRET_LENGTH} characters")
    if not isinstance(stream_id, str) or not stream_id:
        raise ValueError("stream_id must be a non-empty string")
    if subject is not None and (
        not isinstance(subject, str) or not _PRINCIPAL_NAME_RE.fullmatch(subject)
    ):
        raise ValueError("subject must be 1-128 chars of [A-Za-z0-9:_@.-]")
    payload: dict[str, Any] = {
        "exp": _coerce_expiry(expires_at),
        "scope": _READ_TICKET_SCOPE,
        "stream_id": stream_id,
    }
    if subject is not None:
        payload["sub"] = subject
    payload_segment = _b64url_encode(_read_ticket_payload_bytes(payload))
    signature = _read_ticket_signature(secret, payload_segment)
    return f"{_READ_TICKET_VERSION}.{payload_segment}.{signature}"


def _read_ticket_principal(token: str, *, secret: str, leeway_seconds: int) -> Principal | None:
    version, sep, remainder = token.partition(".")
    if version != _READ_TICKET_VERSION or not sep:
        return None
    payload_segment, sep, signature = remainder.partition(".")
    if not payload_segment or not sep or not signature:
        return None

    expected_signature = _read_ticket_signature(secret, payload_segment)
    if not secrets.compare_digest(expected_signature, signature):
        return None

    try:
        payload = json.loads(_b64url_decode(payload_segment))
    except (binascii.Error, json.JSONDecodeError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None

    stream_id = payload.get("stream_id")
    scope = payload.get("scope")
    exp = payload.get("exp")
    if not isinstance(stream_id, str) or not stream_id:
        return None
    if scope != _READ_TICKET_SCOPE:
        return None
    if not isinstance(exp, int | float):
        return None
    if int(exp) + leeway_seconds < int(time.time()):
        raise AuthError(401, "expired bearer token", headers=_AUTH_HEADERS)

    subject = payload.get("sub")
    if subject is not None:
        if not isinstance(subject, str) or not _PRINCIPAL_NAME_RE.fullmatch(subject):
            return None
        principal_name = subject
    else:
        principal_name = "read-ticket"
    return Principal(
        name=principal_name,
        scopes=frozenset({_READ_TICKET_SCOPE}),
        prefixes=(),
        stream_ids=(stream_id,),
    )


def _parse_token_entry(index: int, item: object) -> _TokenEntry:
    if not isinstance(item, dict):
        raise ValueError(f"{_TOKEN_ENV}[{index}] must be an object")

    token = item.get("token")
    token_sha256 = item.get("token_sha256")
    if (token is None) == (token_sha256 is None):
        raise ValueError(
            f"{_TOKEN_ENV}[{index}] must define exactly one of 'token' or 'token_sha256'"
        )
    if token is not None:
        if not isinstance(token, str):
            raise ValueError(f"{_TOKEN_ENV}[{index}].token must be a string")
        if len(token) < _MIN_SECRET_LENGTH:
            raise ValueError(
                f"{_TOKEN_ENV}[{index}].token must be at least {_MIN_SECRET_LENGTH} characters"
            )
    if token_sha256 is not None:
        if not isinstance(token_sha256, str) or len(token_sha256) != 64:
            raise ValueError(f"{_TOKEN_ENV}[{index}].token_sha256 must be a 64-char hex string")
        token_sha256 = token_sha256.lower()
        if any(ch not in "0123456789abcdef" for ch in token_sha256):
            raise ValueError(f"{_TOKEN_ENV}[{index}].token_sha256 must be a 64-char hex string")

    scopes = item.get("scopes")
    if not isinstance(scopes, list) or not scopes or any(not isinstance(scope, str) for scope in scopes):
        raise ValueError(f"{_TOKEN_ENV}[{index}].scopes must be a non-empty array of strings")
    invalid_scopes = [scope for scope in scopes if scope not in _VALID_SCOPES]
    if invalid_scopes:
        raise ValueError(
            f"{_TOKEN_ENV}[{index}].scopes contains unknown scopes: {', '.join(invalid_scopes)}"
        )

    prefixes = item.get("prefixes", [])
    if not isinstance(prefixes, list) or any(not isinstance(prefix, str) for prefix in prefixes):
        raise ValueError(f"{_TOKEN_ENV}[{index}].prefixes must be an array of strings")
    invalid_prefixes = [
        prefix
        for prefix in prefixes
        if not prefix or not prefix.endswith(_PREFIX_DELIMITERS)
    ]
    if invalid_prefixes:
        raise ValueError(
            f"{_TOKEN_ENV}[{index}].prefixes entries must be non-empty and end with one of ':', '_', '-'"
        )

    name = item.get("name", f"token-{index + 1}")
    if not isinstance(name, str) or not _PRINCIPAL_NAME_RE.fullmatch(name):
        raise ValueError(f"{_TOKEN_ENV}[{index}].name must be 1-128 chars of [A-Za-z0-9:_@.-]")

    principal = Principal(
        name=name,
        scopes=frozenset(scopes),
        prefixes=tuple(prefixes),
    )
    return _TokenEntry(principal=principal, token=token, token_sha256=token_sha256)


def _parse_previous_read_ticket_secrets(raw_secrets: str | None) -> tuple[str, ...]:
    if raw_secrets is None or not raw_secrets.strip():
        return ()
    try:
        parsed = json.loads(raw_secrets)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{_READ_TICKET_PREVIOUS_SECRETS_ENV} must be valid JSON") from exc
    if not isinstance(parsed, list):
        raise ValueError(f"{_READ_TICKET_PREVIOUS_SECRETS_ENV} must be a JSON array")
    for index, secret in enumerate(parsed):
        if not isinstance(secret, str) or not secret:
            raise ValueError(
                f"{_READ_TICKET_PREVIOUS_SECRETS_ENV}[{index}] must be a non-empty string"
            )
        if len(secret) < _MIN_SECRET_LENGTH:
            raise ValueError(
                f"{_READ_TICKET_PREVIOUS_SECRETS_ENV}[{index}] must be at least {_MIN_SECRET_LENGTH} characters"
            )
    return tuple(parsed)


@lru_cache
def get_authenticator() -> Authenticator | None:
    raw_tokens = os.getenv(_TOKEN_ENV)
    entries: tuple[_TokenEntry, ...] = ()
    if raw_tokens is not None and raw_tokens.strip():
        try:
            parsed = json.loads(raw_tokens)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{_TOKEN_ENV} must be valid JSON") from exc

        if not isinstance(parsed, list) or not parsed:
            raise ValueError(f"{_TOKEN_ENV} must be a non-empty JSON array")
        entries = tuple(_parse_token_entry(index, item) for index, item in enumerate(parsed))

    read_ticket_secret = os.getenv(_READ_TICKET_SECRET_ENV)
    if read_ticket_secret is not None and not read_ticket_secret.strip():
        read_ticket_secret = None
    if read_ticket_secret is not None and len(read_ticket_secret) < _MIN_SECRET_LENGTH:
        raise ValueError(f"{_READ_TICKET_SECRET_ENV} must be at least {_MIN_SECRET_LENGTH} characters")
    previous_read_ticket_secrets = _parse_previous_read_ticket_secrets(
        os.getenv(_READ_TICKET_PREVIOUS_SECRETS_ENV)
    )
    if read_ticket_secret is None and previous_read_ticket_secrets:
        raise ValueError(
            f"{_READ_TICKET_PREVIOUS_SECRETS_ENV} requires {_READ_TICKET_SECRET_ENV}"
        )

    leeway_raw = os.getenv(_READ_TICKET_LEEWAY_ENV, "5")
    try:
        read_ticket_leeway_seconds = int(leeway_raw)
    except ValueError as exc:
        raise ValueError(f"{_READ_TICKET_LEEWAY_ENV} must be an integer") from exc
    if read_ticket_leeway_seconds < 0:
        raise ValueError(f"{_READ_TICKET_LEEWAY_ENV} must be at least 0")

    read_ticket_secrets = (
        tuple(dict.fromkeys((read_ticket_secret, *previous_read_ticket_secrets)))
        if read_ticket_secret is not None
        else ()
    )

    if not entries and not read_ticket_secrets:
        if _env_bool(_REQUIRE_AUTH_ENV):
            raise ValueError(
                f"{_REQUIRE_AUTH_ENV}=true requires {_TOKEN_ENV} or "
                f"{_READ_TICKET_SECRET_ENV}"
            )
        return None

    return Authenticator(
        entries=entries,
        read_ticket_secrets=read_ticket_secrets,
        read_ticket_leeway_seconds=read_ticket_leeway_seconds,
    )


def clear_auth_cache() -> None:
    get_authenticator.cache_clear()


def authorize_request(request: Request, required_scope: str, stream_id: str) -> Principal | None:
    authenticator = get_authenticator()
    if authenticator is None:
        return None
    principal = authenticator.authorize(
        request.headers.get("authorization"),
        required_scope=required_scope,
        stream_id=stream_id,
    )
    request.state.principal = principal
    return principal


def authorize_global_request(request: Request, required_scope: str) -> Principal | None:
    authenticator = get_authenticator()
    if authenticator is None:
        return None
    principal = authenticator.authorize_global(
        request.headers.get("authorization"),
        required_scope=required_scope,
    )
    request.state.principal = principal
    return principal
