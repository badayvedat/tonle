import asyncio
import os
import time
from collections.abc import AsyncGenerator, Generator
from typing import Any

import httpx
import orjson

TONLE_URL = os.getenv("TONLE_URL", "http://localhost:8000")

# Server long-polls for up to 30s, so read timeout needs headroom.
_TIMEOUT = httpx.Timeout(65.0)
_READ_RETRIES = 2
_RETRY_BACKOFF_SECONDS = 0.25


class StreamError(RuntimeError):
    pass


class StreamTransportError(StreamError):
    def __init__(self, detail: str, *, cause: Exception):
        super().__init__(detail)
        self.detail = detail


class StreamHTTPError(StreamError):
    def __init__(
        self,
        status_code: int,
        detail: str,
        *,
        response: httpx.Response,
        retry_after: str | None = None,
    ):
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.response = response
        self.retry_after = retry_after


class StreamUnauthorizedError(StreamHTTPError):
    pass


class StreamForbiddenError(StreamHTTPError):
    pass


class StreamPayloadTooLargeError(StreamHTTPError):
    pass


class StreamRateLimitedError(StreamHTTPError):
    pass


class StreamServerError(StreamHTTPError):
    pass


class StreamTrimmedError(StreamError):
    def __init__(self, last_id: str, first_available_id: str, detail: str):
        super().__init__(detail)
        self.last_id = last_id
        self.first_available_id = first_available_id
        self.detail = detail


def _timeout(
    timeout: float | httpx.Timeout | None = None,
    *,
    connect_timeout: float | None = None,
    read_timeout: float | None = None,
    write_timeout: float | None = None,
    pool_timeout: float | None = None,
) -> httpx.Timeout:
    if timeout is not None and any(
        value is not None
        for value in (connect_timeout, read_timeout, write_timeout, pool_timeout)
    ):
        raise ValueError("timeout cannot be combined with connect/read/write/pool timeouts")
    if isinstance(timeout, httpx.Timeout):
        return timeout
    if timeout is not None:
        return httpx.Timeout(timeout)
    if any(
        value is not None
        for value in (connect_timeout, read_timeout, write_timeout, pool_timeout)
    ):
        return httpx.Timeout(
            connect=connect_timeout if connect_timeout is not None else _TIMEOUT.connect,
            read=read_timeout if read_timeout is not None else _TIMEOUT.read,
            write=write_timeout if write_timeout is not None else _TIMEOUT.write,
            pool=pool_timeout if pool_timeout is not None else _TIMEOUT.pool,
        )
    return _TIMEOUT


def _response_detail(response: httpx.Response) -> str:
    try:
        body: Any = orjson.loads(response.content)
    except orjson.JSONDecodeError:
        return response.reason_phrase or f"HTTP {response.status_code}"
    if isinstance(body, dict):
        detail = body.get("detail")
        if isinstance(detail, str) and detail:
            return detail
    return response.reason_phrase or f"HTTP {response.status_code}"


def _raise_for_status(response: httpx.Response) -> None:
    if response.status_code < 400:
        return
    detail = _response_detail(response)
    retry_after = response.headers.get("retry-after")
    if response.status_code == 401:
        raise StreamUnauthorizedError(response.status_code, detail, response=response)
    if response.status_code == 403:
        raise StreamForbiddenError(response.status_code, detail, response=response)
    if response.status_code == 413:
        raise StreamPayloadTooLargeError(response.status_code, detail, response=response)
    if response.status_code == 429:
        raise StreamRateLimitedError(
            response.status_code,
            detail,
            response=response,
            retry_after=retry_after,
        )
    if response.status_code >= 500:
        raise StreamServerError(response.status_code, detail, response=response)
    raise StreamHTTPError(response.status_code, detail, response=response)


def _transport_error(exc: httpx.RequestError) -> StreamTransportError:
    return StreamTransportError(str(exc) or exc.__class__.__name__, cause=exc)


def _raise_for_trim_gap(response: httpx.Response) -> None:
    if response.status_code != 409:
        return
    body = orjson.loads(response.content)
    raise StreamTrimmedError(
        last_id=body["last_id"],
        first_available_id=body["first_available_id"],
        detail=body["detail"],
    )


class _SSEParser:
    def __init__(self):
        self._event_type = "message"
        self._event_id: str | None = None
        self._data_lines: list[str] = []

    def feed_line(self, line: str) -> dict | None:
        if line == "":
            if not self._data_lines or self._event_id is None:
                self._event_type = "message"
                self._event_id = None
                self._data_lines = []
                return None
            event = {
                "event": self._event_type,
                "id": self._event_id,
                "data": orjson.loads("\n".join(self._data_lines)),
            }
            self._event_type = "message"
            self._event_id = None
            self._data_lines = []
            return event
        if line.startswith(":"):
            return None

        field, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]

        if field == "id":
            self._event_id = value
        elif field == "event":
            self._event_type = value or "message"
        elif field == "data":
            self._data_lines.append(value)
        return None


def _has_header(headers: dict[str, str], name: str) -> bool:
    lowered = name.lower()
    return any(header_name.lower() == lowered for header_name in headers)


class BufferedStreamWriter:
    def __init__(self, stream: "Stream", max_batch_size: int = 100):
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be at least 1")
        self._stream = stream
        self._max_batch_size = max_batch_size
        self._buffer: list[dict] = []

    def put(self, data: dict) -> list[str]:
        self._buffer.append(data)
        if len(self._buffer) < self._max_batch_size:
            return []
        return self.flush()

    def flush(self) -> list[str]:
        if not self._buffer:
            return []
        batch = self._buffer
        result = self._stream.put_many(batch)
        self._buffer = []
        return result

    def close(self) -> list[str]:
        return self.flush()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, *_):
        if exc_type is None:
            self.close()


class AsyncBufferedStreamWriter:
    def __init__(self, stream: "AsyncStream", max_batch_size: int = 100):
        if max_batch_size < 1:
            raise ValueError("max_batch_size must be at least 1")
        self._stream = stream
        self._max_batch_size = max_batch_size
        self._buffer: list[dict] = []

    async def put(self, data: dict) -> list[str]:
        self._buffer.append(data)
        if len(self._buffer) < self._max_batch_size:
            return []
        return await self.flush()

    async def flush(self) -> list[str]:
        if not self._buffer:
            return []
        batch = self._buffer
        result = await self._stream.put_many(batch)
        self._buffer = []
        return result

    async def aclose(self) -> list[str]:
        return await self.flush()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, *_):
        if exc_type is None:
            await self.aclose()


class Stream:
    def __init__(
        self,
        stream_id: str,
        base_url: str = TONLE_URL,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | httpx.Timeout | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
        read_retries: int = _READ_RETRIES,
        retry_backoff: float = _RETRY_BACKOFF_SECONDS,
    ):
        if read_retries < 0:
            raise ValueError("read_retries must be at least 0")
        if retry_backoff < 0:
            raise ValueError("retry_backoff must be at least 0")
        self._id = stream_id
        self._token = token
        self._headers = dict(headers or {})
        self._read_retries = read_retries
        self._retry_backoff = retry_backoff
        self._client = httpx.Client(
            base_url=base_url,
            timeout=_timeout(
                timeout,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                pool_timeout=pool_timeout,
            ),
        )

    def _request_headers(self, extra_headers: dict[str, str] | None = None) -> dict[str, str]:
        headers = dict(self._headers)
        if self._token is not None and not _has_header(headers, "authorization"):
            headers["Authorization"] = f"Bearer {self._token}"
        if extra_headers:
            headers.update(extra_headers)
        return headers

    def put(self, data: dict) -> str:
        try:
            r = self._client.post(
                f"/streams/{self._id}/events",
                content=orjson.dumps({"data": data}),
                headers=self._request_headers({"Content-Type": "application/json"}),
            )
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)
        return orjson.loads(r.content)["id"]

    def _sleep_before_retry(self, attempt: int) -> None:
        if self._retry_backoff:
            time.sleep(self._retry_backoff * (2 ** attempt))

    def _get_with_read_retries(self, path: str, **kwargs) -> httpx.Response:
        for attempt in range(self._read_retries + 1):
            try:
                response = self._client.get(path, **kwargs)
                _raise_for_trim_gap(response)
                _raise_for_status(response)
                return response
            except (httpx.RequestError, StreamServerError) as exc:
                if attempt >= self._read_retries:
                    if isinstance(exc, httpx.RequestError):
                        raise _transport_error(exc) from exc
                    raise
                self._sleep_before_retry(attempt)
        raise AssertionError("unreachable")

    def events(self, last_id: str = "0", *, transport: str = "sse") -> Generator[dict, None, None]:
        if transport == "sse":
            yield from self._sse_events(last_id=last_id)
            return
        if transport == "long-poll":
            yield from self._long_poll_events(last_id=last_id)
            return
        raise ValueError("transport must be 'sse' or 'long-poll'")

    def _long_poll_events(self, last_id: str = "0") -> Generator[dict, None, None]:
        while True:
            r = self._get_with_read_retries(
                f"/streams/{self._id}/events",
                params={"last_id": last_id, "timeout": 30},
                headers=self._request_headers(),
            )
            if r.status_code == 204:
                continue
            body = orjson.loads(r.content)
            for event in body["events"]:
                yield event
            last_id = body["last_id"]

    def _sse_events(self, last_id: str = "0") -> Generator[dict, None, None]:
        current_last_id = last_id
        while True:
            for attempt in range(self._read_retries + 1):
                headers = {}
                if current_last_id != "0":
                    headers["Last-Event-ID"] = current_last_id
                try:
                    with self._client.stream(
                        "GET",
                        f"/streams/{self._id}/events/sse",
                        params={"timeout": 30},
                        headers=self._request_headers(headers),
                    ) as r:
                        if r.status_code >= 400:
                            r.read()
                        _raise_for_trim_gap(r)
                        _raise_for_status(r)
                        parser = _SSEParser()
                        for line in r.iter_lines():
                            event = parser.feed_line(line)
                            if event is None:
                                continue
                            if event["event"] == "gap":
                                raise StreamTrimmedError(
                                    last_id=event["data"]["last_id"],
                                    first_available_id=event["data"]["first_available_id"],
                                    detail=event["data"]["detail"],
                                )
                            current_last_id = event["id"]
                            yield {"id": event["id"], "data": event["data"]}
                    break
                except (httpx.RequestError, StreamServerError) as exc:
                    if attempt >= self._read_retries:
                        if isinstance(exc, httpx.RequestError):
                            raise _transport_error(exc) from exc
                        raise
                    self._sleep_before_retry(attempt)

    def put_many(self, events: list[dict]) -> list[str]:
        try:
            r = self._client.post(
                f"/streams/{self._id}/events/batch",
                content=orjson.dumps({"events": events}),
                headers=self._request_headers({"Content-Type": "application/json"}),
            )
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)
        return orjson.loads(r.content)["ids"]

    def buffered(self, max_batch_size: int = 100) -> BufferedStreamWriter:
        return BufferedStreamWriter(self, max_batch_size=max_batch_size)

    def delete(self) -> None:
        try:
            r = self._client.delete(f"/streams/{self._id}", headers=self._request_headers())
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)

    def close(self) -> None:
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


class AsyncStream:
    def __init__(
        self,
        stream_id: str,
        base_url: str = TONLE_URL,
        *,
        token: str | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | httpx.Timeout | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
        read_retries: int = _READ_RETRIES,
        retry_backoff: float = _RETRY_BACKOFF_SECONDS,
    ):
        if read_retries < 0:
            raise ValueError("read_retries must be at least 0")
        if retry_backoff < 0:
            raise ValueError("retry_backoff must be at least 0")
        self._id = stream_id
        self._token = token
        self._headers = dict(headers or {})
        self._read_retries = read_retries
        self._retry_backoff = retry_backoff
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=_timeout(
                timeout,
                connect_timeout=connect_timeout,
                read_timeout=read_timeout,
                write_timeout=write_timeout,
                pool_timeout=pool_timeout,
            ),
        )

    def _request_headers(self, extra_headers: dict[str, str] | None = None) -> dict[str, str]:
        headers = dict(self._headers)
        if self._token is not None and not _has_header(headers, "authorization"):
            headers["Authorization"] = f"Bearer {self._token}"
        if extra_headers:
            headers.update(extra_headers)
        return headers

    async def put(self, data: dict) -> str:
        try:
            r = await self._client.post(
                f"/streams/{self._id}/events",
                content=orjson.dumps({"data": data}),
                headers=self._request_headers({"Content-Type": "application/json"}),
            )
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)
        return orjson.loads(r.content)["id"]

    async def _sleep_before_retry(self, attempt: int) -> None:
        if self._retry_backoff:
            await asyncio.sleep(self._retry_backoff * (2 ** attempt))

    async def _get_with_read_retries(self, path: str, **kwargs) -> httpx.Response:
        for attempt in range(self._read_retries + 1):
            try:
                response = await self._client.get(path, **kwargs)
                _raise_for_trim_gap(response)
                _raise_for_status(response)
                return response
            except (httpx.RequestError, StreamServerError) as exc:
                if attempt >= self._read_retries:
                    if isinstance(exc, httpx.RequestError):
                        raise _transport_error(exc) from exc
                    raise
                await self._sleep_before_retry(attempt)
        raise AssertionError("unreachable")

    async def events(self, last_id: str = "0", *, transport: str = "sse") -> AsyncGenerator[dict, None]:
        if transport == "sse":
            async for event in self._sse_events(last_id=last_id):
                yield event
            return
        if transport == "long-poll":
            async for event in self._long_poll_events(last_id=last_id):
                yield event
            return
        raise ValueError("transport must be 'sse' or 'long-poll'")

    async def _long_poll_events(self, last_id: str = "0") -> AsyncGenerator[dict, None]:
        while True:
            r = await self._get_with_read_retries(
                f"/streams/{self._id}/events",
                params={"last_id": last_id, "timeout": 30},
                headers=self._request_headers(),
            )
            if r.status_code == 204:
                continue
            body = orjson.loads(r.content)
            for event in body["events"]:
                yield event
            last_id = body["last_id"]

    async def _sse_events(self, last_id: str = "0") -> AsyncGenerator[dict, None]:
        current_last_id = last_id
        while True:
            for attempt in range(self._read_retries + 1):
                headers = {}
                if current_last_id != "0":
                    headers["Last-Event-ID"] = current_last_id
                try:
                    async with self._client.stream(
                        "GET",
                        f"/streams/{self._id}/events/sse",
                        params={"timeout": 30},
                        headers=self._request_headers(headers),
                    ) as r:
                        if r.status_code >= 400:
                            await r.aread()
                        _raise_for_trim_gap(r)
                        _raise_for_status(r)
                        parser = _SSEParser()
                        async for line in r.aiter_lines():
                            event = parser.feed_line(line)
                            if event is None:
                                continue
                            if event["event"] == "gap":
                                raise StreamTrimmedError(
                                    last_id=event["data"]["last_id"],
                                    first_available_id=event["data"]["first_available_id"],
                                    detail=event["data"]["detail"],
                                )
                            current_last_id = event["id"]
                            yield {"id": event["id"], "data": event["data"]}
                    break
                except (httpx.RequestError, StreamServerError) as exc:
                    if attempt >= self._read_retries:
                        if isinstance(exc, httpx.RequestError):
                            raise _transport_error(exc) from exc
                        raise
                    await self._sleep_before_retry(attempt)

    async def put_many(self, events: list[dict]) -> list[str]:
        try:
            r = await self._client.post(
                f"/streams/{self._id}/events/batch",
                content=orjson.dumps({"events": events}),
                headers=self._request_headers({"Content-Type": "application/json"}),
            )
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)
        return orjson.loads(r.content)["ids"]

    def buffered(self, max_batch_size: int = 100) -> AsyncBufferedStreamWriter:
        return AsyncBufferedStreamWriter(self, max_batch_size=max_batch_size)

    async def delete(self) -> None:
        try:
            r = await self._client.delete(f"/streams/{self._id}", headers=self._request_headers())
        except httpx.RequestError as exc:
            raise _transport_error(exc) from exc
        _raise_for_status(r)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()
