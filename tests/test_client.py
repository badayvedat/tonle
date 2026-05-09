import httpx
import pytest

from tonle.client import (
    AsyncStream,
    Stream,
    StreamForbiddenError,
    StreamPayloadTooLargeError,
    StreamRateLimitedError,
    StreamServerError,
    StreamTransportError,
    StreamTrimmedError,
    StreamUnauthorizedError,
)


class BytesStream(httpx.SyncByteStream):
    def __init__(self, *chunks: bytes, exc: Exception | None = None):
        self._chunks = chunks
        self._exc = exc

    def __iter__(self):
        yield from self._chunks
        if self._exc is not None:
            raise self._exc


class AsyncBytesStream(httpx.AsyncByteStream):
    def __init__(self, *chunks: bytes, exc: Exception | None = None):
        self._chunks = chunks
        self._exc = exc

    async def __aiter__(self):
        for chunk in self._chunks:
            yield chunk
        if self._exc is not None:
            raise self._exc


def test_events_long_poll_yields_event_ids():
    def handler(request):
        assert request.url.path == "/streams/demo/events"
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = Stream("demo")
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        event = next(stream.events(transport="long-poll"))
        assert event == {"id": "1-0", "data": {"n": 1}}
    finally:
        stream.close()


def test_events_sse_yields_event_ids():
    def handler(request):
        assert request.url.path == "/streams/demo/events/sse"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 1-0\ndata: {"n":1}\n\n',
        )

    stream = Stream("demo")
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        event = next(generator)
        generator.close()
        assert event == {"id": "1-0", "data": {"n": 1}}
    finally:
        stream.close()


def test_events_sse_retries_with_updated_last_event_id():
    seen_last_event_ids = []
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        seen_last_event_ids.append(request.headers.get("last-event-id"))
        if calls == 1:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=BytesStream(
                    b'id: 1-0\ndata: {"n":1}\n\n',
                    exc=httpx.ReadError("stream dropped", request=request),
                ),
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 2-0\ndata: {"n":2}\n\n',
        )

    stream = Stream("demo", read_retries=1, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        assert next(generator) == {"id": "1-0", "data": {"n": 1}}
        assert next(generator) == {"id": "2-0", "data": {"n": 2}}
        generator.close()
        assert seen_last_event_ids == [None, "1-0"]
    finally:
        stream.close()


def test_events_sse_maps_streamed_http_error_detail():
    def handler(request):
        return httpx.Response(
            429,
            headers={"Content-Type": "application/json", "Retry-After": "3"},
            stream=BytesStream(b'{"detail":"too many listeners"}'),
        )

    stream = Stream("demo", read_retries=0, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamRateLimitedError, match="too many listeners") as exc:
            next(stream.events())
        assert exc.value.retry_after == "3"
    finally:
        stream.close()


def test_events_sse_exhausts_transient_retries():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("network unavailable", request=request)

    stream = Stream("demo", read_retries=1, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTransportError, match="network unavailable"):
            next(stream.events())
        assert calls == 2
    finally:
        stream.close()


def test_events_sse_retries_server_error():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"detail": "temporarily unavailable"})
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 1-0\ndata: {"n":1}\n\n',
        )

    stream = Stream("demo", read_retries=1, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        assert next(generator) == {"id": "1-0", "data": {"n": 1}}
        generator.close()
        assert calls == 2
    finally:
        stream.close()


def test_stream_sends_bearer_token_and_custom_headers():
    seen_requests = []

    def handler(request):
        seen_requests.append(request)
        if request.url.path == "/streams/demo/events":
            return httpx.Response(
                200,
                json={
                    "events": [{"id": "1-0", "data": {"n": 1}}],
                    "last_id": "1-0",
                },
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 2-0\ndata: {"n":2}\n\n',
        )

    stream = Stream("demo", token="ticket-1", headers={"X-Trace-Id": "abc"})
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        assert next(stream.events(transport="long-poll")) == {"id": "1-0", "data": {"n": 1}}
        generator = stream.events(last_id="1-0")
        assert next(generator) == {"id": "2-0", "data": {"n": 2}}
        generator.close()
    finally:
        stream.close()

    assert len(seen_requests) == 2
    assert seen_requests[0].headers["authorization"] == "Bearer ticket-1"
    assert seen_requests[0].headers["x-trace-id"] == "abc"
    assert "last-event-id" not in seen_requests[0].headers
    assert seen_requests[1].headers["authorization"] == "Bearer ticket-1"
    assert seen_requests[1].headers["x-trace-id"] == "abc"
    assert seen_requests[1].headers["last-event-id"] == "1-0"


def test_stream_accepts_custom_timeout_values():
    stream = Stream(
        "demo",
        connect_timeout=1.0,
        read_timeout=2.0,
        write_timeout=3.0,
        pool_timeout=4.0,
    )

    try:
        assert stream._client.timeout.connect == 1.0
        assert stream._client.timeout.read == 2.0
        assert stream._client.timeout.write == 3.0
        assert stream._client.timeout.pool == 4.0
    finally:
        stream.close()


def test_stream_rejects_combined_shared_and_stage_timeouts():
    with pytest.raises(ValueError, match="timeout cannot be combined"):
        Stream("demo", timeout=10.0, read_timeout=65.0)


def test_events_rejects_unknown_transport():
    stream = Stream("demo")
    stream.close()
    try:
        with pytest.raises(ValueError, match="transport must be 'sse' or 'long-poll'"):
            next(stream.events(transport="websocket"))
    finally:
        stream.close()


@pytest.mark.parametrize(
    ("status_code", "exception_type"),
    [
        (401, StreamUnauthorizedError),
        (403, StreamForbiddenError),
        (413, StreamPayloadTooLargeError),
        (429, StreamRateLimitedError),
        (500, StreamServerError),
    ],
)
def test_put_maps_common_http_errors(status_code, exception_type):
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(
            status_code,
            json={"detail": "mapped failure"},
            headers={"Retry-After": "7"} if status_code == 429 else {},
        )

    stream = Stream("demo")
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(exception_type, match="mapped failure") as exc:
            stream.put({"n": 1})
        assert exc.value.status_code == status_code
        if status_code == 429:
            assert exc.value.retry_after == "7"
        assert calls == 1
    finally:
        stream.close()


def test_events_long_poll_retries_transient_read_failure():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("network unavailable", request=request)
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = Stream("demo", read_retries=1, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        assert next(stream.events(transport="long-poll")) == {"id": "1-0", "data": {"n": 1}}
        assert calls == 2
    finally:
        stream.close()


def test_events_long_poll_retries_server_error():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"detail": "temporarily unavailable"})
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = Stream("demo", read_retries=1, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        assert next(stream.events(transport="long-poll")) == {"id": "1-0", "data": {"n": 1}}
        assert calls == 2
    finally:
        stream.close()


def test_events_long_poll_wraps_transient_read_failure_after_retries():
    def handler(request):
        raise httpx.ConnectError("network unavailable", request=request)

    stream = Stream("demo", read_retries=0, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTransportError, match="network unavailable"):
            next(stream.events(transport="long-poll"))
    finally:
        stream.close()


def test_put_does_not_retry_transient_write_failure():
    calls = 0

    def handler(request):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("network unavailable", request=request)

    stream = Stream("demo", read_retries=3, retry_backoff=0)
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTransportError, match="network unavailable"):
            stream.put({"n": 1})
        assert calls == 1
    finally:
        stream.close()


def test_events_long_poll_raises_trim_gap():
    def handler(request):
        assert request.url.path == "/streams/demo/events"
        return httpx.Response(
            409,
            json={
                "detail": "requested last_id has been trimmed from the stream",
                "last_id": "1-0",
                "first_available_id": "5-0",
            },
        )

    stream = Stream("demo")
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTrimmedError, match="trimmed from the stream") as exc:
            next(stream.events(transport="long-poll", last_id="1-0"))
        assert exc.value.last_id == "1-0"
        assert exc.value.first_available_id == "5-0"
    finally:
        stream.close()


def test_events_sse_raises_trim_gap():
    def handler(request):
        assert request.url.path == "/streams/demo/events/sse"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=(
                b"event: gap\n"
                b"id: 5-0\n"
                b'data: {"detail":"requested last_id has been trimmed from the stream","last_id":"1-0","first_available_id":"5-0"}\n\n'
            ),
        )

    stream = Stream("demo")
    stream.close()
    stream._client = httpx.Client(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTrimmedError, match="trimmed from the stream") as exc:
            next(stream.events(last_id="1-0"))
        assert exc.value.last_id == "1-0"
        assert exc.value.first_available_id == "5-0"
    finally:
        stream.close()


def test_buffered_writer_flushes_by_batch_size_and_on_close():
    stream = Stream("demo")
    stream.close()
    calls = []

    def fake_put_many(events):
        calls.append(events)
        return [f"{len(calls)}-{index}" for index, _ in enumerate(events)]

    stream.put_many = fake_put_many

    writer = stream.buffered(max_batch_size=2)

    assert writer.put({"n": 1}) == []
    assert writer.put({"n": 2}) == ["1-0", "1-1"]
    assert writer.put({"n": 3}) == []
    assert writer.close() == ["2-0"]
    assert calls == [[{"n": 1}, {"n": 2}], [{"n": 3}]]


def test_buffered_writer_preserves_buffer_on_failed_flush():
    stream = Stream("demo")
    stream.close()
    calls = []
    should_fail = True

    def fake_put_many(events):
        nonlocal should_fail
        calls.append(list(events))
        if should_fail:
            should_fail = False
            raise RuntimeError("boom")
        return [f"ok-{index}" for index, _ in enumerate(events)]

    stream.put_many = fake_put_many

    writer = stream.buffered(max_batch_size=2)

    assert writer.put({"n": 1}) == []
    with pytest.raises(RuntimeError):
        writer.put({"n": 2})
    assert writer._buffer == [{"n": 1}, {"n": 2}]
    assert writer.flush() == ["ok-0", "ok-1"]
    assert writer._buffer == []
    assert calls == [[{"n": 1}, {"n": 2}], [{"n": 1}, {"n": 2}]]


def test_buffered_writer_context_manager_flushes_on_clean_exit():
    stream = Stream("demo")
    stream.close()
    calls = []

    def fake_put_many(events):
        calls.append(list(events))
        return ["1-0"]

    stream.put_many = fake_put_many

    with stream.buffered(max_batch_size=10) as writer:
        writer.put({"n": 1})

    assert calls == [[{"n": 1}]]


def test_buffered_writer_context_manager_preserves_original_exception():
    stream = Stream("demo")
    stream.close()
    calls = []

    def fake_put_many(events):
        calls.append(list(events))
        raise RuntimeError("flush failed")

    stream.put_many = fake_put_many

    with pytest.raises(ValueError, match="body failed"):
        with stream.buffered(max_batch_size=10) as writer:
            writer.put({"n": 1})
            raise ValueError("body failed")

    assert calls == []


async def test_async_events_long_poll_yields_event_ids():
    async def handler(request):
        assert request.url.path == "/streams/demo/events"
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = AsyncStream("demo")
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        events = stream.events(transport="long-poll")
        event = await anext(events)
        await events.aclose()
        assert event == {"id": "1-0", "data": {"n": 1}}
    finally:
        await stream.aclose()


async def test_async_events_sse_yields_event_ids():
    async def handler(request):
        assert request.url.path == "/streams/demo/events/sse"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 1-0\ndata: {"n":1}\n\n',
        )

    stream = AsyncStream("demo")
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        event = await anext(generator)
        await generator.aclose()
        assert event == {"id": "1-0", "data": {"n": 1}}
    finally:
        await stream.aclose()


async def test_async_events_rejects_unknown_transport():
    stream = AsyncStream("demo")
    await stream.aclose()
    try:
        with pytest.raises(ValueError, match="transport must be 'sse' or 'long-poll'"):
            await anext(stream.events(transport="websocket"))
    finally:
        await stream.aclose()


async def test_async_events_sse_retries_with_updated_last_event_id():
    seen_last_event_ids = []
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        seen_last_event_ids.append(request.headers.get("last-event-id"))
        if calls == 1:
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                stream=AsyncBytesStream(
                    b'id: 1-0\ndata: {"n":1}\n\n',
                    exc=httpx.ReadError("stream dropped", request=request),
                ),
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 2-0\ndata: {"n":2}\n\n',
        )

    stream = AsyncStream("demo", read_retries=1, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        assert await anext(generator) == {"id": "1-0", "data": {"n": 1}}
        assert await anext(generator) == {"id": "2-0", "data": {"n": 2}}
        await generator.aclose()
        assert seen_last_event_ids == [None, "1-0"]
    finally:
        await stream.aclose()


async def test_async_events_sse_maps_streamed_http_error_detail():
    async def handler(request):
        return httpx.Response(
            429,
            headers={"Content-Type": "application/json", "Retry-After": "3"},
            stream=AsyncBytesStream(b'{"detail":"too many listeners"}'),
        )

    stream = AsyncStream("demo", read_retries=0, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamRateLimitedError, match="too many listeners") as exc:
            await anext(stream.events())
        assert exc.value.retry_after == "3"
    finally:
        await stream.aclose()


async def test_async_events_sse_exhausts_transient_retries():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        raise httpx.ConnectError("network unavailable", request=request)

    stream = AsyncStream("demo", read_retries=1, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTransportError, match="network unavailable"):
            await anext(stream.events())
        assert calls == 2
    finally:
        await stream.aclose()


async def test_async_events_sse_retries_server_error():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"detail": "temporarily unavailable"})
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 1-0\ndata: {"n":1}\n\n',
        )

    stream = AsyncStream("demo", read_retries=1, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events()
        assert await anext(generator) == {"id": "1-0", "data": {"n": 1}}
        await generator.aclose()
        assert calls == 2
    finally:
        await stream.aclose()


async def test_async_stream_sends_bearer_token_and_custom_headers():
    seen_requests = []

    async def handler(request):
        seen_requests.append(request)
        if request.url.path == "/streams/demo/events":
            return httpx.Response(
                200,
                json={
                    "events": [{"id": "1-0", "data": {"n": 1}}],
                    "last_id": "1-0",
                },
            )
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=b'id: 2-0\ndata: {"n":2}\n\n',
        )

    stream = AsyncStream("demo", token="ticket-1", headers={"X-Trace-Id": "abc"})
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events(transport="long-poll")
        assert await anext(generator) == {"id": "1-0", "data": {"n": 1}}
        await generator.aclose()

        sse_generator = stream.events(last_id="1-0")
        assert await anext(sse_generator) == {"id": "2-0", "data": {"n": 2}}
        await sse_generator.aclose()
    finally:
        await stream.aclose()

    assert len(seen_requests) == 2
    assert seen_requests[0].headers["authorization"] == "Bearer ticket-1"
    assert seen_requests[0].headers["x-trace-id"] == "abc"
    assert "last-event-id" not in seen_requests[0].headers
    assert seen_requests[1].headers["authorization"] == "Bearer ticket-1"
    assert seen_requests[1].headers["x-trace-id"] == "abc"
    assert seen_requests[1].headers["last-event-id"] == "1-0"


async def test_async_events_long_poll_retries_transient_read_failure():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("network unavailable", request=request)
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = AsyncStream("demo", read_retries=1, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events(transport="long-poll")
        assert await anext(generator) == {"id": "1-0", "data": {"n": 1}}
        await generator.aclose()
        assert calls == 2
    finally:
        await stream.aclose()


async def test_async_stream_rejects_combined_shared_and_stage_timeouts():
    with pytest.raises(ValueError, match="timeout cannot be combined"):
        AsyncStream("demo", timeout=10.0, read_timeout=65.0)


async def test_async_events_long_poll_retries_server_error():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        if calls == 1:
            return httpx.Response(503, json={"detail": "temporarily unavailable"})
        return httpx.Response(
            200,
            json={
                "events": [{"id": "1-0", "data": {"n": 1}}],
                "last_id": "1-0",
            },
        )

    stream = AsyncStream("demo", read_retries=1, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        generator = stream.events(transport="long-poll")
        assert await anext(generator) == {"id": "1-0", "data": {"n": 1}}
        await generator.aclose()
        assert calls == 2
    finally:
        await stream.aclose()


async def test_async_put_maps_common_http_error_without_retry():
    calls = 0

    async def handler(request):
        nonlocal calls
        calls += 1
        return httpx.Response(503, json={"detail": "service unavailable"})

    stream = AsyncStream("demo", read_retries=3, retry_backoff=0)
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamServerError, match="service unavailable"):
            await stream.put({"n": 1})
        assert calls == 1
    finally:
        await stream.aclose()


async def test_async_events_long_poll_raises_trim_gap():
    async def handler(request):
        assert request.url.path == "/streams/demo/events"
        return httpx.Response(
            409,
            json={
                "detail": "requested last_id has been trimmed from the stream",
                "last_id": "1-0",
                "first_available_id": "5-0",
            },
        )

    stream = AsyncStream("demo")
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTrimmedError, match="trimmed from the stream") as exc:
            await anext(stream.events(transport="long-poll", last_id="1-0"))
        assert exc.value.last_id == "1-0"
        assert exc.value.first_available_id == "5-0"
    finally:
        await stream.aclose()


async def test_async_events_sse_raises_trim_gap():
    async def handler(request):
        assert request.url.path == "/streams/demo/events/sse"
        return httpx.Response(
            200,
            headers={"Content-Type": "text/event-stream"},
            content=(
                b"event: gap\n"
                b"id: 5-0\n"
                b'data: {"detail":"requested last_id has been trimmed from the stream","last_id":"1-0","first_available_id":"5-0"}\n\n'
            ),
        )

    stream = AsyncStream("demo")
    await stream.aclose()
    stream._client = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
        timeout=httpx.Timeout(65.0),
    )

    try:
        with pytest.raises(StreamTrimmedError, match="trimmed from the stream") as exc:
            await anext(stream.events(last_id="1-0"))
        assert exc.value.last_id == "1-0"
        assert exc.value.first_available_id == "5-0"
    finally:
        await stream.aclose()


async def test_async_buffered_writer_flushes_by_batch_size_and_on_close():
    stream = AsyncStream("demo")
    await stream.aclose()
    calls = []

    async def fake_put_many(events):
        calls.append(events)
        return [f"{len(calls)}-{index}" for index, _ in enumerate(events)]

    stream.put_many = fake_put_many

    writer = stream.buffered(max_batch_size=2)

    assert await writer.put({"n": 1}) == []
    assert await writer.put({"n": 2}) == ["1-0", "1-1"]
    assert await writer.put({"n": 3}) == []
    assert await writer.aclose() == ["2-0"]
    assert calls == [[{"n": 1}, {"n": 2}], [{"n": 3}]]


async def test_async_buffered_writer_preserves_buffer_on_failed_flush():
    stream = AsyncStream("demo")
    await stream.aclose()
    calls = []
    should_fail = True

    async def fake_put_many(events):
        nonlocal should_fail
        calls.append(list(events))
        if should_fail:
            should_fail = False
            raise RuntimeError("boom")
        return [f"ok-{index}" for index, _ in enumerate(events)]

    stream.put_many = fake_put_many

    writer = stream.buffered(max_batch_size=2)

    assert await writer.put({"n": 1}) == []
    with pytest.raises(RuntimeError):
        await writer.put({"n": 2})
    assert writer._buffer == [{"n": 1}, {"n": 2}]
    assert await writer.flush() == ["ok-0", "ok-1"]
    assert writer._buffer == []
    assert calls == [[{"n": 1}, {"n": 2}], [{"n": 1}, {"n": 2}]]


async def test_async_buffered_writer_context_manager_flushes_on_clean_exit():
    stream = AsyncStream("demo")
    await stream.aclose()
    calls = []

    async def fake_put_many(events):
        calls.append(list(events))
        return ["1-0"]

    stream.put_many = fake_put_many

    async with stream.buffered(max_batch_size=10) as writer:
        await writer.put({"n": 1})

    assert calls == [[{"n": 1}]]


async def test_async_buffered_writer_context_manager_preserves_original_exception():
    stream = AsyncStream("demo")
    await stream.aclose()
    calls = []

    async def fake_put_many(events):
        calls.append(list(events))
        raise RuntimeError("flush failed")

    stream.put_many = fake_put_many

    with pytest.raises(ValueError, match="body failed"):
        async with stream.buffered(max_batch_size=10) as writer:
            await writer.put({"n": 1})
            raise ValueError("body failed")

    assert calls == []
