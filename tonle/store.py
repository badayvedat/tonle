import re

import orjson

from redis.asyncio import Redis

from .config import STREAM_MAXLEN, STREAM_TTL

_STREAM_CURSOR_RE = re.compile(r"^(?:[+$]|\d+(?:-\d+)?)$")


class InvalidStreamCursor(ValueError):
    pass


class TrimmedStreamCursor(ValueError):
    def __init__(self, last_id: str, first_available_id: str):
        super().__init__(last_id)
        self.last_id = last_id
        self.first_available_id = first_available_id


def validate_stream_cursor(last_id: str) -> None:
    if not _STREAM_CURSOR_RE.fullmatch(last_id):
        raise InvalidStreamCursor(last_id)


def _parse_cursor_parts(value: str) -> tuple[int, int] | None:
    if value in {"$", "+"}:
        return None
    major, sep, minor = value.partition("-")
    if not sep:
        minor = "0"
    return int(major), int(minor)


class Store:
    def __init__(self, redis: Redis):
        self._redis = redis

    @property
    def redis(self) -> Redis:
        return self._redis

    def _key(self, stream_id: str) -> str:
        return f"stream:{stream_id}"

    async def add(self, stream_id: str, data: dict) -> str:
        key = self._key(stream_id)
        payload = orjson.dumps(data)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.xadd(key, {"data": payload}, maxlen=STREAM_MAXLEN, approximate=True)
            pipe.expire(key, STREAM_TTL)
            entry_id, _ = await pipe.execute()
        return entry_id.decode()

    async def add_many(self, stream_id: str, items: list[dict]) -> list[str]:
        if not items:
            return []
        key = self._key(stream_id)
        async with self._redis.pipeline(transaction=True) as pipe:
            for item in items:
                pipe.xadd(key, {"data": orjson.dumps(item)}, maxlen=STREAM_MAXLEN, approximate=True)
            pipe.expire(key, STREAM_TTL)
            results = await pipe.execute()
        return [entry_id.decode() for entry_id in results[:-1]]

    async def read(self, stream_id: str, last_id: str, count: int, block_ms: int) -> list[tuple[str, dict]]:
        validate_stream_cursor(last_id)
        key = self._key(stream_id)
        cursor_parts = _parse_cursor_parts(last_id)
        if cursor_parts not in {None, (0, 0)}:
            first_entry = await self._redis.xrange(key, min="-", max="+", count=1)
            if first_entry:
                first_available_id = first_entry[0][0].decode()
                first_parts = _parse_cursor_parts(first_available_id)
                if first_parts is not None and first_parts > cursor_parts:
                    raise TrimmedStreamCursor(last_id, first_available_id)
        kwargs: dict = {"count": count}
        if block_ms > 0:
            kwargs["block"] = block_ms
        result = await self._redis.xread({key: last_id}, **kwargs)
        if not result:
            return []
        return [
            (entry_id.decode(), orjson.loads(fields[b"data"]))
            for entry_id, fields in result[0][1]
        ]

    async def length(self, stream_id: str) -> int:
        return await self._redis.xlen(self._key(stream_id))

    async def delete(self, stream_id: str) -> None:
        await self._redis.delete(self._key(stream_id))

    async def ping(self) -> bool:
        return bool(await self._redis.ping())
