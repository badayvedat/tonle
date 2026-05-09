import pytest

from tonle.store import InvalidStreamCursor, Store


class FakePipeline:
    def __init__(self, result):
        self.calls = []
        self.result = result

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        return None

    def xadd(self, *args, **kwargs):
        self.calls.append(("xadd", args, kwargs))

    def expire(self, *args, **kwargs):
        self.calls.append(("expire", args, kwargs))

    async def execute(self):
        return self.result


class FakeRedis:
    def __init__(self):
        self.pipeline_transaction = None
        self.pipeline_instance = FakePipeline([b"1-0", True])
        self.xread_kwargs = None
        self.xread_streams = None
        self.xread_calls = 0
        self.xread_result = []
        self.pipeline_calls = 0

    def pipeline(self, transaction=True):
        self.pipeline_calls += 1
        self.pipeline_transaction = transaction
        return self.pipeline_instance

    async def xread(self, streams, **kwargs):
        self.xread_calls += 1
        self.xread_streams = streams
        self.xread_kwargs = kwargs
        return self.xread_result


async def test_add_uses_transaction_for_write_and_ttl():
    redis = FakeRedis()
    store = Store(redis)

    entry_id = await store.add("demo", {"n": 1})

    assert entry_id == "1-0"
    assert redis.pipeline_transaction
    assert redis.pipeline_instance.calls == [
        ("xadd", ("stream:demo", {"data": b'{"n":1}'}), {"maxlen": 10000, "approximate": True}),
        ("expire", ("stream:demo", 600), {}),
    ]


async def test_add_many_uses_transaction_for_all_writes_and_ttl():
    redis = FakeRedis()
    redis.pipeline_instance = FakePipeline([b"1-0", b"2-0", True])
    store = Store(redis)

    entry_ids = await store.add_many("demo", [{"n": 1}, {"n": 2}])

    assert entry_ids == ["1-0", "2-0"]
    assert redis.pipeline_transaction
    assert redis.pipeline_instance.calls == [
        ("xadd", ("stream:demo", {"data": b'{"n":1}'}), {"maxlen": 10000, "approximate": True}),
        ("xadd", ("stream:demo", {"data": b'{"n":2}'}), {"maxlen": 10000, "approximate": True}),
        ("expire", ("stream:demo", 600), {}),
    ]


async def test_add_many_noops_for_empty_input():
    redis = FakeRedis()
    store = Store(redis)

    entry_ids = await store.add_many("demo", [])

    assert entry_ids == []
    assert redis.pipeline_calls == 0


async def test_read_omits_block_for_zero_timeout():
    redis = FakeRedis()
    store = Store(redis)

    await store.read("demo", "0", 5, 0)

    assert redis.xread_streams == {"stream:demo": "0"}
    assert redis.xread_kwargs == {"count": 5}


async def test_read_rejects_invalid_cursor_before_redis_call():
    redis = FakeRedis()
    store = Store(redis)

    with pytest.raises(InvalidStreamCursor):
        await store.read("demo", "abc", 5, 1000)
    assert redis.xread_calls == 0


async def test_read_allows_special_cursor_forms():
    redis = FakeRedis()
    store = Store(redis)

    await store.read("demo", "$", 5, 1000)

    assert redis.xread_streams == {"stream:demo": "$"}


async def test_read_decodes_binary_ids_and_payloads():
    redis = FakeRedis()
    redis.xread_result = [
        (
            b"stream:demo",
            [
                (b"1-0", {b"data": b'{"n":1}'}),
                (b"2-0", {b"data": b'{"n":2}'}),
            ],
        )
    ]
    store = Store(redis)

    events = await store.read("demo", "0", 5, 1000)

    assert events == [("1-0", {"n": 1}), ("2-0", {"n": 2})]
