import shutil
import socket
import subprocess
import tempfile
import time
import uuid

import pytest
import redis
from starlette.testclient import TestClient

import tonle.api as api


def _find_free_tcp_port() -> int:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return sock.getsockname()[1]
    except PermissionError:
        pytest.skip("local TCP binding is not permitted in this environment")


class _RedisServer:
    def __init__(self):
        self._process: subprocess.Popen[str] | None = None
        self.port = _find_free_tcp_port()
        self._tmpdir = tempfile.TemporaryDirectory()
        self.url = f"redis://127.0.0.1:{self.port}/0"
        self.client = redis.Redis.from_url(self.url, decode_responses=False)

    def start(self) -> None:
        executable = shutil.which("redis-server")
        if executable is None:
            pytest.skip("redis-server is not installed")

        self._process = subprocess.Popen(
            [
                executable,
                "--save", "",
                "--appendonly", "no",
                "--port", str(self.port),
                "--dir", self._tmpdir.name,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        deadline = time.time() + 5
        last_error: Exception | None = None
        while time.time() < deadline:
            if self._process.poll() is not None:
                output = ""
                if self._process.stdout is not None:
                    output = self._process.stdout.read()
                pytest.skip(f"temporary redis-server exited early: {output.strip()}")
            try:
                self.client.ping()
                return
            except Exception as exc:
                last_error = exc
                time.sleep(0.05)

        pytest.skip(f"temporary redis-server did not become ready: {last_error!r}")

    def stop(self) -> None:
        try:
            self.client.close()
        except Exception:
            pass

        if self._process is not None and self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait(timeout=3)

        self._tmpdir.cleanup()


@pytest.fixture(scope="module")
def redis_server():
    server = _RedisServer()
    server.start()
    original_url = api.REDIS_URL
    api.REDIS_URL = server.url
    yield server
    api.REDIS_URL = original_url
    server.stop()


@pytest.fixture
def http_client(redis_server):
    redis_server.client.flushdb()
    api.app_metrics.reset()
    with TestClient(api.app) as client:
        yield client
    api.app_metrics.reset()


@pytest.fixture
def stream_id():
    return f"itest:{uuid.uuid4().hex}"


def test_push_poll_info_delete_round_trip(http_client, stream_id):
    pushed = http_client.post(
        f"/streams/{stream_id}/events",
        json={"data": {"n": 1}},
    )
    assert pushed.status_code == 201
    event_id = pushed.json()["id"]

    polled = http_client.get(
        f"/streams/{stream_id}/events",
        params={"last_id": "0", "timeout": 1},
    )
    assert polled.status_code == 200
    assert polled.json() == {
        "events": [{"id": event_id, "data": {"n": 1}}],
        "last_id": event_id,
    }

    info = http_client.get(f"/streams/{stream_id}")
    assert info.status_code == 200
    assert info.json() == {"id": stream_id, "length": 1}

    deleted = http_client.delete(f"/streams/{stream_id}")
    assert deleted.status_code == 204

    info_after_delete = http_client.get(f"/streams/{stream_id}")
    assert info_after_delete.status_code == 200
    assert info_after_delete.json() == {"id": stream_id, "length": 0}


def test_batch_push_and_poll_preserve_order(http_client, stream_id):
    pushed = http_client.post(
        f"/streams/{stream_id}/events/batch",
        json={"events": [{"n": 1}, {"n": 2}]},
    )
    assert pushed.status_code == 201
    ids = pushed.json()["ids"]
    assert len(ids) == 2

    polled = http_client.get(
        f"/streams/{stream_id}/events",
        params={"last_id": "0", "timeout": 1},
    )
    assert polled.status_code == 200
    assert polled.json() == {
        "events": [
            {"id": ids[0], "data": {"n": 1}},
            {"id": ids[1], "data": {"n": 2}},
        ],
        "last_id": ids[1],
    }


def test_poll_resumes_from_last_id(http_client, stream_id):
    first = http_client.post(
        f"/streams/{stream_id}/events",
        json={"data": {"n": 1}},
    )
    second = http_client.post(
        f"/streams/{stream_id}/events",
        json={"data": {"n": 2}},
    )
    assert first.status_code == 201
    assert second.status_code == 201
    first_id = first.json()["id"]
    second_id = second.json()["id"]

    polled = http_client.get(
        f"/streams/{stream_id}/events",
        params={"last_id": first_id, "timeout": 1},
    )
    assert polled.status_code == 200
    assert polled.json() == {
        "events": [{"id": second_id, "data": {"n": 2}}],
        "last_id": second_id,
    }


def test_health_ready_and_metrics_endpoints(http_client):
    health = http_client.get("/healthz")
    ready = http_client.get("/readyz")
    metrics = http_client.get("/metrics")

    assert health.status_code == 200
    assert health.json() == {"status": "ok"}

    assert ready.status_code == 200
    assert ready.json() == {"status": "ok"}

    assert metrics.status_code == 200
    assert metrics.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
    assert 'tonle_requests_total{method="GET",route="/healthz",status="200"} 1' in metrics.text
    assert 'tonle_requests_total{method="GET",route="/readyz",status="200"} 1' in metrics.text
