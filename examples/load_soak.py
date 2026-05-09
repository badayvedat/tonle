"""Small load/soak harness for tonle fan-out testing."""

from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from dataclasses import dataclass, field

from tonle import AsyncStream, StreamTrimmedError


@dataclass
class LatencySample:
    max_samples: int
    count: int = 0
    min_ms: float | None = None
    max_ms: float | None = None
    sum_ms: float = 0.0
    samples: list[float] = field(default_factory=list)
    _rng: random.Random = field(default_factory=lambda: random.Random(0))

    def add(self, value_ms: float) -> None:
        self.count += 1
        self.sum_ms += value_ms
        self.min_ms = value_ms if self.min_ms is None else min(self.min_ms, value_ms)
        self.max_ms = value_ms if self.max_ms is None else max(self.max_ms, value_ms)
        if len(self.samples) < self.max_samples:
            self.samples.append(value_ms)
            return
        index = self._rng.randrange(self.count)
        if index < self.max_samples:
            self.samples[index] = value_ms

    def average_ms(self) -> float | None:
        if self.count == 0:
            return None
        return self.sum_ms / self.count

    def percentile_ms(self, percentile: float) -> float | None:
        if not self.samples:
            return None
        ordered = sorted(self.samples)
        index = int((len(ordered) - 1) * percentile)
        return ordered[index]


@dataclass
class RunStats:
    latency: LatencySample
    published: int = 0
    received: int = 0
    receive_without_timestamp: int = 0
    trimmed_errors: int = 0
    producer_errors: int = 0
    listener_errors: int = 0

    def record_published(self, count: int) -> None:
        self.published += count

    def record_received(self, sent_at_ns: int | None) -> None:
        self.received += 1
        if sent_at_ns is None:
            self.receive_without_timestamp += 1
            return
        latency_ms = (time.perf_counter_ns() - sent_at_ns) / 1_000_000
        self.latency.add(latency_ms)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--mode", choices=("sse", "poll"), default="sse")
    parser.add_argument("--streams", type=int, default=10)
    parser.add_argument("--listeners-per-stream", type=int, default=5)
    parser.add_argument("--events-per-stream", type=int, default=100)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--payload-bytes", type=int, default=128)
    parser.add_argument("--producer-delay-ms", type=float, default=0.0)
    parser.add_argument("--startup-delay-seconds", type=float, default=0.5)
    parser.add_argument("--stream-prefix", default="load:")
    parser.add_argument("--writer-token")
    parser.add_argument("--reader-token")
    parser.add_argument("--latency-sample-size", type=int, default=50000)
    args = parser.parse_args()
    if args.streams < 1:
        parser.error("--streams must be at least 1")
    if args.listeners_per_stream < 1:
        parser.error("--listeners-per-stream must be at least 1")
    if args.events_per_stream < 1:
        parser.error("--events-per-stream must be at least 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1")
    if args.payload_bytes < 0:
        parser.error("--payload-bytes must be at least 0")
    if args.producer_delay_ms < 0:
        parser.error("--producer-delay-ms must be at least 0")
    if args.startup_delay_seconds < 0:
        parser.error("--startup-delay-seconds must be at least 0")
    if args.latency_sample_size < 1:
        parser.error("--latency-sample-size must be at least 1")
    return args


async def _produce_stream(stream_id: str, args: argparse.Namespace, stats: RunStats) -> None:
    payload = "x" * args.payload_bytes
    delay_seconds = args.producer_delay_ms / 1000

    async with AsyncStream(stream_id, base_url=args.base_url, token=args.writer_token) as stream:
        sent = 0
        while sent < args.events_per_stream:
            batch = []
            remaining = min(args.batch_size, args.events_per_stream - sent)
            for _ in range(remaining):
                batch.append(
                    {
                        "stream_id": stream_id,
                        "seq": sent,
                        "sent_at_ns": time.perf_counter_ns(),
                        "payload": payload,
                    }
                )
                sent += 1
            if len(batch) == 1:
                await stream.put(batch[0])
            else:
                await stream.put_many(batch)
            stats.record_published(len(batch))
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)


async def _consume_stream(stream_id: str, args: argparse.Namespace, stats: RunStats) -> None:
    async with AsyncStream(stream_id, base_url=args.base_url, token=args.reader_token) as stream:
        generator = stream.events() if args.mode == "sse" else stream.events(transport="long-poll")
        seen = 0
        try:
            async for event in generator:
                sent_at_ns = None
                data = event["data"]
                if isinstance(data, dict):
                    raw_sent_at_ns = data.get("sent_at_ns")
                    if isinstance(raw_sent_at_ns, int):
                        sent_at_ns = raw_sent_at_ns
                stats.record_received(sent_at_ns)
                seen += 1
                if seen >= args.events_per_stream:
                    break
        finally:
            await generator.aclose()


def _task_exceptions(results: list[object], expected_exc: type[BaseException] | None = None) -> list[BaseException]:
    exceptions: list[BaseException] = []
    for result in results:
        if isinstance(result, BaseException):
            if expected_exc is not None and isinstance(result, expected_exc):
                continue
            exceptions.append(result)
    return exceptions


def _rate(count: int, duration_seconds: float) -> float | None:
    if duration_seconds <= 0:
        return None
    return count / duration_seconds


def _summary_dict(args: argparse.Namespace, stats: RunStats, run_started_at: float, producer_started_at: float, producer_finished_at: float, run_finished_at: float) -> dict:
    publish_duration = producer_finished_at - producer_started_at
    total_duration = run_finished_at - run_started_at
    expected_published = args.streams * args.events_per_stream
    expected_received = args.streams * args.listeners_per_stream * args.events_per_stream
    return {
        "mode": args.mode,
        "streams": args.streams,
        "listeners_per_stream": args.listeners_per_stream,
        "events_per_stream": args.events_per_stream,
        "batch_size": args.batch_size,
        "expected": {
            "published": expected_published,
            "received": expected_received,
        },
        "actual": {
            "published": stats.published,
            "received": stats.received,
            "receive_without_timestamp": stats.receive_without_timestamp,
            "trimmed_errors": stats.trimmed_errors,
            "producer_errors": stats.producer_errors,
            "listener_errors": stats.listener_errors,
        },
        "durations": {
            "publish_seconds": round(publish_duration, 3),
            "total_seconds": round(total_duration, 3),
        },
        "throughput": {
            "publish_events_per_second": _rate(stats.published, publish_duration),
            "receive_events_per_second": _rate(stats.received, total_duration),
        },
        "latency_ms": {
            "count": stats.latency.count,
            "avg": stats.latency.average_ms(),
            "min": stats.latency.min_ms,
            "p50": stats.latency.percentile_ms(0.50),
            "p95": stats.latency.percentile_ms(0.95),
            "p99": stats.latency.percentile_ms(0.99),
            "max": stats.latency.max_ms,
        },
    }


async def _run(args: argparse.Namespace) -> int:
    stats = RunStats(latency=LatencySample(max_samples=args.latency_sample_size))
    stream_ids = [f"{args.stream_prefix}{index}" for index in range(args.streams)]
    listener_tasks = [
        asyncio.create_task(_consume_stream(stream_id, args, stats))
        for stream_id in stream_ids
        for _ in range(args.listeners_per_stream)
    ]

    run_started_at = time.perf_counter()
    await asyncio.sleep(args.startup_delay_seconds)
    producer_started_at = time.perf_counter()
    producer_results = await asyncio.gather(
        *[asyncio.create_task(_produce_stream(stream_id, args, stats)) for stream_id in stream_ids],
        return_exceptions=True,
    )
    producer_finished_at = time.perf_counter()

    producer_exceptions = _task_exceptions(list(producer_results))
    if producer_exceptions:
        stats.producer_errors = len(producer_exceptions)
        for task in listener_tasks:
            task.cancel()

    listener_results = await asyncio.gather(*listener_tasks, return_exceptions=True)
    trimmed_errors = [result for result in listener_results if isinstance(result, StreamTrimmedError)]
    listener_exceptions = _task_exceptions(list(listener_results), expected_exc=asyncio.CancelledError)
    stats.trimmed_errors = len(trimmed_errors)
    stats.listener_errors = len(
        [result for result in listener_exceptions if not isinstance(result, StreamTrimmedError)]
    )

    run_finished_at = time.perf_counter()
    summary = _summary_dict(
        args,
        stats,
        run_started_at=run_started_at,
        producer_started_at=producer_started_at,
        producer_finished_at=producer_finished_at,
        run_finished_at=run_finished_at,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))

    expected = summary["expected"]
    actual = summary["actual"]
    if producer_exceptions or stats.listener_errors > 0 or actual["published"] != expected["published"] or actual["received"] != expected["received"]:
        return 1
    if stats.trimmed_errors > 0:
        return 2
    return 0


def main() -> None:
    args = _parse_args()
    raise SystemExit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
