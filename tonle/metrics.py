import threading
import time
from collections import Counter


def _format_labels(labels: dict[str, str]) -> str:
    if not labels:
        return ""
    parts = []
    for key, value in sorted(labels.items()):
        escaped = value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")
        parts.append(f'{key}="{escaped}"')
    return "{" + ",".join(parts) + "}"


class AppMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self.reset()

    def reset(self) -> None:
        with self._lock:
            self._requests_total: Counter[tuple[str, str, str]] = Counter()
            self._request_duration_seconds_sum: Counter[tuple[str, str]] = Counter()
            self._request_duration_seconds_count: Counter[tuple[str, str]] = Counter()
            self._auth_denials_total = 0
            self._payload_too_large_total = 0
            self._trim_gaps_total = 0
            self._redis_errors_total = 0
            self._readiness_failures_total = 0
            self._sse_disconnects_total = 0
            self._sse_connections_active = 0
            self._polls_active = 0
            self._connection_limit_rejections_total: Counter[str] = Counter()
            self._rate_limit_rejections_total: Counter[str] = Counter()
            self._tenant_quota_rejections_total: Counter[str] = Counter()

    def record_request(self, route: str, method: str, status_code: int, duration_seconds: float) -> None:
        with self._lock:
            self._requests_total[(route, method, str(status_code))] += 1
            self._request_duration_seconds_sum[(route, method)] += duration_seconds
            self._request_duration_seconds_count[(route, method)] += 1

    def record_auth_denial(self) -> None:
        with self._lock:
            self._auth_denials_total += 1

    def record_payload_too_large(self) -> None:
        with self._lock:
            self._payload_too_large_total += 1

    def record_trim_gap(self) -> None:
        with self._lock:
            self._trim_gaps_total += 1

    def record_redis_error(self) -> None:
        with self._lock:
            self._redis_errors_total += 1

    def record_readiness_failure(self) -> None:
        with self._lock:
            self._readiness_failures_total += 1

    def record_connection_limit_rejection(self, kind: str) -> None:
        with self._lock:
            self._connection_limit_rejections_total[kind] += 1

    def record_rate_limit_rejection(self, kind: str) -> None:
        with self._lock:
            self._rate_limit_rejections_total[kind] += 1

    def record_tenant_quota_rejection(self, quota: str) -> None:
        with self._lock:
            self._tenant_quota_rejections_total[quota] += 1

    def sse_open(self) -> None:
        with self._lock:
            self._sse_connections_active += 1

    def sse_close(self, *, disconnected: bool = False) -> None:
        with self._lock:
            if self._sse_connections_active > 0:
                self._sse_connections_active -= 1
            if disconnected:
                self._sse_disconnects_total += 1

    def poll_open(self) -> None:
        with self._lock:
            self._polls_active += 1

    def poll_close(self) -> None:
        with self._lock:
            if self._polls_active > 0:
                self._polls_active -= 1

    def render_prometheus(self) -> str:
        with self._lock:
            requests_total = list(self._requests_total.items())
            duration_sum = list(self._request_duration_seconds_sum.items())
            duration_count = list(self._request_duration_seconds_count.items())
            auth_denials_total = self._auth_denials_total
            payload_too_large_total = self._payload_too_large_total
            trim_gaps_total = self._trim_gaps_total
            redis_errors_total = self._redis_errors_total
            readiness_failures_total = self._readiness_failures_total
            sse_disconnects_total = self._sse_disconnects_total
            sse_connections_active = self._sse_connections_active
            polls_active = self._polls_active
            connection_limit_rejections_total = list(self._connection_limit_rejections_total.items())
            rate_limit_rejections_total = list(self._rate_limit_rejections_total.items())
            tenant_quota_rejections_total = list(self._tenant_quota_rejections_total.items())

        lines = [
            "# HELP tonle_requests_total Total HTTP requests handled.",
            "# TYPE tonle_requests_total counter",
        ]
        for (route, method, status), value in sorted(requests_total):
            lines.append(
                f"tonle_requests_total{_format_labels({'route': route, 'method': method, 'status': status})} {value}"
            )

        lines.extend(
            [
                "# HELP tonle_request_duration_seconds_sum Sum of request durations in seconds.",
                "# TYPE tonle_request_duration_seconds_sum counter",
            ]
        )
        for (route, method), value in sorted(duration_sum):
            lines.append(
                f"tonle_request_duration_seconds_sum{_format_labels({'route': route, 'method': method})} {value}"
            )

        lines.extend(
            [
                "# HELP tonle_request_duration_seconds_count Count of timed requests.",
                "# TYPE tonle_request_duration_seconds_count counter",
            ]
        )
        for (route, method), value in sorted(duration_count):
            lines.append(
                f"tonle_request_duration_seconds_count{_format_labels({'route': route, 'method': method})} {value}"
            )

        lines.extend(
            [
                "# HELP tonle_auth_denials_total Total auth denials.",
                "# TYPE tonle_auth_denials_total counter",
                f"tonle_auth_denials_total {auth_denials_total}",
                "# HELP tonle_payload_too_large_total Total 413 payload rejections.",
                "# TYPE tonle_payload_too_large_total counter",
                f"tonle_payload_too_large_total {payload_too_large_total}",
                "# HELP tonle_trim_gaps_total Total detected trim-gap events.",
                "# TYPE tonle_trim_gaps_total counter",
                f"tonle_trim_gaps_total {trim_gaps_total}",
                "# HELP tonle_redis_errors_total Total Redis/backend errors observed by the app.",
                "# TYPE tonle_redis_errors_total counter",
                f"tonle_redis_errors_total {redis_errors_total}",
                "# HELP tonle_readiness_failures_total Total readiness failures.",
                "# TYPE tonle_readiness_failures_total counter",
                f"tonle_readiness_failures_total {readiness_failures_total}",
                "# HELP tonle_sse_disconnects_total Total SSE disconnects observed by the app.",
                "# TYPE tonle_sse_disconnects_total counter",
                f"tonle_sse_disconnects_total {sse_disconnects_total}",
                "# HELP tonle_sse_connections_active Current number of active SSE connections.",
                "# TYPE tonle_sse_connections_active gauge",
                f"tonle_sse_connections_active {sse_connections_active}",
                "# HELP tonle_polls_active Current number of active long-poll requests.",
                "# TYPE tonle_polls_active gauge",
                f"tonle_polls_active {polls_active}",
            ]
        )
        lines.extend(
            [
                "# HELP tonle_connection_limit_rejections_total Total connection limit rejections.",
                "# TYPE tonle_connection_limit_rejections_total counter",
            ]
        )
        for kind, value in sorted(connection_limit_rejections_total):
            lines.append(
                f"tonle_connection_limit_rejections_total{_format_labels({'kind': kind})} {value}"
            )
        lines.extend(
            [
                "# HELP tonle_rate_limit_rejections_total Total rate limit rejections.",
                "# TYPE tonle_rate_limit_rejections_total counter",
            ]
        )
        for kind, value in sorted(rate_limit_rejections_total):
            lines.append(
                f"tonle_rate_limit_rejections_total{_format_labels({'kind': kind})} {value}"
            )
        lines.extend(
            [
                "# HELP tonle_tenant_quota_rejections_total Total tenant quota rejections.",
                "# TYPE tonle_tenant_quota_rejections_total counter",
            ]
        )
        for quota, value in sorted(tenant_quota_rejections_total):
            lines.append(
                f"tonle_tenant_quota_rejections_total{_format_labels({'quota': quota})} {value}"
            )
        return "\n".join(lines) + "\n"


app_metrics = AppMetrics()


class MetricsMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        started_at = time.perf_counter()
        status_code = 500

        async def send_wrapper(message):
            nonlocal status_code
            if message["type"] == "http.response.start":
                status_code = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            route = getattr(scope.get("route"), "path", None) or "unmatched"
            method = scope.get("method", "UNKNOWN")
            app_metrics.record_request(
                route=route,
                method=method,
                status_code=status_code,
                duration_seconds=time.perf_counter() - started_at,
            )
