"""Prometheus metrics shared by the HTTP services (gateway, metadata, storage nodes)."""

import time

from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, CollectorRegistry, Counter, Histogram, generate_latest

REQUESTS = Counter("dos_http_requests_total", "HTTP requests handled",
                   ["service", "method", "route", "status"])
DURATION = Histogram("dos_http_request_duration_seconds", "Time to handle an HTTP request, body included",
                     ["service", "method", "route"],
                     buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120))
BYTES = Counter("dos_http_body_bytes_total", "Request and response body bytes",
                ["service", "direction"])


class MetricsMiddleware:
    """Counts requests and their duration per route template (e.g. /buckets/{bucket}/objects/{key}),
    never per raw path, so the number of time series stays small."""

    def __init__(self, app, service: str):
        self.app = app
        self.service = service

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/metrics":
            return await self.app(scope, receive, send)
        start, status = time.perf_counter(), 500

        async def counting_receive():
            message = await receive()
            if message["type"] == "http.request" and message.get("body"):
                BYTES.labels(self.service, "in").inc(len(message["body"]))
            return message

        async def counting_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            elif message["type"] == "http.response.body" and message.get("body"):
                BYTES.labels(self.service, "out").inc(len(message["body"]))
            await send(message)

        try:
            await self.app(scope, counting_receive, counting_send)
        finally:
            route = getattr(scope.get("route"), "path", "unmatched")
            REQUESTS.labels(self.service, scope["method"], route, str(status)).inc()
            DURATION.labels(self.service, scope["method"], route).observe(time.perf_counter() - start)


def metrics_response(*extra: CollectorRegistry):
    """The /metrics body: the shared registry plus any per-app registries."""
    from fastapi import Response

    body = generate_latest(REGISTRY) + b"".join(generate_latest(r) for r in extra)
    return Response(body, media_type=CONTENT_TYPE_LATEST)
