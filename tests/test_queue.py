import asyncio
import json
from datetime import UTC, datetime

import httpx
import pytest

from apiqueue.cli import read_jobs
from apiqueue.core import APIQueue, Job, QueueConfig, RateLimiter, retry_after


class Clock:
    def __init__(self):
        self.now = 100.0
        self.sleeps = []

    def __call__(self):
        return self.now

    async def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


async def execute(handler, jobs, **config):
    clock = Clock()
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        queue = APIQueue(client, QueueConfig(**config), clock=clock, sleep=clock.sleep)
        return await queue.run(jobs), clock


async def test_429_honors_retry_after_then_success():
    calls = []

    def handler(request):
        calls.append(request)
        return (
            httpx.Response(429, headers={"Retry-After": "2"})
            if len(calls) == 1
            else httpx.Response(200, json={"ok": True})
        )

    results, clock = await execute(handler, [Job("a", "https://example.test/a")])
    assert results[0].attempts == 2 and results[0].error is None
    assert json.loads(results[0].body) == {"ok": True}
    assert 2.0 in clock.sleeps


async def test_post_does_not_retry_without_explicit_safe_flag():
    results, _ = await execute(
        lambda request: httpx.Response(503), [Job("a", "https://example.test", method="POST")]
    )
    assert results[0].attempts == 1 and results[0].error == "http_503"
    results, _ = await execute(
        lambda request: httpx.Response(503),
        [Job("a", "https://example.test", method="POST", retry_safe=True)],
    )
    assert results[0].attempts == 3


async def test_transport_error_retry_and_no_secret_logging(caplog):
    def handler(request):
        raise httpx.ReadTimeout("https://example.test?secret=private", request=request)

    with caplog.at_level("INFO"):
        results, _ = await execute(handler, [Job("a", "https://example.test?secret=private")])
    assert results[0].error == "ReadTimeout" and results[0].attempts == 3
    assert "private" not in caplog.text
    for record in caplog.records:
        assert json.loads(record.message)["event"] == "attempt"


async def test_long_retry_after_stops_without_retrying_early():
    results, clock = await execute(
        lambda request: httpx.Response(429, headers={"Retry-After": "120"}),
        [Job("a", "https://example.test")],
        max_retry_delay=5,
    )
    assert results[0].error == "retry_delay_limit" and results[0].attempts == 1
    assert clock.sleeps == []


async def test_response_size_limit_and_redirect_not_followed():
    results, _ = await execute(
        lambda request: httpx.Response(200, content=b"oversized"),
        [Job("a", "https://example.test")],
        max_response_bytes=4,
    )
    assert results[0].error == "response_size_limit" and results[0].body == ""
    results, _ = await execute(
        lambda request: httpx.Response(302, headers={"Location": "/next"}),
        [Job("a", "https://example.test")],
    )
    assert results[0].attempts == 1 and results[0].status == 302


async def test_concurrency_limit_and_order():
    active = peak = 0

    async def handler(request):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.01 if request.url.path == "/0" else 0)
        active -= 1
        return httpx.Response(200, text=request.url.path)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        queue = APIQueue(
            client, QueueConfig(concurrency=2, queue_size=1, requests_per_second=10000)
        )
        results = await queue.run(Job(str(i), f"https://example.test/{i}") for i in range(8))
    assert peak == 2
    assert [result.id for result in results] == [str(i) for i in range(8)]


async def test_duplicate_ids_and_job_budget_cancel_workers():
    with pytest.raises(ExceptionGroup) as error:
        await execute(
            lambda request: httpx.Response(200),
            [Job("a", "https://example.test"), Job("a", "https://example.test")],
        )
    assert "Duplicate job id" in str(error.value.exceptions[0])
    with pytest.raises(ExceptionGroup) as error:
        await execute(
            lambda request: httpx.Response(200),
            [Job(str(i), "https://example.test") for i in range(3)],
            max_jobs=2,
        )
    assert "max_jobs" in str(error.value.exceptions[0])


async def test_cancellation_closes_tasks():
    entered = asyncio.Event()
    exited = asyncio.Event()

    async def handler(request):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            exited.set()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        queue = APIQueue(client, QueueConfig(concurrency=1))
        task = asyncio.create_task(queue.run([Job("a", "https://example.test")]))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert exited.is_set()


async def test_global_rate_reservations():
    clock = Clock()
    limiter = RateLimiter(2, clock=clock, sleep=clock.sleep)
    await limiter.wait()
    await limiter.wait()
    await limiter.wait()
    assert clock.sleeps == [0.5, 0.5]


@pytest.mark.parametrize(
    "value, expected", [("3", 3), ("-1", 0), ("invalid", None), ("NaN", None), (None, None)]
)
def test_retry_after_seconds(value, expected):
    assert retry_after(value) == expected


def test_retry_after_http_date():
    now = datetime(2026, 10, 9, 10, 0, 0, tzinfo=UTC)
    assert retry_after("Fri, 09 Oct 2026 10:00:05 GMT", now=now) == 5


@pytest.mark.parametrize(
    "config",
    [{"concurrency": 0}, {"timeout": float("nan")}, {"attempts": 1.2}, {"queue_size": True}],
)
def test_invalid_config(config):
    with pytest.raises(ValueError):
        QueueConfig(**config)


@pytest.mark.parametrize("url", ["file:///etc/passwd", "https://user:pass@example.test", "no"])
def test_invalid_urls(url):
    with pytest.raises(ValueError):
        Job("a", url)


def test_jsonl_reader_rejects_budget_and_invalid_method(tmp_path):
    path = tmp_path / "jobs.jsonl"
    path.write_text(
        "\n".join(json.dumps({"id": str(i), "url": "https://example.test"}) for i in range(2))
    )
    with pytest.raises(ValueError, match="max_jobs"):
        list(read_jobs(path, 1))
    with pytest.raises(ValueError, match="uppercase"):
        Job("a", "https://example.test", method="get")
