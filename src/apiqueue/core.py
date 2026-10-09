"""Bounded async HTTP execution, with explicit retry semantics."""

from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

LOGGER = logging.getLogger(__name__)
SAFE_METHODS = {"GET", "HEAD", "OPTIONS"}
RETRY_STATUSES = {429, 500, 502, 503, 504}


@dataclass(frozen=True)
class QueueConfig:
    concurrency: int = 4
    queue_size: int = 16
    requests_per_second: float = 5.0
    timeout: float = 10.0
    attempts: int = 3
    backoff: float = 0.5
    max_retry_delay: float = 60.0
    max_response_bytes: int = 1_000_000
    max_jobs: int = 10_000

    def __post_init__(self) -> None:
        for name in ("concurrency", "queue_size", "attempts", "max_response_bytes", "max_jobs"):
            value = getattr(self, name)
            if not isinstance(value, int) or isinstance(value, bool) or value < 1:
                raise ValueError(f"{name} must be a positive integer")
        for name in ("requests_per_second", "timeout", "backoff", "max_retry_delay"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be a finite positive number")


@dataclass(frozen=True)
class Job:
    id: str
    url: str
    method: str = "GET"
    json_body: Any = None
    retry_safe: bool = False

    def __post_init__(self) -> None:
        url = httpx.URL(self.url)
        if url.scheme not in {"http", "https"} or not url.host or url.username or url.password:
            raise ValueError("Job URL must be HTTP(S), have a host, and contain no credentials")
        if not isinstance(self.id, str) or not self.id or len(self.id) > 128:
            raise ValueError("Job id must contain 1 to 128 characters")
        if self.method not in {"GET", "HEAD", "OPTIONS", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError("Use an uppercase supported HTTP method")
        if not isinstance(self.retry_safe, bool):
            raise ValueError("retry_safe must be a boolean")


@dataclass(frozen=True)
class Result:
    id: str
    status: int | None
    attempts: int
    body: str
    error: str | None
    duration_ms: float


def retry_after(value: str | None, *, now: datetime | None = None) -> float | None:
    """Parse Retry-After seconds or an HTTP date; invalid values use normal backoff."""
    if not value:
        return None
    try:
        seconds = float(value)
        return max(0.0, seconds) if math.isfinite(seconds) else None
    except ValueError:
        try:
            date = parsedate_to_datetime(value)
            if date.tzinfo is None:
                date = date.replace(tzinfo=UTC)
            return max(0.0, (date - (now or datetime.now(UTC))).total_seconds())
        except (ValueError, TypeError, OverflowError):
            return None


class RateLimiter:
    """Reserve global start times under a lock; sleeping never holds the lock."""

    def __init__(
        self,
        rate: float,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.interval = 1.0 / rate
        self.clock = clock
        self.sleep = sleep
        self.next_at = 0.0
        self.lock = asyncio.Lock()

    async def wait(self) -> None:
        async with self.lock:
            now = self.clock()
            reserved = max(now, self.next_at)
            self.next_at = reserved + self.interval
        delay = reserved - now
        if delay > 0:
            await self.sleep(delay)


class APIQueue:
    def __init__(
        self,
        client: httpx.AsyncClient,
        config: QueueConfig | None = None,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.client = client
        self.config = config or QueueConfig()
        self.sleep = sleep
        self.clock = clock
        self.limiter = RateLimiter(self.config.requests_per_second, clock=clock, sleep=sleep)

    async def _execute(self, job: Job) -> Result:
        started = self.clock()
        can_retry = job.method in SAFE_METHODS or job.retry_safe
        status: int | None = None
        body = ""
        error: str | None = None
        for attempt in range(1, self.config.attempts + 1):
            await self.limiter.wait()
            retry_header: str | None = None
            try:
                async with self.client.stream(
                    job.method,
                    job.url,
                    json=job.json_body,
                    timeout=self.config.timeout,
                    follow_redirects=False,
                ) as response:
                    status = response.status_code
                    retry_header = response.headers.get("retry-after")
                    chunks = bytearray()
                    async for chunk in response.aiter_bytes():
                        if len(chunks) + len(chunk) > self.config.max_response_bytes:
                            return Result(
                                job.id,
                                status,
                                attempt,
                                "",
                                "response_size_limit",
                                (self.clock() - started) * 1000,
                            )
                        chunks.extend(chunk)
                    body = bytes(chunks).decode(response.encoding or "utf-8", errors="replace")
                error = None if 200 <= status < 300 else f"http_{status}"
                retry = status in RETRY_STATUSES
            except httpx.TransportError as exc:
                # Exception messages may contain secrets from query strings; log type only.
                error = type(exc).__name__
                status = None
                body = ""
                retry = True
            LOGGER.info(
                json.dumps(
                    {
                        "event": "attempt",
                        "job_id": job.id,
                        "attempt": attempt,
                        "status": status,
                        "error": error,
                    }
                )
            )
            if not retry or not can_retry or attempt == self.config.attempts:
                break
            instructed = retry_after(retry_header)
            delay = (
                instructed if instructed is not None else self.config.backoff * 2 ** (attempt - 1)
            )
            if delay > self.config.max_retry_delay:
                error = "retry_delay_limit"
                break
            await self.sleep(delay)
        return Result(job.id, status, attempt, body, error, (self.clock() - started) * 1000)

    async def run(self, jobs: Iterable[Job]) -> list[Result]:
        """Consume a bounded queue; preserve input ordering; cancel all tasks on failure."""
        queue: asyncio.Queue[tuple[int, Job] | None] = asyncio.Queue(self.config.queue_size)
        results: dict[int, Result] = {}

        async def produce() -> None:
            ids: set[str] = set()
            for index, job in enumerate(jobs):
                if index >= self.config.max_jobs:
                    raise ValueError("max_jobs exceeded")
                if job.id in ids:
                    raise ValueError(f"Duplicate job id: {job.id}")
                ids.add(job.id)
                await queue.put((index, job))
            for _ in range(self.config.concurrency):
                await queue.put(None)

        async def consume() -> None:
            while True:
                item = await queue.get()
                try:
                    if item is None:
                        return
                    index, job = item
                    results[index] = await self._execute(job)
                finally:
                    queue.task_done()

        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(produce())
            for _ in range(self.config.concurrency):
                tasks.create_task(consume())
        return [results[index] for index in sorted(results)]


def result_json(result: Result) -> str:
    return json.dumps(asdict(result), ensure_ascii=False)
