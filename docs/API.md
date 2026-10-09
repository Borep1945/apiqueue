# Python API reference

All imports below are from `apiqueue.core` unless specified. The caller owns the
HTTPX client and must close it after the batch. Python 3.11 or newer is required.

## QueueConfig

`QueueConfig(concurrency=4, queue_size=16, requests_per_second=5.0, timeout=10.0,
attempts=3, backoff=0.5, max_retry_delay=60.0, max_response_bytes=1_000_000,
max_jobs=10_000)` is a frozen dataclass. Counts must be positive integers;
rates, timeouts and delays must be finite positive numbers. Invalid fields raise
`ValueError`, and unknown constructor arguments raise `TypeError`.

`concurrency` is the worker count per batch. `queue_size` limits pending input;
it does not cap retained results. `timeout` is HTTPX's timeout per request phase,
not an overall batch deadline. The caller can wrap `run` in `asyncio.wait_for`
for a whole-batch deadline. Cancellation propagates through TaskGroup to workers.
`max_jobs` caps retained results and input ids. `max_response_bytes` applies to
decoded response byte chunks before converting the body to text.

## Job

`Job(id: str, url: str, method: str = "GET", json_body: Any = None,
retry_safe: bool = False)` is a frozen dataclass. Ids contain 1–128 characters
and must be unique within one batch. URLs require an HTTP(S) scheme and host and
cannot contain embedded credentials. Supported methods are uppercase GET, HEAD,
OPTIONS, POST, PUT, PATCH and DELETE.

GET, HEAD and OPTIONS can retry by default. A write can retry only when
`retry_safe=True`; the caller must provide a genuine server-side idempotency
guarantee. The library neither supplies nor invents idempotency keys. Job ids
appear in attempt logs; use identifiers without secrets.

## APIQueue

`APIQueue(client: httpx.AsyncClient, config: QueueConfig | None = None, *,
sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
clock: Callable[[], float] = time.monotonic)` uses injected sleep and clock
functions for deterministic tests.

`await queue.run(jobs: Iterable[Job]) -> list[Result]` consumes jobs lazily and
returns results in input order. A batch with no jobs returns an empty list.
Each instance has one shared pacing limiter, including retries. If callers run
multiple batches concurrently on the same instance, pacing is shared but each
batch creates its own worker count; concurrency is therefore limited per batch.

Producer errors (duplicate ids, exceeded job count, exceptions from the input
iterator) propagate as an `ExceptionGroup` and cancel workers. Requests already
sent are not rolled back. Late input validation may occur after earlier jobs
have executed, so this is not an atomic batch transaction.

```python
import asyncio
import httpx
from apiqueue.core import APIQueue, Job, QueueConfig

async def fetch_health():
    async with httpx.AsyncClient(
        headers={"Authorization": "Bearer YOUR_TOKEN"}, trust_env=False
    ) as client:
        queue = APIQueue(client, QueueConfig(concurrency=2, requests_per_second=3))
        return await asyncio.wait_for(queue.run([
            Job("health", "https://YOUR_API/health")
        ]), timeout=30)
```

## Result

`Result(id: str, status: int | None, attempts: int, body: str,
error: str | None, duration_ms: float)` is a frozen dataclass.

`status` is absent for a transport failure. `body` is decoded text and is not
byte-preserving for binary responses. Successful 2xx responses have `error=None`.
Errors include `http_<status>`, HTTPX transport exception type names,
`response_size_limit` and `retry_delay_limit`. Results can contain remote private
data. `result_json(result: Result) -> str` produces one JSON object without a
trailing newline.

429, 500, 502, 503, 504 and transport errors are candidates for finite retries.
Redirects are returned without following them. Retries remain subject to method
safety and the attempt limit. `retry_after(value: str | None, *,
now: datetime | None = None) -> float | None` parses seconds or an HTTP date;
invalid input returns `None`. Server-directed delays above the maximum end the
job instead of retrying before the instructed time.

## RateLimiter and JSONL input

`RateLimiter(rate: float, *, clock=time.monotonic, sleep=asyncio.sleep)` is the
internal pacing primitive. `await limiter.wait()` reserves a globally spaced
start time. Use the validated `QueueConfig`/`APIQueue` entry point for public
batches; direct limiter callers must supply a positive finite rate.

`apiqueue.cli.read_jobs(path: Path, max_jobs: int) -> Iterator[Job]` reads UTF-8
JSONL, skips empty lines, limits one line to 1 MB, and rejects a count above the
selected maximum. It is incremental and does not prevalidate the entire file.

See [architecture](architecture.md) and [local verification](VERIFICATION.md)
for operational limits and checks. Automated tests use MockTransport; a live
service, real authentication and server-side idempotency are not verified by them.
