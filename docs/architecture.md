# Architecture and operational notes

The producer lazily consumes jobs into an `asyncio.Queue` of `queue_size`. A fixed
number of workers process jobs. `TaskGroup` cancels all tasks when the producer
fails or the caller cancels. Completed results are keyed by input position and
returned in input order. This bounds pending jobs, not total result memory.

Each HTTP attempt reserves one globally spaced start time under a lock. A slow
request can overlap a later start up to the concurrency cap. Retries acquire a
new reservation; a `Retry-After` applies to its job rather than pausing all jobs.
Limits apply per `APIQueue` instance. Concurrent batches sharing one instance
share pacing, but each batch has its own workers and hence its own concurrency cap.

HTTPX receives a per-phase timeout; this is not a whole-batch deadline. Retries
have finite attempts and a maximum permitted delay. A valid server-directed
wait above that delay ceiling ends the job. Missing or malformed Retry-After
uses deterministic exponential backoff; production deployments may want jitter
when many independent queue instances target the same server.

Do not use `retry_safe` on a write unless an application-level idempotency key or
server guarantee makes a repeated request safe. The client does not invent such
keys. A timed-out write may already have reached the server.

Job ids must be unique within a batch. The producer may discover a malformed
late job after earlier jobs have already executed: validation is incremental,
not an atomic all-or-nothing transaction. Retain outputs and handle side effects
at the application level. Results use decoded text, so binary responses are not
byte-preserving. The CLI has a one-megabyte JSONL line limit; job-file paths and
remote destinations are explicitly selected by the caller.

## References

The request/response lifetime uses HTTPX's documented [async streaming context](https://www.python-httpx.org/async/).
Structured concurrency follows Python's [TaskGroup contract](https://docs.python.org/3/library/asyncio-task.html#task-groups).
