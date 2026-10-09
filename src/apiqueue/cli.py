from __future__ import annotations

import argparse
import asyncio
import json
import logging
import tomllib
from collections.abc import Iterator
from pathlib import Path

import httpx

from apiqueue.core import APIQueue, Job, QueueConfig, result_json


def read_jobs(path: Path, max_jobs: int) -> Iterator[Job]:
    with path.open(encoding="utf-8") as handle:
        count = 0
        while line := handle.readline(1_000_001):
            if len(line) > 1_000_000:
                raise ValueError("Job line exceeds 1 MB")
            if not line.strip():
                continue
            count += 1
            if count > max_jobs:
                raise ValueError("max_jobs exceeded")
            yield Job(**json.loads(line))


async def run(path: Path, config: QueueConfig) -> int:
    limits = httpx.Limits(
        max_connections=config.concurrency, max_keepalive_connections=config.concurrency
    )
    async with httpx.AsyncClient(limits=limits, trust_env=False) as client:
        results = await APIQueue(client, config).run(read_jobs(path, config.max_jobs))
    for result in results:
        print(result_json(result))
    return int(any(result.error is not None for result in results))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run bounded HTTP jobs from a JSONL file")
    parser.add_argument("jobs", type=Path)
    parser.add_argument("--config", type=Path, help="TOML with a [queue] section")
    parser.add_argument("--verbose", action="store_true", help="JSON attempt logs on stderr")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING, format="%(message)s"
    )
    try:
        settings = tomllib.loads(args.config.read_text()) if args.config else {}
        if set(settings) - {"queue"}:
            raise ValueError("Unknown configuration section")
        return asyncio.run(run(args.jobs, QueueConfig(**settings.get("queue", {}))))
    except (OSError, ValueError, TypeError, ExceptionGroup) as exc:
        parser.exit(2, f"apiqueue: {exc}\n")
    return 2
