"""Two HTTP jobs, a simulated rate-limit response, and no external network."""

import asyncio

import httpx

from apiqueue.core import APIQueue, Job, QueueConfig, result_json


async def main():
    attempts = {}

    def respond(request):
        key = request.url.path
        attempts[key] = attempts.get(key, 0) + 1
        if key == "/busy" and attempts[key] == 1:
            return httpx.Response(429, headers={"Retry-After": "0"})
        return httpx.Response(200, json={"path": key, "source": "offline fixture"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        queue = APIQueue(client, QueueConfig(requests_per_second=100, backoff=0.01))
        for result in await queue.run(
            [Job("ok", "https://fixture.test/ok"), Job("busy", "https://fixture.test/busy")]
        ):
            print(result_json(result))


asyncio.run(main())
