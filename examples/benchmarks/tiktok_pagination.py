"""Benchmark: sync sequential pagination vs async bounded-concurrent pagination.

Measures the payoff of converting a connector's ``data()`` to ``async`` — the
TikTok Ads paginator (`_paginate`) reports ``total_page`` on the first response,
so pages 2..N are fetched concurrently (bounded by ``PAGE_CONCURRENCY``) instead
of one at a time.

Fair latency model: a custom **async** transport that ``await asyncio.sleep``s
(letting the event loop overlap requests) vs a **sync** transport that
``time.sleep``s. The async path runs the real connector code; the sync path is
the pre-conversion reference loop. No network — deterministic and offline.

Run: ``uv run python examples/benchmarks/tiktok_pagination.py``

Representative result (100 ms/request, PAGE_CONCURRENCY=8):

     pages   sync (s)  async (s)   speedup
         1      0.106      0.102      1.0x   # nothing to overlap
         5      0.523      0.204      2.6x
        20      2.091      0.408      5.1x
        50      5.243      0.814      6.4x   # → caps near PAGE_CONCURRENCY

Takeaway: the win scales with pages and tops out near ``PAGE_CONCURRENCY``;
single-page reports gain nothing (correctly). Async pays off for deep reports.
"""

from __future__ import annotations

import asyncio
import time
import types

import httpx
from interloper.rest.client import AsyncRESTClient
from interloper_assets.tiktok_ads import constants
from interloper_assets.tiktok_ads.source import _paginate

LATENCY = 0.10  # seconds per request


def _page(page: int, total_page: int) -> dict:
    return {"code": 0, "data": {"list": [{"i": page}], "page_info": {"total_page": total_page}}}


class _AsyncLatency(httpx.AsyncBaseTransport):
    def __init__(self, total_page: int) -> None:
        self.total_page = total_page

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        await asyncio.sleep(LATENCY)
        return httpx.Response(200, json=_page(int(request.url.params.get("page", "1")), self.total_page))


class _SyncLatency(httpx.BaseTransport):
    def __init__(self, total_page: int) -> None:
        self.total_page = total_page

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        time.sleep(LATENCY)
        return httpx.Response(200, json=_page(int(request.url.params.get("page", "1")), self.total_page))


def _sync_paginate(client: httpx.Client, total_page: int) -> list:
    """The pre-conversion sequential loop, for comparison."""
    items: list = []
    page, total = 1, 1
    while page <= total:
        r = client.get("/x/", params={"page": page, "page_size": constants.PAGE_SIZE})
        r.raise_for_status()
        data = r.json()["data"]
        items.extend(data["list"])
        total = data["page_info"]["total_page"]
        page += 1
    return items


async def _time_async(total_page: int) -> float:
    conn = types.SimpleNamespace(aclient=AsyncRESTClient("https://t.test", transport=_AsyncLatency(total_page)))
    start = time.perf_counter()
    async with conn.aclient:
        items = await _paginate(conn, "/x/", {})
    assert len(items) == total_page
    return time.perf_counter() - start


def _time_sync(total_page: int) -> float:
    client = httpx.Client(base_url="https://t.test", transport=_SyncLatency(total_page))
    start = time.perf_counter()
    items = _sync_paginate(client, total_page)
    client.close()
    assert len(items) == total_page
    return time.perf_counter() - start


def main() -> None:
    print(f"per-request latency = {LATENCY * 1000:.0f} ms, PAGE_CONCURRENCY = {constants.PAGE_CONCURRENCY}\n")
    print(f"{'pages':>6} {'sync (s)':>10} {'async (s)':>10} {'speedup':>9}")
    for total_page in (1, 5, 20, 50):
        sync_s = _time_sync(total_page)
        async_s = asyncio.run(_time_async(total_page))
        print(f"{total_page:>6} {sync_s:>10.3f} {async_s:>10.3f} {sync_s / async_s:>8.1f}x")


if __name__ == "__main__":
    main()
