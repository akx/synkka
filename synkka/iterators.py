from __future__ import annotations

import asyncio
from typing import Any, AsyncIterable, Callable, Coroutine, Iterable, cast

from synkka.exceptions import TaskFailed
from synkka.types import TIn, TOut
from synkka.utilities import async_chunked, iterable_to_async_iterable


class Sentinel:
    pass


async def imap_chunked_pairs(
    func: Callable[[TIn], Coroutine[Any, Any, TOut]],
    items: Iterable[TIn] | AsyncIterable[TIn],
    chunk_size: int,
) -> AsyncIterable[tuple[TIn, TOut]]:
    """
    Maps `func` over `items` in chunks of size `chunk_size`.
    Yields pairs of (item, result).
    """
    chunk: list[TIn]
    async for chunk in async_chunked(items, chunk_size):
        for pair in zip(chunk, await asyncio.gather(*[func(thing) for thing in chunk])):
            yield pair


async def imap_unordered_pairs(
    func: Callable[[TIn], Coroutine[Any, Any, TOut]],
    items: Iterable[TIn] | AsyncIterable[TIn],
    concurrency: int,
) -> AsyncIterable[tuple[TIn, TOut]]:
    """
    Maps `func` over `items` with `concurrency` workers.
    Yields pairs of (item, result).
    """
    sentinel = Sentinel()
    inbox: asyncio.Queue[TIn | Sentinel] = asyncio.Queue()
    outbox: asyncio.Queue[
        tuple[TIn | Sentinel, TOut | None, BaseException | None]
    ] = asyncio.Queue()
    cancel = False
    n_workers_quit = 0

    async def worker():
        nonlocal n_workers_quit
        while not cancel:
            work_item = await inbox.get()
            if work_item is sentinel:
                break
            try:
                work_result = await func(work_item)
            except Exception as e:
                await outbox.put((work_item, None, e))
            else:
                await outbox.put((work_item, work_result, None))
        n_workers_quit += 1
        if n_workers_quit == concurrency:
            await outbox.put((sentinel, None, None))

    async for item in iterable_to_async_iterable(items):
        inbox.put_nowait(item)
    for _ in range(concurrency):
        inbox.put_nowait(sentinel)

    workers = [asyncio.create_task(worker()) for _ in range(concurrency)]
    while True:
        res_item, result, exception = await outbox.get()
        if res_item is sentinel:
            break
        assert not isinstance(res_item, Sentinel)
        if exception is not None:
            cancel = True
            raise TaskFailed(str(exception), item=res_item) from exception
        yield (res_item, cast(TOut, result))
    await asyncio.gather(*workers)
