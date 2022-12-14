from __future__ import annotations

from typing import AsyncIterable, Iterable, cast

from synkka.types import TIn


def iterable_to_async_iterable(
    iterable: Iterable[TIn] | AsyncIterable[TIn],
) -> AsyncIterable[TIn]:
    """
    Converts an iterable to an async iterable.
    """
    if hasattr(iterable, "__aiter__"):
        return cast(AsyncIterable[TIn], iterable)

    async def async_iter():
        for item in iterable:
            yield item

    return async_iter()


async def async_chunked(
    items: Iterable[TIn] | AsyncIterable[TIn], chunk_size: int
) -> AsyncIterable[list[TIn]]:
    """
    Yields chunks of `items` of (up to) size `chunk_size`.
    """
    chunk: list[TIn] = []
    async for item in iterable_to_async_iterable(items):
        chunk.append(item)
        if len(chunk) == chunk_size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk
