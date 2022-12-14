import asyncio

import pytest

from synkka.exceptions import TaskFailed
from synkka.iterators import imap_chunked_pairs, imap_unordered_pairs


@pytest.mark.asyncio
@pytest.mark.parametrize("imap_fun", [imap_chunked_pairs, imap_unordered_pairs])
async def test_imap_pairs_functions(imap_fun):
    in_flight = 0
    flight_limit = 5

    async def do_thing(value):
        nonlocal in_flight
        try:
            in_flight += 1
            await asyncio.sleep(0)
            return value * 10
        finally:
            assert in_flight <= flight_limit, "Too many things in flight"
            in_flight -= 1

    async for a, b in imap_fun(do_thing, range(50), flight_limit):
        assert a * 10 == b


@pytest.mark.asyncio
async def test_imap_unordered_pairs_does_no_work_after_first_exception():
    did_fail = False

    async def do_thing(value):
        nonlocal did_fail
        assert not did_fail  # We shouldn't enter this function after the first failure
        if value == 5:
            did_fail = True
            raise ValueError("Nope")
        await asyncio.sleep(0)
        return value * 10

    with pytest.raises(TaskFailed) as ei:
        async for a, b in imap_unordered_pairs(do_thing, range(10), 2):
            pass
    assert ei.value.args[0] == "Nope"
    assert isinstance(ei.value.__cause__, ValueError)  # original exception is preserved
    assert did_fail
