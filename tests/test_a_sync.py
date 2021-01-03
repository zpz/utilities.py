import asyncio
from zpz.a_sync import concurrent_gather, MaybeAwait

import pytest


@pytest.mark.asyncio
async def test_concurrent_gather():
    async def square(x):
        await asyncio.sleep(0.0001)
        return x * x

    values = list(range(10000))
    tasks = [square(v) for v in values]
    got = sum(await concurrent_gather(*tasks))
    assert got == sum(v*v for v in values)


@pytest.mark.asyncio
async def _test_maybeawait():
    async def foo(x, y, negate=False):
        if negate:
            return -(x + y)
        return x + y

    async def goo(f: MaybeAwait, trigger: int):
        if trigger > 3:
            return await f
        return 'none'

    xx = MaybeAwait(foo, 3, 2, negate=True)
    assert (await goo(xx, 4)) == -5

    yy = MaybeAwait(foo, 4, 5, negate=False)
    assert (await goo(yy, 2)) == 'none'

    with pytest.warns(RuntimeError):
        def ff():
            _ = foo(7, 8)
            return 3
            # Will raise the "coroutine ... was never awaited"
            # warning

        assert ff() == 3

    async def gg():
        _ = MaybeAwait(ff, 7, 8)
        return 3
        # No warning

    assert (await gg()) == 3
