import asyncio
from zpz.a_sync import concurrent_gather

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

