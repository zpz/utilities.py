import asyncio
import math
import random

import pytest

from zpz.streamer import Stream, Buffer, Transformer, Sink, Batcher, Unbatcher


async def f1(x):
    await asyncio.sleep(random.random() * 0.01)
    return x +  3.8


async def f2(x):
    await asyncio.sleep(random.random() * 0.01)
    return x*2


SYNC_INPUT = list(range(278))


@pytest.mark.asyncio
async def test_transformer():
    expected = [v + 3.8 for v in SYNC_INPUT]
    s = Transformer(Stream(SYNC_INPUT), f1)
    got = [v async for v in s]
    assert got == expected


def generate_data():
    for x in SYNC_INPUT:
        yield x


async def a_generate_data():
    for x in SYNC_INPUT:
        yield x


@pytest.mark.asyncio
async def test_input():
    expected = [v + 3.8 for v in SYNC_INPUT]

    s = Transformer(Stream(generate_data()), f1)
    got = [v async for v in s]
    assert got == expected

    s = Transformer(a_generate_data(), f1)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_chain():
    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Stream(SYNC_INPUT)
    s = Transformer(s, f1)
    s = Transformer(s, f2)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_buffer():
    expected = [(v + 3.8) * 2 for v in SYNC_INPUT]
    s = Transformer(
            Transformer(
                Buffer(Stream(SYNC_INPUT)),
                f1),
            f2)
    got = [v async for v in s]
    assert got == expected


@pytest.mark.asyncio
async def test_sink():
    class MySink:
        def __init__(self):
            self.result = 0

        async def __call__(self, x):
            await asyncio.sleep(random.random() * 0.01)
            self.result += x * 3

    mysink = MySink()
    s = Transformer(Stream(SYNC_INPUT), f1)
    s = Sink(s, mysink)
    await s.a_drain()
    got = mysink.result

    expected = sum((v + 3.8) * 3 for v in SYNC_INPUT)
    assert math.isclose(got, expected)


async def f3(x):
    return sum(x)


@pytest.mark.asyncio
async def test_batch():
    data = list(range(11))
    s = Transformer(Batcher(Stream(data), 3), f3)
    expected = [3, 12, 21, 19]
    got = [v async for v in s]
    assert got == expected


async def f4(x):
    return [x-1, x+1]


@pytest.mark.asyncio
async def test_unbatcher():
    data = [1, 2, 3, 4, 5]
    s = Unbatcher(Transformer(Stream(data), f4))
    expected = [0, 2, 1, 3, 2, 4, 3, 5, 4, 6]
    got = [v async for v in s]
    assert got == expected
