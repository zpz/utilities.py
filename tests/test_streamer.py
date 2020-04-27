import asyncio
import logging
import random

from zpz.streamer import Streamer
from zpz.logger import config_logger

logger = logging.getLogger(__name__)
config_logger()


async def f1(x: int):
    await asyncio.sleep(random.random() * 0.01)
    return int(x +  3.8)


async def f2(x: int):
    await asyncio.sleep(random.random() * 0.01)
    return x*2


SYNC_INPUT = list(range(278))

async def get_input():
    for x in SYNC_INPUT:
        await asyncio.sleep(0)
        yield x


def test_transform():
    got = []
    Streamer(get_input()).transform(f1).transform(f2).collect(got, log_every=3)
    expected = [int(v + 3.8) * 2 for v in SYNC_INPUT]
    assert got == expected


def test_buffered_transform():
    got = []
    Streamer(get_input()).transform(f1, buffer_size=10).transform(f2, buffer_size=20).collect(got, log_every=3)
    expected = [int(v + 3.8) * 2 for v in SYNC_INPUT]
    assert got == expected

