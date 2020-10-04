import asyncio
import itertools
import logging
import random
import time
from typing import List

import pytest

from coyote.batched_service import VectorTransformer, BatchedService
from coyote.logging import config_logger
from coyote.iterate import iterbatches
from coyote.mp import SubProcessError

logger = logging.getLogger(__name__)
config_logger(level='debug')


class MyModel(VectorTransformer):
    def preprocess(self, x: List[float]) -> List[int]:
        return [int(_ + 1) for _ in x]

    def transform(self, x: List[int]):
        logger.info('predicting %d items', len(x))
        time.sleep(1.2)
        return [_*_ for _ in x]


def do_single(service, x):
    async def run():
        tasks = [service.a_do_one(v) for v in x]
        y = await asyncio.gather(*tasks)
        return y

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(run())


def do_bulk(service, x):
    async def run():
        tasks = [service.a_do_bulk(list(batch)) for batch in iterbatches(x, 40)]
        y = await asyncio.gather(*tasks)
        return list(itertools.chain.from_iterable(y))

    loop = asyncio.get_event_loop()
    return loop.run_until_complete(run())


def test_single():
    x = [random.uniform(0.3, 98) for _ in range(333)]
    y = [int(v + 1) * int(v + 1) for v in x]

    my = BatchedService(max_batch_size=100, worker_class=MyModel)
    my.start()

    print('')
    z = do_single(my, x)
    assert z == y
    my.stop()


def test_bulk():
    x = [random.uniform(0.3, 98) for _ in range(333)]
    y = [int(v + 1) * int(v + 1) for v in x]

    my = BatchedService(max_batch_size=100, worker_class=MyModel)
    my.start()

    print('')
    z = do_bulk(my, x)
    assert z == y
    my.stop()


class Error9(Exception):
    pass


class Error5(Exception):
    pass


class Error10(Exception):
    pass


class YourModel(VectorTransformer):
    def transform(self, x: List[int]):
        time.sleep(0.2)
        if 9 in x:
            raise Error9('found 9')
        return x


class YourService(BatchedService):
    def __init__(self):
        super().__init__(max_batch_size=3, worker_class=YourModel)

    async def preprocess(self, x):
        if 5 in x:
            raise Error5('found 5')
        return x

    async def postprocess(self, x):
        if 10 in x:
            raise Error10('found 10')
        return x


def test_exception():
    you = YourService()
    you.start()

    run_async = asyncio.get_event_loop().run_until_complete

    z = run_async(you.a_do_one(3))
    assert z == 3

    with pytest.raises(SubProcessError):
        z = run_async(you.a_do_one(5))

    with pytest.raises(SubProcessError):
        z = run_async(you.a_do_bulk([7, 8, 9]))

    with pytest.raises(Error10):
        z = run_async(you.a_do_bulk([10, 11, 12]))

    you.stop()
