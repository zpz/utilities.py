import asyncio
import logging
import multiprocessing
import traceback
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Union

from .a_sync import create_loud_task, IO_WORKERS

logger = logging.getLogger(__name__)

NO_MORE_DATA = object()

# Iterable vs iterator
#
# if we need to use
#
#   for v in X:
#       ...
#
# then `X.__iter__()` is called to get an "iterator".
# In this case, `X` is an "iterable", and it must implement `__iter__`.
#
# If we do not use it that way, but rather only directly call
#
#   next(X)
#
# then `X` must implement `__next__` (but does not need to implement `__iter__`).
# This `X` is an "iterator".
#
# Often we let `__iter__` return `self`, and implement `__next__` in the same class.
# This way the class is both an iterable and an iterator.


async def stream(x: Iterable):
    '''
    Turn a sync iterable into an async iterable.
    '''
    for xx in x:
        yield xx
        await asyncio.sleep(0)


class Queue(asyncio.Queue, AsyncIterator):
    def __aiter__(self):
        return self

    async def __anext__(self):
        z = await self.get()
        if z is NO_MORE_DATA:
            raise StopAsyncIteration
        return z


class Buffer(Queue):
    def __init__(self, in_stream: AsyncIterable, buffer_size: int = None):
        super().__init__(maxsize=buffer_size or 1024)
        self._in_stream = in_stream
        self._t_start = create_loud_task(self._start())

    async def _start(self):
        async for x in self._in_stream:
            await self.put(x)
        await self.put(NO_MORE_DATA)


async def batch(x: AsyncIterable, batch_size: int):
    '''
    Take elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size, except
    possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady
    supply.
    If that is a concern, having a `Buffer` on the input stream
    may help.
    '''
    assert 0 < batch_size <= 10000
    batch_ = []
    n = 0
    async for xx in x:
        batch_.append(xx)
        n += 1
        if n >= batch_size:
            yield batch_
            batch_ = []
            n = 0
    if n:
        yield batch_


async def unbatch(batches: AsyncIterable):
    async for batch in batches:
        for x in batch:
            yield x
            await asyncio.sleep(0)


async def transform(
    x: AsyncIterator,
    func,
    *,
    workers: int = None,
    out_buffer_size: int = None,
    **func_args,
):
    '''
    `func`: an async function that takes a single input item,
    and produces a result. Additional args can be passed in
    via `func_args`.

    The outputs are in the order
    of the input elements in `x`.

    `workers`: max number of concurrent calls to `func`.
    If <= 1, no concurrency.
    By default there are multiple.
    Pass in 0 or 1 to enforce single worker.
    '''
    in_stream = x
    if workers is None:
        workers = IO_WORKERS

    if workers < 2:
        async for x in in_stream:
            y = await func(x, **func_args)
            yield y
        return

    if out_buffer_size is None:
        out_buffer_size = workers * 2
    out_stream = Queue(out_buffer_size)
    finished = False

    async def _process():
        try:
            x = await in_stream.__anext__()
            fut = asyncio.Future()
            await out_stream.put(fut)
            y = await func(x, **func_args)
            fut.set_result(y)
        except StopAsyncIteration:
            nonlocal finished
            if not finished:
                finished = True
                await out_stream.put(NO_MORE_DATA)

    t_processes = [
        create_loud_task(_process())
        for _ in range(workers)
    ]

    while True:
        fut = await out_stream.get()
        if fut == NO_MORE_DATA:
            break
        yield await fut

    for t in t_processes:
        await t


async def unordered_transform(
    x: AsyncIterator,
    func,
    *,
    workers: int = None,
    out_buffer_size: int = None,
    **func_args,
):
    in_stream = x
    if workers is None:
        workers = IO_WORKERS
    assert workers > 1

    if out_buffer_size is None:
        out_buffer_size = workers * 2
    out_stream = Queue(out_buffer_size)
    finished = False

    async def _process():
        try:
            x = await in_stream.__anext__()
            y = await func(x, **func_args)
            await out_stream.put(y)
        except StopAsyncIteration:
            nonlocal finished
            if not finished:
                finished = True
                await out_stream.put(NO_MORE_DATA)

    t_processes = [
        create_loud_task(_process())
        for _ in range(workers)
    ]

    while True:
        y = await out_stream.get()
        if y == NO_MORE_DATA:
            break
        yield y

    for t in t_processes:
        await t


async def sink(
        x: AsyncIterable,
        func,
        *,
        workers: int = None,
        log_every: int = 1000,
        **func_args,
):
    '''
    `func`: an async function that takes a single input item
    but does not produce (useful) return.
    Example operation of `func`: insert into DB.
    Additional arguments can be passed in via `func_args`.

    When `workers > 1` (or is `None`),
    order of processing of elements in `in_stream`
    is NOT guaranteed to be the same as the elements' order
    in `in_stream`.
    However, the shuffling of order is local.
    '''
    if workers is not None and workers == 1:
        trans = transform(
            x,
            func,
            workers=workers,
            **func_args,
        )
    else:
        trans = unordered_transform(
            x,
            func,
            workers=workers,
            **func_args,
        )

    n = 0
    async for _ in trans:
        if log_every:
            n += 1
            if n % log_every == 0:
                logger.info('drained %d', n)
