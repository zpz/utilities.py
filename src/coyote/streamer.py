import asyncio
import logging
import multiprocessing
import traceback
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from typing import Union

from .a_sync import create_loud_task

logger = logging.getLogger(__name__)

NO_MORE_DATA = 'THERE WILL BE NO MORE DATA, DUDE'

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


# An `AsyncIterable` flows through most functions defined in this module.
# An `AsyncIterable` is either an object that has method `__aiter__`,
# or an `AsyncGenerator` like this:
#
#   async def make_data():
#       for x in ...:
#           yield x


class Stream(AsyncIterable):
    def __init__(self, data: Iterable):
        self._data = data

    async def __aiter__(self):
        for x in self._data:
            yield x


class Queue(asyncio.Queue, AsyncIterator):
    def __aiter__(self):
        return self

    async def __anext__(self):
        z = await self.get()
        if z == NO_MORE_DATA:
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


class Batcher(AsyncIterable):
    '''
    A `Batcher` object takes elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.
    '''
    def __init__(self,
                 in_stream: AsyncIterable,
                 batch_size: int,
                 ):
        assert 0 < batch_size <= 10000
        self._in_stream = in_stream
        self._batch_size = batch_size

    async def __aiter__(self):
        batch_size = self._batch_size
        batch = []
        n = 0
        async for x in self._in_stream:
            batch.append(x)
            n += 1
            if n == batch_size:
                yield batch
                batch = []
                n = 0
        if n:
            yield batch


class Unbatcher:
    '''
    An `Unbatcher` object does the opposite of `Batcher`.
    '''
    def __init__(self, in_stream: AsyncIterable):
        self._in_stream = in_stream

    async def __aiter__(self):
        async for batch in self._in_stream:
            for x in batch:
                yield x


class Transformer(AsyncIterable):
    def __init__(
        self,
        in_stream: AsyncIterable,
        func,
        *,
        workers: int = None,
        out_buffer_size: int = None,
    ):
        '''
        `func`: an async function that takes a single input item,
        and produces a result.

        The results put in `self._out_stream` are in the order
        of the input elements in `in_stream`.

        `workers`: max number of concurrent calls to `func`.
        If <= 1, no concurrency.
        '''
        self._in_stream = in_stream
        self._func = func

        if workers is None:
            workers = multiprocessing.cpu_count() * 4
        self._workers = workers

        self._out_stream = Queue(maxsize=out_buffer_size or 1024)
        self._t_start = create_loud_task(self._start())

    async def _collect(self):
        while True:
            v = await self._q_t.get()
            if v == NO_MORE_DATA:
                break
            z = await v
            await self._out_stream.put(z)
        await self._out_stream.put(NO_MORE_DATA)

    async def _start(self):
        if self._workers <= 1:
            async for x in self._in_stream:
                z = await self._func(x)
                await self._out_stream.put(z)
            await self._out_stream.put(NO_MORE_DATA)
        else:
            self._q_t = asyncio.Queue(self._workers)
            t_collect = create_loud_task(self._collect())
            async for x in self._in_stream:
                t = create_loud_task(self._func(x))
                await self._q_t.put(t)
            await self._q_t.put(NO_MORE_DATA)
            await t_collect

    def __aiter__(self):
        return self._out_stream


class Sink:
    def __init__(self,
                 in_stream: AsyncIterable,
                 func,
                 *,
                 workers: int = None,
    ):
        '''
        `workers`: max number of concurrent calls to `func`.
        If <= 1, no concurrency.

        `func`: an async function that takes a single input item
        but does not produce (useful) return.
        Example operation of `func`: insert into DB.

        When `workers > 1` (or is `None`),
        order of processing of elements in `in_stream`
        is NOT guaranteed to be the same as the elements' order
        in `in_stream`.
        '''
        self._in_stream = in_stream
        self._func = func

        if workers is None:
            workers = multiprocessing.cpu_count() * 4
        self._workers = workers
        if workers > 1:
            self._q_t = asyncio.Queue(workers)

    async def _start(self):
        async for x in self._in_stream:
            t = create_loud_task(self._func(x))
            await self._q_t.put(t)
        await self._q_t.put(NO_MORE_DATA)

    async def a_drain(self):
        # This must be called to finish up the job.
        if self._workers > 1:
            t = create_loud_task(self._start())
            while True:
                v = await self._q_t.get()
                if v == NO_MORE_DATA:
                    break
                await v
            await t
        else:
            async for x in self._in_stream:
                await self._func(x)
