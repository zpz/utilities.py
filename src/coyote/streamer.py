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
        if isinstance(self._data, AsyncIterable):
            async for x in self._data:
                yield x
        else:
            for x in self._data:
                yield x
                await asyncio.sleep(0)
                # TODO: is this needed?


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


class Batcher(AsyncIterable):
    '''
    A `Batcher` object takes elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.

    The output batches are all of the specified size, except
    possibly the final batch.
    There is no 'timeout' logic to produce a smaller batch.
    For efficiency, this requires the input stream to have a steady
    supply.
    If that is a concern, having a `Buffer` on the input stream
    may help. Note, the output of `Transformer` is already buffered.
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
            if n >= batch_size:
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
                await asyncio.sleep(0)
                # TODO: is this needed?


class Transformer(AsyncIterable):
    def __init__(
        self,
        in_stream: AsyncIterable,
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

        The results put in `self._out_stream` are in the order
        of the input elements in `in_stream`.

        `workers`: max number of concurrent calls to `func`.
        If <= 1, no concurrency.
        By default there are multiple.
        Pass in 0 or 1 to enforce single worker.
        '''
        self._in_stream = in_stream
        self._func = func

        if workers is None:
            workers = IO_WORKERS

        self._out_stream = Queue(maxsize=out_buffer_size or 1024)
        self._t_start = create_loud_task(self._start(
            func, workers=workers, **func_args
        ))

    async def _collect(self, q_t):
        while True:
            v = await q_t.get()
            if v is NO_MORE_DATA:
                break
            z = await v
            await self._out_stream.put(z)

    async def _collect2(self, q_t, NOOP):
        while True:
            v = await q_t.get()
            if v is NOOP:
                continue
            if v is NO_MORE_DATA:
                return
            z = await v
            await self._out_stream.put(z)

    async def _start(self, func, workers, **func_args):
        if workers < 2:
            async for x in self._in_stream:
                z = await func(x, **func_args)
                await self._out_stream.put(z)
            await self._out_stream.put(NO_MORE_DATA)
        elif workers == 2:
            q_t = asyncio.Queue(1)
            NOOP = object()
            t_collect = create_loud_task(self._collect2(q_t, NOOP))
            async for x in self._in_stream:
                await q_t.put(NOOP)
                t = create_loud_task(func(x, **func_args))
                await q_t.put(t)
            await q_t.put(NO_MORE_DATA)
            await t_collect
            await self._out_stream.put(NO_MORE_DATA)
        else:
            q_t = asyncio.Queue(workers - 2)
            t_collect = create_loud_task(self._collect(q_t))
            async for x in self._in_stream:
                t = create_loud_task(func(x, **func_args))
                await q_t.put(t)
            await q_t.put(NO_MORE_DATA)
            await t_collect
            await self._out_stream.put(NO_MORE_DATA)

    def __aiter__(self):
        return self._out_stream


class EagerTransformer(AsyncIterable):
    def __init__(self,
                 in_stream: AsyncIterable,
                 func,
                 *,
                 workers: int = None,
                 out_buffer_size: int = None,
                 **func_args,
                 ):
        self._in_stream = in_stream
        if workers is None:
            workers = IO_WORKERS
        else:
            assert workers > 1
        self._out_stream = Queue(maxsize=out_buffer_size or 1024)
        self._t_start = create_loud_task(self._start(
            workers, func, **func_args
        ))

    async def _collect(self, q, func, **func_args):
        while True:
            x = await q.get()
            if x is NO_MORE_DATA:
                return
            z = await func(x, **func_args)
            await self._out_stream.put(z)

    async def _start(self, workers, func, **func_args):
        q = Queue(self._out_stream.maxsize)
        ws = [
            create_loud_task(self._collect(q, func, **func_args))
            for _ in range(workers)
        ]
        async for x in self._in_stream:
            await q.put(x)
        for _ in workers:
            await q.put(NO_MORE_DATA)
        for w in ws:
            await w
        await self._out_stream.put(NO_MORE_DATA)

    def __aiter__(self):
        return self._out_stream


class Sink:
    def __init__(self,
                 in_stream: AsyncIterable,
                 func,
                 *,
                 workers: int = None,
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
            self._trans = Transformer(
                in_stream,
                func,
                workers=workers,
                **func_args,
            )
        else:
            self._trans = EagerTransformer(
                in_stream,
                func,
                workers=workers,
                **func_args,
            )

    async def a_drain(self, log_every: int = 1000):
        # This must be called to finish up the job.
        n = 0
        async for _ in self._trans:
            if log_every:
                n += 1
                if n % log_every == 0:
                    logger.info('drained %d', n)
