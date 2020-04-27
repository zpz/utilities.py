import asyncio
import logging
import multiprocessing as mp
from collections.abc import AsyncIterable, AsyncIterator, Iterable, Iterator
from concurrent.futures import ProcessPoolExecutor
from typing import Callable, Awaitable, Any, Union

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


class BufferedTransformer:
    def __init__(
            self,
            input_stream: AsyncIterable,
            trans_func: Callable[[Any], Union[asyncio.Future, asyncio.Task]],
            buffer_size: int,
            *,
            n_buffers: int = 8,
            keep_order: bool = True,
            return_exceptions: bool = False,
            loop=None,
    ):
        '''
        trans_func: takes a single input, returns a `Task` (created by `create_task`)
            or a `Future` (created by `run_in_executor`); see `Streamer.transform`
            and `Streamer.mp_transform`. Example of the real action: web request.
        '''
        assert isinstance(input_stream, AsyncIterable)
        assert 0 < buffer_size <= 10000
        assert 0 < n_buffers <= 64

        self._input_stream = input_stream
        self._trans_func = trans_func
        self._task_get_input = None
        self._loop = loop or asyncio.get_event_loop()
        self._keep_order = keep_order
        self._return_exceptions = return_exceptions

        if n_buffers > 1:
            logger.warning('buffer queue not implemented yet')

        self._q = []
        self._sem = asyncio.Semaphore(buffer_size)
        self._no_more_input = False

    def __del__(self):
        if self._task_get_input is not None and not self._task_get_input.done():
            self._task_get_input.cancel()

    async def _get_input(self):
        async for x in self._input_stream:
            await self._sem.acquire()
            self._q.append(self._trans_func(x))
        self._no_more_input = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._task_get_input is None:
            self._task_get_input = self._loop.create_task(self._get_input())

        while not self._q:
            if self._no_more_input:
                await self._task_get_input
                raise StopAsyncIteration
            await asyncio.sleep(0.0001)

        if self._keep_order:
            task = self._q.pop(0)
            self._sem.release()
            try:
                y = await task
            except Exception as e:
                if self._return_exceptions:
                    logger.warning('%s; Exception object is returned', e)
                    y = e
                else:
                    raise
        else:
            if self._q[0].done():
                task = self._q.pop(0)
                self._sem.release()
            else:
                done, pending = await asyncio.wait(self._q, return_when=asyncio.FIRST_COMPLETED)

                task = done.pop()
                self._sem.release()

                q = list(done) + list(pending)
                if len(self._q) == len(q) + 1:
                    self._q = q
                else:
                    # More tasks were added to `self._q` when we were waiting above.
                    logger.debug('%d tasks before waiting, %d after',
                                 len(q) + 1, len(self._q))
                    assert len(self._q) > len(q) + 1
                    self._q = q + self._q[len(q) + 1 :]

            try:
                y = task.result()
            except Exception as e:
                if self._return_exceptions:
                    logger.warning('%s; Exception object is returned', e)
                    y = e
                else:
                    raise

        if self._task_get_input.done():
            if self._task_get_input.exception():
                raise self._task_get_input.exception()

        return y


class Transformer:
    def __init__(
            self,
            input_stream: AsyncIterable,
            trans_func: Callable[[Any], Union[asyncio.Task, asyncio.Future]],
            *,
            return_exceptions: bool = False,
    ):
        assert isinstance(input_stream, AsyncIterable)
        self._input_stream = input_stream
        self._trans_func = trans_func
        self._return_exceptions = return_exceptions

    async def __aiter__(self):
        async for z in self._input_stream:
            try:
                yield await self._trans_func(z)
            except Exception as e:
                if self._return_exceptions:
                    logger.warning('%s; Exception object is returned', e)
                    yield e
                else:
                    raise


class Streamer:
    def __init__(
            self,
            input_stream: AsyncIterable,
            n_process_workers: int = -2,
            loop=None,
    ):
        self._transformers = [input_stream]
        self._process_executor: ProcessPoolExecutor = None
        self._loop = loop or asyncio.get_event_loop()
        self._n_process_workers = mp.cpu_count() + (n_process_workers or 0)

    def _transform(
        self,
        trans_func: Callable[[Any], Union[asyncio.Task, asyncio.Future]],
        *,
        buffer_size: int = None,
        keep_order: bool = True,
        return_exceptions: bool = False,
    ):
        if buffer_size and buffer_size > 1:
            self._transformers.append(
                BufferedTransformer(
                    self._transformers[-1],
                    trans_func,
                    buffer_size=buffer_size,
                    keep_order=keep_order,
                    return_exceptions=return_exceptions,
                )
            )
        else:
            self._transformers.append(
                Transformer(
                    self._transformers[-1],
                    trans_func,
                    return_exceptions=return_exceptions,
                )
            )
        return self

    def transform(
            self,
            trans_func: Callable[[Any], Awaitable[Any]],
            *args,
            buffer_size: int = None,
            **kwargs,
    ):
        if buffer_size and buffer_size > 1:
            def transformer_(x):
                return self._loop.create_task(trans_func(x, *args))
        else:
            async def transformer_(x):
                return await trans_func(x, *args)

        return self._transform(transformer_, buffer_size=buffer_size, **kwargs)

    def __aiter__(self):
        z = self._transformers[-1]
        if not isinstance(z, AsyncIterator):
            assert isinstance(z, AsyncIterable)
            z = z.__aiter__()
        return z

    def collect(self, out, log_every: int = 1000) -> None:
        async def foo():
            n = 0
            async for x in self:
                out.append(x)
                if log_every:
                    n += 1
                    if n % log_every == 0:
                        logger.info('collected %d items', n)

        self._loop.run_until_complete(foo())