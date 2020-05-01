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


class Batcher:
    '''
    A `Batcher` object takes elements from an input stream,
    and bundle them up into batches up to a size limit,
    and produce the batches in an iterable.
    '''
    def __init__(self,
                 input_stream: Union[AsyncIterable, AsyncIterator],
                 batch_size: int,
                 timeout_seconds: float = None,
                 ):
        if not isinstance(input_stream, AsyncIterator):
            assert isinstance(input_stream, AsyncIterable)
            input_stream = input_stream.__aiter__()
        assert 0 < batch_size <= 10000
        self._input_stream = input_stream
        self._batch_size = batch_size
        if timeout_seconds:
            assert timeout_seconds > 0
        self._timeout = timeout_seconds
        self._start_time = None
        self._batch = [None for _ in range(self._batch_size)]
        self._batch_len = 0
        self._loop = asyncio.get_event_loop()

    def __aiter__(self):
        return self
    
    async def __anext__(self):
        while self._batch_len < self._batch_size:
            try:
                if not self._timeout:
                    z = await self._input_stream.__anext__()
                else:
                    if self._batch_len == 0:
                        z = await self._input_stream.__anext__()
                        if self._timeout:
                            self._start_time = self._loop.time()
                    else:
                        if self._loop.time() - self._start_time >= self._timeout:
                            break
                        try:
                            z = await asyncio.wait_for(
                                self._input_stream.__anext__(),
                                self._timeout - (self._loop.time() - self._start_time),
                            )
                        except asyncio.TimeoutError:
                            # TODO
                            # when timeout happens, will it mess up the state of
                            # `self._input_stream`?
                            # Return with the partial batch.
                            break
                self._batch[self._batch_len] = z
                self._batch_len += 1
            except StopAsyncIteration:
                break

        if self._batch_len > 0:
            z = self._batch[: self._batch_len]
            self._batch = [None for _ in range(self._batch_size)]
            self._batch_len = 0
            return z
        else:
            raise StopAsyncIteration

    
class Unbatcher:
    def __init__(self, input_stream: Union[AsyncIterable, AsyncIterator]):
        if not isinstance(input_stream, AsyncIterator):
            assert isinstance(input_stream, AsyncIterable)
            input_stream = input_stream.__aiter__()
        self._input_stream = input_stream
        self._batch = None

    def __aiter__(self):
        return self

    async def __anext__(self):
        while True:
            if self._batch is None:
                batch = await self._input_stream.__anext__()
                # If this raises StopAsyncIteration, let it propagate
                if not isinstance(batch, Iterator):
                    batch = iter(batch)
                self._batch = batch
            try:
                return next(self._batch)
            except StopIteration:
                self._batch = None


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


class BufferedSink:
    def __init__(
            self,
            input_stream: AsyncIterable,
            sink_func: Callable[[Any], Union[asyncio.Task, asyncio.Future]],
            buffer_size: int,
            skip_exceptions: bool = False,
    ):
        assert isinstance(input_stream, AsyncIterable)
        self._input_stream = input_stream
        self._sink_func = sink_func
        self._skip_exceptions = skip_exceptions
        self._task_get_input = None
        self._loop = asyncio.get_event_loop()
        assert 0 < buffer_size <= 10000
        self._q = asyncio.Queue()
        self._sem = asyncio.Semaphore(buffer_size)

    def __del__(self):
        if self._task_get_input is not None and not self._task_get_input.done():
            self._task_get_input.cancel()

    async def _get_input(self):
        async for x in self._input_stream:
            await self._sem.acquire()
            self._q.put_nowait(self._sink_func(x))
        self._q.put_nowait(NO_MORE_DATA)
    
    async def a_drain(self, log_every: int = 1000):
        self._task_get_input = self._loop.create_task(self._get_input())
        n = 0
        while True:
            task = await self._q.get()
            self._sem.release()
            if task == NO_MORE_DATA:
                break
            try:
                await task
            except Exception as e:
                if self._skip_exceptions:
                    logger.warning('%s: skipped', e)
                else:
                    raise

            if log_every:
                n += 1
                if n % log_every == 0:
                    logger.info('drained %d items', n)
        await self._task_get_input

    def drain(self, log_every: int = 1000):
        self._loop.run_until_complete(self.a_drain(log_every))


class Sink:
    def __init__(self,
                 input_stream: AsyncIterable,
                 sink_func: Callable[[Any], Union[asyncio.Future, asyncio.Task]],
                 skip_exceptions: bool = False,
                 ):
        assert isinstance(input_stream, AsyncIterable)
        self._input_stream = input_stream
        self._sink_func = sink_func
        self._skip_exceptions = skip_exceptions
        self._loop = asyncio.get_event_loop()

    async def a_drain(self, log_every: int = 1000):
        n = 0
        async for v in self._input_stream:
            try:
                await self._sink_func(v)
            except Exception as e:
                if self._skip_exceptions:
                    logger.warning('%s: skipped', e)
                else:
                    raise

            if log_every:
                n += 1
                if n % log_every == 0:
                    logger.info('drained %d items', n)

    def drain(self, log_every: int = 1000):
        self._loop.run_until_complete(self.a_drain(log_every))


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

    def batch(self, batch_size: int, timeout_seconds: float = None):
        self._transformers.append(
            Batcher(
                self._transformers[-1],
                batch_size,
                timeout_seconds
            )
        )

    def unbatch(self):
        self._transformers.append(Unbatcher(self._transformers[-1]))
        return self

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

    def _sink(
            self,
            sink_func: Callable[[Any], Union[asyncio.Task, asyncio.Future]],
            *,
            buffer_size: int = None,
            skip_exceptions: bool = False,
    );
        if buffer_size and buffer_size > 1:
            return BufferedSink(
                self._transformers[-1],
                sink_func,
                buffer_size=buffer_size,
                skip_exceptions=skip_exceptions,
            )
        else:
            return Sink(self._transformers[-1], sink_func, skip_exceptions=skip_exceptions)

    def sink(self,
             sink_func: Callable[[Any], Awaitable[None]],
             *args,
             buffe_size: int = None,
             **kwargs):
        if buffe_size is None or buffe_size < 2:
            async def sink_(x):
                await sink_func(x, *args)
        else:
            def sink_(x):
                return self._loop.create_task(sink_func(x, *args))

        return self._sink(sink_func, buffer_size=buffe_size, **kwargs)

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