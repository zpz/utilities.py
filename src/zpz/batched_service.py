import asyncio
import logging
import multiprocessing as mp
import threading
from abc import ABCMeta, abstractmethod
from typing import List, Type, Dict

from .mp import SubProcessError


logger = logging.getLogger(__name__)

NO_MORE_INPUT = 'THERE WILL NO NO MORE INPUT'
NO_MORE_OUTPUT = 'THERE WILL NO NO MORE OUTPUT'


class VectorTransformer(metaclass=ABCMeta):
    # Subclasses should define `__init__` to receive
    # necessary info. Ideally, parameters to `__init__`
    # are simple, built-in types such as numbers and strings.

    def preprocess(self, x: List) -> 'preprocessed':
        # This method takes the data from the main process
        # and prepares it for `transform`.
        # This method should not take arguments other than `x`.
        #
        # By default, `x` is returned w/o change.
        return x

    @abstractmethod
    def transform(self, x: 'preprocessed') -> 'transformed':
        # This is a "vectorized" function
        raise NotImplementedError

    def postprocess(self, pre: 'preprocessed', trans: 'transformed') -> 'postprocessed':
        # This method takes the outputs of `preprocess` and `transform`
        # and returns something that will be sent back to the main process
        # in a queue.
        # The output of this method must be pickleable.
        #
        # In typical situations, this method only needs `trans`, but the output
        # of `preprocess` (i.e. the input to `transform`) is also provided
        # just in case parts of it are needed in determining the return value.
        #
        # By default, `trans` is returned w/o change, and `pre` is ignored.
        return trans

    def terminate(self):
        pass

    def run(self, *, q_in: mp.Queue, q_out: mp.Queue):
        cls_name = self.__class__.__name__

        # Put a 'ready' signal in the queue.
        q_out.put(None)

        while True:
            x = q_in.get()
            if x == NO_MORE_INPUT:
                logger.info(f'{cls_name}: shutting down')
                self.terminate()
                return

            if isinstance(x, SubProcessError):
                logger.info('received SubProcessError from the main process; sending it back right away')
                w = x
            else:
                try:
                    y = self.preprocess(x)
                    z = self.transform(y)
                    w = self.postprocess(y, z)
                except Exception as e:
                    logger.exception(str(e))
                    logger.info('sending SubProcessError back to the main process')
                    w = SubProcessError(e)
            q_out.put(w)

    @classmethod
    def start(cls, *, q_in: mp.Queue, q_out: mp.Queue, init_kwargs: Dict = None):
        transformer = cls(**(init_kwargs or {}))
        transformer.run(q_in=q_in, q_out=q_out)


class BatchedService:
    def __init__(
        self,
        worker_class: Type[VectorTransformer],
        max_batch_size: int,
        *,
        timeout_seconds: float = None,
        max_queue_size: int = None,
        worker_init_kwargs: Dict = None,
    ):
        '''
        Consider a service that would be more efficient in batch (or vectorized)
        model. This class does two things:

            - Run this service in its own process;
            - Gather input elements in batches (of desired size) and send the batches
            to the service; once the service has returned the result for a batch,
            assign individual results to the individual input elements (which formed
            the batch) in the right order).

        This class also handles a special case: if user makes a request to the service
        with a bulk of elements, then this bulk (large or small) is sent to the service,
        skipping the batching process. The input to this bulk-request does not have
        to be a list of the elements that would be accepted by the element-request
        as long as the application itself can distinguish and handle accordingly.

        Args:
            max_batch_size: max count of individual intput elements
                to gather and process as a batch.
            timeout_seconds: wait-time before processing a partial batch.
            max_queue_size: max count of batches that can be in progress
                at the same time.
        '''
        assert 1 <= max_batch_size <= 10000
        # `max_batch_size = 1` will effectively
        # disable batching. Use it mainly for testing purposes.

        if timeout_seconds is None:
            timeout_seconds = 0.1
        else:
            assert 0 < timeout_seconds <= 1

        if max_queue_size is None:
            if max_batch_size <= 10:
                max_queue_size = 100
            else:
                max_queue_size = 32
        else:
            assert 1 <= max_queue_size <= 128

        self.max_batch_size = max_batch_size
        self.max_queue_size = max_queue_size
        self.timeout_seconds = timeout_seconds

        self._worker_class = worker_class
        self._worker_init_kwargs = worker_init_kwargs

        self._batch = [None for _ in range(max_batch_size)]
        self._batch_futures = [None for _ in range(max_batch_size)]
        self._batch_len = 0
        self._submit_batch_timer = None

        self._q_batches = asyncio.Queue(max_queue_size)

        # A queue storing Future objects for "in-progress" batches.
        # Each element is either a Future (for a "bulk"-submitted batch)
        # or a `List[Future]` (for a "batching"-submitted batch).
        self._future_results = asyncio.Queue()

        self._q_to_worker = mp.Queue(max_queue_size)
        self._q_from_worker = mp.Queue(max_queue_size)

        self._p_worker = None
        self._t_preprocessor = None
        self._t_postprocessor = None

        self._loop = None

    def start(self, loop=None):
        self._loop = loop or asyncio.get_event_loop()
        logger.info('Starting %s ...', self.__class__.__name__)

        p_worker = mp.Process(
            target=self._worker_class.start,
            kwargs={
                'q_in': self._q_to_worker,
                'q_out': self._q_from_worker,
                'init_kwargs': self._worker_init_kwargs,
            },
            name=f'{self._worker_class.__name__}-process',
        )
        p_worker.start()
        _ = self._q_from_worker.get()

        self._p_worker = p_worker
        self._t_preprocessor = self._loop.create_task(self._run_preprocess())
        self._t_postprocessor = self._loop.create_task(self._run_postprocess())
        logger.info('%s is ready to serve', self.__class__.__name__)

    def stop(self):
        logger.info('Stopping %s ...', self.__class__.__name__)
        if not self._t_preprocessor.done():
            self._t_preprocessor.cancel()
        if self._p_worker.is_alive():
            self._p_worker.terminate()
        if not self._t_postprocessor.done():
            self._t_postprocessor.cancel()
        logger.info('%s is stopped', self.__class__.__name__)

    def __del__(self):
        self.stop()

    async def preprocess(self, x):
        '''
        This performs transformation on a batch just before it is
        placed in the queue to be handled by the worker-process.
        This transformation happens in the current process.

        The single input parameter is either the data passed into
        `self.a_do_bulk` and submitted by `self._submit_bulk`,
        or a batch (a `list`) of the inputs passed into `self.a_do_one`
        and submitted by `self._submit_batch`.
        The user needs to know the input format of the two calls.

        The output of this method must be serializable, because
        it is going to be sent to the worker process.

        If you override this method, remember it must be `async`.        
        '''
        return x

    async def postprocess(self, x):
        '''
        This performs transformation on the result obtained from the queue,
        which has been placed in there by the worker process.
        This transformation happens in the current process.

        If you override this method, remember it must be `async`.
        '''
        return x

    async def _run_preprocess(self):
        while True:
            x = await self._q_batches.get()

            if x == NO_MORE_INPUT:
                while self._q_to_worker.full():
                    await asyncio.sleep(0.0015)
                self._q_to_worker.put(NO_MORE_INPUT)
                logger.info('preprocess: shutting down')
                return

            x, fut = x
            self._future_results.put_nowait(fut)

            try:
                x = await self.preprocess(x)
            except Exception as e:
                logger.exception(str(e))
                logger.info('passing SubProcessError to worker process to keep thingsin order')
                x = SubProcessError(e)

            while self._q_to_worker.full():
                await asyncio.sleep(0.0015)
            self._q_to_worker.put(x)

    async def _run_postprocess(self):
        while True:
            while self._q_from_worker.empty():
                await asyncio.sleep(0.0018)

            result = self._q_from_worker.get()
            await asyncio.sleep(0)

            if result == NO_MORE_OUTPUT:
                logger.debug('postprocess: shutting down')
                return

            if not isinstance(result, SubProcessError):
                try:
                    result = await self.postprocess(result)
                except Exception as e:
                    logger.exception(str(e))
                    result = e

            # Get the future 'box' that should receive this result.
            # The logic guarantees that things are in correct order.
            future = self._future_results.get_nowait()

            if isinstance(future, asyncio.Future):
                if not future.cancelled():
                    if isinstance(result, Exception):
                        future.set_exception(result)
                    else:
                        future.set_result(result)
                else:
                    logger.info('Future object is already cancelled')
            else:
                if isinstance(result, Exception):
                    for f in future:
                        if not f.cancelled():
                            f.set_exception(result)
                        else:
                            logger.info('Future object is already cancelled')
                else:
                    for f, r in zip(future, result):
                        if not f.cancelled():
                            f.set_result(r)
                        else:
                            logger.info('Future object is already cancelled')

            # The Future objects are referenced in the originating requests,
            # hence the `future` and `f` here are a second reference.
            # Going out of scope here will not destroy the object.
            # Once the waiting request has consumed the corresponding Future object
            # and returned, the Future object will be garbage collected.

    def _set_batch_submitter(self, wait_seconds=None):
        if self._submit_batch_timer is None:
            self._submit_batch_timer = self._loop.call_later(
                wait_seconds or self.timeout_seconds,
                self._submit_batch,
            )

    def _unset_batch_submitter(self):
        if self._submit_batch_timer is not None:
            self._submit_batch_timer.cancel()
            self._submit_batch_timer = None

    def _submit_batch(self):
        # This is a call-back function that is either called by `_submit_one`
        # or triggered by a timer.
        # To support the timer use case, it has to be a regular sync function.

        self._unset_batch_submitter()

        if self._q_batches.full():
            self._set_batch_submitter(self.timeout_seconds * 0.2)
            return

        x = self._batch[: self._batch_len]
        fut = self._batch_futures[: self._batch_len]
        self._q_batches.put_nowait((x, fut))

        for i in range(self._batch_len):
            self._batch[i] = None
            self._batch_futures[i] = None

        self._batch_len = 0

    async def _submit_one(self, x):
        while self._batch_len >= self.max_batch_size:
            # Basket is not ready to receive new request. Wait and retry.
            if self._submit_batch_timer is None:
                self._set_batch_submitter(self.timeout_seconds * 0.1)
            await asyncio.sleep(0.0012)

        # Put request in basket.
        # Create corresponding Future object.
        self._batch[self._batch_len] = x
        fut = self._loop.create_future()
        self._batch_futures[self._batch_len] = fut
        self._batch_len += 1

        if self._batch_len == self.max_batch_size:
            # Basket is full because of the last request we'e just put in.
            # `batching_max_batch_size` can be `1`, hence
            # this test should come before testing `self._batch_len == 1`.
            self._submit_batch()
        elif self._batch_len == 1:
            # Just starting a new batch (i.e. just placed the first request
            # in the basket).
            # Schedule `_submit_batch_timer` to trigger after certain time,
            # even if basket is not full at that time.
            # This scheduling returns right away.
            self._set_batch_submitter(self.timeout_seconds)
        else:
            # This request does not fill up the basket.
            # Do not trigger any further action.
            # At a later time, the basket will be processed
            # either when it is full, or when the wait-time expires.
            pass

        # When the basket is processed and results obtained,
        # the Future object of this request will contain its response.
        return fut

    async def _submit_bulk(self, x) -> asyncio.Future:
        # Create a Future object to receive the result,
        # and put it in the queue.
        fut = self._loop.create_future()

        # Send the bulk for preprocessing.
        # This skips the "batching" process, which non-bulk requests go through.
        await self._q_batches.put((x, fut))

        # Once the result is available, it will be put in this Future.
        return fut

    # `a_do_one` and `a_do_bulk` are API for end user.
    # Both methods may raise exceptions.
    # If user captures and ignores the exception, the service will continue.
    # If user stops at exception, they should stop the service by calling `stop`.

    async def a_do_one(self, x):
        fut = await self._submit_one(x)
        return await fut

    async def a_do_bulk(self, x):
        fut = await self._submit_bulk(x)
        return await fut
