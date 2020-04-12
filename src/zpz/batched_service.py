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
READY = 'I AM READY'


class VectorTransformer(metaclass=ABCMeta):
    # Subclasses should define `__init__` to receive
    # necessary info. Ideally, parameters to `__init__`
    # are simple, built-in types such as numbers and strings.

    def load(self):
        # Subclass may add named arguments to this method
        pass

    def preprocess(self, x: List) -> 'preprocessed':
        # This method runs in a subprocess.
        # it takes the data off of a queue and prepares it to
        # be consumed by `transform`.
        # This method should not take arguments other than `x`.
        #
        # By default, `x` is returned w/o change.
        return x

    @abstractmethod
    def transform(self, x: 'preprocessed') -> 'transformed':
        # This is a "vectorized" function
        raise NotImplementedError

    def terminate(self):
        pass

    def postprocess(self, pre: 'preprocessed', trans: 'transformed') -> 'postprocessed':
        # This method runs in a subprocess.
        # It takes the outputs of `preprocess` and `transform`
        # and returns something that will be sent back to the main process
        # in a queue.
        # The output of this method must be pickleable.
        #
        # In typical situations, this method only needs `trans`, but the output
        # of `preprocess` (i.e. the input to `transform`) is also passed in
        # just in case parts of it are needed in determining the return value.
        #
        # By default, `trans` is returned w/o change, and `pre` is ignored.
        return trans

    @classmethod
    def run(cls, *,
            q_in: mp.Queue,
            q_out: mp.Queue,
            init_kwargs: Dict = None,
            load_kwargs: Dict = None,
            ):
        # This method runs in a subprocess.
        # It creates an object of this class, waits on the input queue for tasks,
        # processes the tasks, and sends results back via the output queue.

        cls_name = cls.__name__
        transformer = cls(**(init_kwargs or {}))
        transformer.load(**(load_kwargs or {}))
        q_out.put(READY)

        while True:
            x = q_in.get()
            if x == NO_MORE_INPUT:
                logger.debug(f'{cls_name}: shutting down')
                transformer.terminate()
                return

            if isinstance(x, SubProcessError):
                logger.info('received SubProcessError from the main process; sending it back right away')
                w = x
            else:
                try:
                    y = transformer.preprocess(x)
                    z = transformer.transform(y)
                    w = transformer.postprocess(y, z)
                except Exception as e:
                    logger.exception(str(e))
                    logger.info('sending SubProcessError back to the main process')
                    w = SubProcessError(e)

            q_out.put(w)


class BatchedService:
    def __init__(
        self,
        batching_max_batch_size: int,
        worker_class: Type[VectorTransformer],
        *,
        batching_timeout_seconds: float = None,
        batching_max_queue_size: int = None,
        worker_init_kwargs: Dict = None,
        worker_load_kwargs: Dict = None,
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
            batching_max_batch_size: max count of individual intput elements
                to gather and process as a batch.
            batching_timeout_seconds: wait-time before processing a partial batch.
            batching_max_queue_size: max count of batches that can be in progress
                at the same time.
        '''
        assert 1 <= batching_max_batch_size <= 10000
        # `batching_max_batch_size = 1` will effectively
        # disable batching. Use it mainly for testing purposes.

        if batching_timeout_seconds is None:
            batching_timeout_seconds = 0.1
        else:
            assert 0 < batching_timeout_seconds <= 1

        if batching_max_queue_size is None:
            if batching_max_batch_size <= 10:
                batching_max_queue_size = 100
            else:
                batching_max_queue_size = 32
        else:
            assert 1 <= batching_max_queue_size <= 128

        self.batching_max_batch_size = batching_max_batch_size
        self.batching_max_queue_size = batching_max_queue_size
        self.batching_timeout_seconds = batching_timeout_seconds

        self._worker_class = worker_class
        self._worker_init_kwargs = worker_init_kwargs
        self._worker_load_kwargs = worker_load_kwargs

        self._batch = [None for _ in range(batching_max_batch_size)]
        self._batch_futures = [None for _ in range(batching_max_batch_size)]
        self._batch_len = 0
        self._submit_batch_timer = None
        
        self._q_batches = asyncio.Queue(batching_max_queue_size)

        # A queue storing Future objects for "in-progress" batches.
        # Each element is either a Future (for a "bulk"-submitted batch)
        # or a `List[Future]` (for a "batching"-submitted batch).
        self._future_results = asyncio.Queue()

        self._q_to_worker = mp.Queue(batching_max_queue_size)
        self._q_from_worker = mp.Queue(batching_max_queue_size)

        self._p_worker = None
        self._t_preprocessor = None
        self._t_postprocessor = None

        self._loop = asyncio.get_event_loop()

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
                logger.debug('preprocess: shutting down')
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
            self._q_to_worker.put_nowait(x)

    async def _run_postprocess(self):
        while True:
            while self._q_from_worker.empty():
                await asyncio.sleep(0.0018)

            result = self._q_from_worker.get_nowait()

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
                future.set_result(result)
            else:
                if isinstance(result, Exception):
                    for f in future:
                        f.set_result(result)
                else:
                    for f, r in zip(future, result):
                        f.set_result(r)

            # The Future objects are referenced in the originating requests,
            # hence the `future` and `f` here are a second reference.
            # Going out of scope here will not destroy the object.
            # Once the waiting request has consumed the corresponding Future object
            # and returned, the Future object will be garbage collected.
            await asyncio.sleep(0.0012)

    def start(self):
        logger.info('Starting %s ...', self.__class__.__name__)
        p_worker = mp.Process(
            target=self._worker_class.run,
            kwargs={
                'q_in': self._q_to_worker,
                'q_out': self._q_from_worker,
                'init_kwargs': self._worker_init_kwargs,
                'load_kwargs': self._worker_load_kwargs,
            },
            name=f'{self._worker_class.__name__}-process',
        )
        p_worker.start()
        z = self._q_from_worker.get()
        assert z == READY

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

    def _submit_batch(self):
        # This is a call-back function that is either called by `_submit_one`
        # or triggered by a timer.
        # To support the timer use case, it has to be a regular sync function.

        if self._submit_batch_timer is not None:
            self._submit_batch_timer.cancel()
            self._submit_batch_timer = None

        if self._q_batches.full():
            self._submit_batch_timer = self._loop.call_later(
                0.0089,
                self._submit_batch,
            )
            return

        x = self._batch[: self._batch_len]
        fut = self._batch_futures[: self._batch_len]
        self._q_batches.put_nowait((x, fut))

        for i in range(self._batch_len):
            self._batch[i] = None
            self._batch_futures[i] = None

        self._batch_len = 0

    async def _submit_one(self, x):
        while self._batch_len >= self.batching_max_batch_size:
            # Basket is not ready to receive new request. Wait and retry.
            await asyncio.sleep(0.0012)

        # Put request in basket.
        # Create corresponding Future object.
        self._batch[self._batch_len] = x
        fut = self._loop.create_future()
        self._batch_futures[self._batch_len] = fut
        self._batch_len += 1

        if self._batch_len == self.batching_max_batch_size:
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
            self._submit_batch_timer = self._loop.call_later(
                self.batching_timeout_seconds,
                self._submit_batch
            )
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
        while True:
            if not self._q_batches.full():
                self._q_batches.put_nowait((x, fut))
                break
            await asyncio.sleep(0.0024)

        # Once the result is available, it will be put in this Future.
        return fut

    # `a_do_one` and `a_do_bulk` are API for end user.
    # Both methods may raise exceptions.
    # If user captures and ignores the exception, the service will continue.
    # If user stops at exception, they should stop the service by calling `stop`.

    async def a_do_one(self, x):
        fut = await self._submit_one(x)
        z = await fut
        if isinstance(z, Exception):
            raise z
        return z

    async def a_do_bulk(self, x):
        fut = await self._submit_bulk(x)
        z = await fut
        if isinstance(z, Exception):
            raise z
        return z
