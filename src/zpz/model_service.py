
'''
Although named after machine learning use cases,
this service utility is generic.
'''

import asyncio
import logging
import multiprocessing as mp
import queue
from abc import ABCMeta, abstractmethod
from typing import List, Type, Union

import psutil

from .mp import SubProcessError


logger = logging.getLogger(__name__)


class Modelet(metaclass=ABCMeta):
    # Typically a subclass needs to enhance
    # `__init__` and implement `predict`.
    def __init__(self, *, batch_size: int = None, batch_wait_time: float = None):
        # `batch_wait_time`: seconds, may be 0.
        self.batch_size = batch_size or 0
        self.batch_wait_time = 0 if batch_wait_time is None else batch_wait_time

    def __start_single(self, *, q_in, q_out, q_err):
        batch_size = self.batch_size
        while True:
            uid, x = q_in.get()
            try:
                if batch_size:
                    y = self.predict([x])[0]
                else:
                    y = self.predict(x)
                q_out.put((uid, y))

            except Exception as e:
                logger.info(e)
                # There are opportunities to print traceback
                # and details later using the `SubProcessError`
                # object. Be brief on the logging here.
                err = SubProcessError(e)
                q_err.put((uid, err))

    def _start_batch(self, *, q_in, q_out, q_err):
        batch_size = self.batch_size
        batch_wait_time = self.batch_wait_time
        while True:
            batch = []
            uids = []
            n = 0
            if batch_wait_time == 0:
                uid, x = q_in.get()
                batch.append(x)
                uids.append(uid)
                n += 1
                while n < batch_size:
                    try:
                        uid, x = q_in.get_nowait()
                        batch.append(x)
                        uids.append(x)
                        n += 1
                    except queue.Empty:
                        break
            else:
                try:
                    uid, x = q_in.get(timeout=batch_wait_time)
                    batch.append(x)
                    uids.append(uid)
                    n += 1
                except queue.Empty:
                    uid, x = q_in.get()
                    batch.append(x)
                    uids.append(uid)
                    n += 1
                    while n < batch_size:
                        try:
                            uid, x = q_in.get_nowait()
                            batch.append(x)
                            uids.append(uid)
                            n += 1
                        except queue.Empty:
                            break
                else:
                    while n < batch_size:
                        try:
                            uid, x = q_in.get(timeout=batch_wait_time)
                            batch.append(x)
                            uids.append(x)
                            n += 1
                        except queue.Empty:
                            break

            try:
                results = self.predict(batch)
            except Exception as e:
                logger.info(e)
                err = SubProcessError(e)
                for uid in uids:
                    q_err.put((uid, err))
            else:
                for uid, y in zip(uids, results):
                    q_out.put((uid, y))

    def start(self, *, q_in: mp.Queue, q_out: mp.Queue, q_err: mp.Queue):
        process_name = f'{self.__class__.__name__}--{mp.current_process().name}'
        logger.info('%s started', process_name)
        if self.batch_size > 1:
            self._start_batch(q_in=q_in, q_out=q_out, q_err=q_err)
        else:
            self.__start_single(q_in=q_in, q_out=q_out, q_err=q_err)

    @abstractmethod
    def predict(self, x):
        # `x`: a single element if `self.batch_size == 0`;
        # else, a list of elements.
        # When `batch_size == 0`, hence `x` is a single element,
        # return corresponding result.
        # When `batch_size > 0`, return list of results
        # corresponding to elements in `x`.
        raise NotImplementedError

    @classmethod
    def run(cls, *,
            q_in: mp.Queue, q_out: mp.Queue, q_err: mp.Queue,
            cpus: List[int] = None, **init_kwargs):
        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)
        modelet = cls(**init_kwargs)
        modelet.start(q_in=q_in, q_out=q_out, q_err=q_err)


class ModelService:
    def __init__(self,
                 max_queue_size: int = None,
                 cpus: List[int] = None):
        self.max_queue_size = max_queue_size or 1024
        self._q_in_out = [mp.Queue(self.max_queue_size)]
        self._q_err = mp.Queue(self.max_queue_size)

        self._uid_to_futures = {}
        self._t_gather_results = None
        self._modelets = []

        if cpus:
            psutil.Process().cpu_affinity(cpus=cpus)

        self._started = False

    def add_modelet(self,
                    modelet: Type[Modelet],
                    *,
                    cpus: Union[int, List[int]] = None,
                    **init_kwargs):
        # `modelet` is the class object, not instance.
        q_in = self._q_in_out[-1]
        q_out = mp.Queue(self.max_queue_size)
        self._q_in_out.append(q_out)
        if cpus is None:
            cpus = [None]
        elif isinstance(cpus, int):
            cpus = [None for _ in range(cpus)]
        else:
            assert isinstance(cpus, (list, tuple))
        n_cpus = psutil.cpu_count(logical=True)

        for cpu in cpus:
            if cpu is None:
                logger.info('adding modelet %s', modelet.__name__)
                cc = None
            else:
                assert 0 <= cpu < n_cpus
                logger.info('adding modelet %s at CPU %d', modelet.__name__, cpu)
                cc = [cpu]
            self._modelets.append(
                mp.Process(
                    target=modelet.run,
                    name=f'modelet-{cpu}',
                    kwargs={
                        'q_in': q_in,
                        'q_out': q_out, 
                        'q_err': self._q_err,
                        'cpus': cc,
                        **init_kwargs,
                    },
                )
            )

    def start(self):
        assert self._modelets
        assert not self._started
        for m in self._modelets:
            m.start()
        self._t_gather_results = asyncio.create_task(self._gather_results())
        self._started = True

    def stop(self):
        if not self._started:
            return
        if self._t_gather_results is not None and not self._t_gather_results.done():
            self._t_gather_results.cancel()
        for m in self._modelets:
            m.terminate()
            m.join()
        self._started = False

    def __del__(self):
        self.stop()

    async def _gather_results(self):
        q_out = self._q_in_out[-1]
        q_err = self._q_err
        futures = self._uid_to_futures
        while True:
            if not q_out.empty():
                uid, y = q_out.get()
                fut = futures.pop(uid)
                if not fut.cancelled():
                    fut.set_result(y)
                else:
                    logger.warning('Future object is already cancelled')
                continue

            if not q_err.empty():
                uid, err = q_err.get()
                fut = futures.pop(uid)
                fut.set_exception(err)
                continue

            await asyncio.sleep(0.0089)

    async def a_predict(self, x):
        fut = asyncio.get_running_loop().create_future()
        uid = id(fut)
        self._uid_to_futures[uid] = fut
        q_in = self._q_in_out[0]
        while q_in.full():
            await asyncio.sleep(0.0078)
        while True:
            try:
                q_in.put_nowait((uid, x))
                break
            except queue.Full:
                await asyncio.sleep(0.0078)
        return await fut

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, ex_value, ex_traceback):
        self.stop()
