from functools import wraps
import time
from typing import Callable


def timed(func: Callable) -> Callable:
    @wraps(func)
    def profiled_func(*args, **kwargs):
        time0 = time.perf_counter()
        result = func(*args, **kwargs)
        time1 = time.perf_counter()

        print('')
        print('Function `', func.__name__, '` took ', time1 - time0,
              'seconds to finish')
        print('')
        return result

    return profiled_func


class Timer:
    def __init__(self):
        self._running = False

    def start(self):
        self._t_start = time.perf_counter()
        self._t_stop = None
        self._running = True
        return self

    def stop(self):
        if self._running:
            self._t_stop = time.perf_counter()
            self._running = False
        return self

    @property
    def seconds(self):
        if self._running:
            return time.perf_counter() - self._t_start
        return self._t_stop - self._t_start

    @property
    def milliseconds(self):
        return self.seconds * 1000
