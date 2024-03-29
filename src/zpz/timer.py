import sys
import time
from contextlib import contextmanager
from functools import wraps
from typing import Callable, List


def humanize(seconds: float) -> List[str]:
    msg = []
    if seconds >= 3600:
        hours, seconds = divmod(seconds, 3600)
        hours = int(hours)
        if hours < 2:
            msg.append(f"{hours} hour")
        else:
            msg.append(f"{hours} hours")
    if seconds >= 60:
        minutes, seconds = divmod(seconds, 60)
        minutes = int(minutes)
        if minutes < 2:
            msg.append(f"{minutes} minute")
        else:
            msg.append(f"{minutes} minutes")
    msg.append(f"{round(seconds, 4)} seconds")
    return msg


def timed(print_func: Callable = None) -> Callable:
    """
    Usage 1:

        @timed()
        def myfunc(...):
            ...

    Usage 2:

        import logging
        logger = logging.getLogger(__name__)

        @timed(logger.info)
        def myfunc(...):
            ...
    """
    if print_func is None:

        def print_func(text):
            print(text, file=sys.stderr)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def profiled_func(*args, **kwargs):
            print_func(f"Starting function `{func.__name__}`")
            t0 = time.monotonic()
            result = func(*args, **kwargs)
            duration = ", ".join(humanize(time.monotonic() - t0))
            print_func(f"Finishing function `{func.__name__}`")
            print_func(f"Function `{func.__name__}` took {duration} to finish")
            return result

        return profiled_func

    return decorator


def timed_call(func, *args, **kwargs):
    t0 = time.monotonic()
    z = func(*args, **kwargs)
    seconds = time.monotonic() - t0
    return z, seconds


@contextmanager
def timer(msg: str):
    """
    Time a code block:

        with timer('my block'):
            x = 3
            y = 4
            ...
    """
    t0 = time.monotonic()
    yield
    t1 = time.monotonic()
    print(f"{msg}: {humanize(t1 - t0)}")
