import asyncio
import logging
from typing import Callable, Awaitable

from .mp import MAX_THREADS

logger = logging.getLogger(__name__)


async def concurrent_gather(*tasks, max_workers=None, return_exceptions=False):
    if max_workers is None:
        max_workers = MAX_THREADS
        # This default is suitable for I/O bound operations.
        # For others, user may need to customize this value.
    semaphore = asyncio.Semaphore(max_workers)

    async def sem_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(
        *(sem_task(task) for task in tasks),
        return_exceptions=return_exceptions,
    )


def use_uvloop():
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


class MaybeAwait:
    '''
    Keep the call context of a coroutine w/o calling it.
    If need to call, await this object.
    If never awaited, no Awaitable will be created, hence
    won't have the "coroutine ... was never awaited" warning.

    Example:

        async def myfunc(x, y, opts):
            ...

        zz = MaybeAwait(myfunc, 3, 4, opts=8)
        ...
        if need_to_call:
            result = await zz
    '''

    def __init__(self, func: Callable[..., Awaitable], *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __await__(self):
        return self.func(*self.args, **self.kwargs).__await__()
