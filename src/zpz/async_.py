import asyncio
import concurrent.futures
import functools
import logging

logger = logging.getLogger(__name__)


async def async_call(
    func, *args, executor: concurrent.futures.Executor = None, **kwargs
):
    loop = asyncio.get_running_loop()
    if kwargs:
        func = functools.partial(func, **kwargs)

    return await loop.run_in_executor(executor, func, *args)
