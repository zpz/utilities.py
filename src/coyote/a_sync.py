import asyncio
import logging
import multiprocessing
import traceback

logger = logging.getLogger(__name__)


IO_WORKERS = min(32, multiprocessing.cpu_count() + 4)


def create_loud_task(*args, **kwargs):
    def done_callback(t):
        if t.cancelled():
            logger.debug('task was cancelled')
        elif t.exception():
            e = t.exception()
            logger.warning(
                'task raised exception: %s\n%s',
                e, '\n'.join(traceback.format_tb(e.__traceback__))
            )
            raise e
        else:
            pass

    task = asyncio.create_task(*args, **kwargs)
    task.add_done_callback(done_callback)
    return task


async def concurrent_gather(*tasks, max_workers=None, return_exceptions=False):
    if max_workers is None:
        max_workers = IO_WORKERS
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
