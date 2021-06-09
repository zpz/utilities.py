import multiprocessing

# Using `concurrent.futures.ProcessPoolExecutor`
# or `asyncio.get_event_loop().run_in_executor`
# will handle remote exceptions properly, hence
# using those in place of a raw Process is recommended
# when possible.


MAX_THREADS = min(32, multiprocessing.cpu_count() + 4)
# This default is suitable for I/O bound operations.
# For others, user may want to specify a smaller value.
