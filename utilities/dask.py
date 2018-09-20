import os
import os.path

from distributed import Client as Client


class Dask(Client):
    def __init__(self) -> None:
        scheduler = os.environ['DASK_SCHEDULER_URL']
        super().__init__(scheduler)


class LocalDask(Client):
    def __init__(self, *args, local_dir=None, **kwargs) -> None:
        local_dir = local_dir or os.path.join(os.environ['DATADIR'], 'dask-worker-space')
        super().__init__(*args, **kwargs, local_dir=local_dir)