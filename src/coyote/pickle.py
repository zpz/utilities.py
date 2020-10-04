import os
import os.path
import pickle
import zlib
from typing import Any

from anyio import aopen

from .path import prepare_path


def pickle_dumps(x: Any) -> bytes:
    return pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)


def pickle_loads(x: bytes) -> Any:
    return pickle.loads(x)


def pickle_z_dumps(x: Any) -> bytes:
    return zlib.compress(pickle_dumps(x))


def pickle_z_loads(x: bytes) -> Any:
    return pickle_loads(zlib.decompress(x))


def pickle_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    with open(ff, 'wb') as f:
        pickle.dump(x, f, protocol=pickle.HIGHEST_PROTOCOL)


def pickle_load(path: str, *path_elements) -> Any:
    with open(os.path.join(path, *path_elements), 'rb') as f:
        return pickle.load(f)


def pickle_z_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    with open(ff, 'wb') as f:
        f.write(pickle_z_dumps(x))


def pickle_z_load(path: str, *path_elements) -> Any:
    with open(os.path.join(path, *path_elements), 'rb') as f:
        return pickle_z_loads(f.read())


async def a_pickle_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    async with await aopen(ff, 'wb') as f:
        await f.write(pickle_dumps(x))


async def a_pickle_load(path: str, *path_elements) -> Any:
    async with await aopen(os.path.join(path, *path_elements), 'rb') as f:
        return pickle_loads(await f.read())


async def a_pickle_z_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    async with await aopen(ff, 'wb') as f:
        await f.write(pickle_z_dumps(x))


async def a_pickle_z_load(path: str, *path_elements) -> Any:
    async with await aopen(os.path.join(path, *path_elements), 'rb') as f:
        return pickle_z_loads(await f.read())
