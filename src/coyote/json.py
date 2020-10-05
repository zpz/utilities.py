import json
import os
import os.path
import zlib
from typing import Any

import orjson
from anyio import open_file

from .path import prepare_path


# NOTE:
# async file I/O is *slower* than sync.
# Refer to:
#   https://github.com/mosquito/aiofile/issues/18


json_dumps = json.dumps
json_loads = json.loads


def json_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    with open(ff, 'w') as f:
        json.dump(x, f)


def json_load(path: str, *path_elements) -> Any:
    with open(os.path.join(path, *path_elements), 'r') as f:
        return json.load(f)


async def a_json_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    async with await open_file(ff, 'w') as f:
        await f.write(json_dumps(x))


async def a_json_load(path: str, *path_elements) -> Any:
    async with await open_file(os.path.join(path, *path_elements), 'r') as f:
        return json_loads(await f.read())


orjson_dumps = orjson.dumps
orjson_loads = orjson.loads


def orjson_z_dumps(x):
    return zlib.compress(orjson_dumps(x), level=3)


def orjson_z_loads(x):
    return orjson_loads(zlib.decompress(x))


def orjson_dump(x, path, *path_elements):
    ff = prepare_path(path, *path_elements)
    with open(ff, 'wb') as file:
        file.write(orjson_dumps(x))


def orjson_load(path, *path_elements):
    with open(os.path.join(path, *path_elements), 'rb') as file:
        return orjson_loads(file.read())


def orjson_z_dump(x, path, *path_elements):
    ff = prepare_path(path, *path_elements)
    with open(ff, 'wb') as file:
        file.write(orjson_z_dumps(x))


def orjson_z_load(path, *path_elements):
    with open(os.path.join(path, *path_elements), 'rb') as file:
        return orjson_z_loads(file.read())


async def a_orjson_dump(x, path, *path_elements):
    ff = prepare_path(path, *path_elements)
    async with await open_file(ff, 'wb') as file:
        await file.write(orjson_dumps(x))


async def a_orjson_load(path, *path_elements):
    async with await open_file(os.path.join(path, *path_elements), 'rb') as file:
        return orjson_loads(file.read())


async def a_orjson_z_dump(x, path, *path_elements):
    ff = prepare_path(path, *path_elements)
    async with await open_file(ff, 'wb') as file:
        await file.write(orjson_z_dumps(x))


async def a_orjson_z_load(path, *path_elements):
    async with await open_file(os.path.join(path, *path_elements), 'rb') as file:
        return orjson_z_loads(file.read())
