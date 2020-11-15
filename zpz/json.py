import json
import os
import os.path
import zlib
from typing import Any

import orjson

from .path import prepare_path
from ._functools import nogc


# NOTE:
# async file I/O is *slower* than sync.
# Refer to:
#   https://github.com/mosquito/aiofile/issues/18


json_dumps = json.dumps


@nogc
def json_loads(x):
    return json.loads(x)


def json_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    with open(ff, 'w') as f:
        f.write(json_dumps(x))


def json_load(path: str, *path_elements) -> Any:
    with open(os.path.join(path, *path_elements), 'r') as f:
        return json_loads(f.read())


orjson_dumps = orjson.dumps


@nogc
def orjson_loads(x):
    return orjson.loads(x)


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
