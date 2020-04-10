import json
import os
import os.path
from typing import Any

from anyio import aopen

from .path import prepare_path

# TODO
# Check out `orjson`, which claims to be fast.


def json_dumps(x: Any) -> str:
    return json.dumps(x)


def json_loads(x: str) -> Any:
    return json.loads(x)


def json_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    with open(ff, 'w') as f:
        json.dump(x, f)


def json_load(path: str, *path_elements) -> Any:
    with open(os.path.join(path, *path_elements), 'r') as f:
        return json.load(f)


async def a_json_dump(x: Any, path: str, *path_elements) -> None:
    ff = prepare_path(path, *path_elements)
    async with await aopen(ff, 'w') as f:
        await f.write(json_dumps(x))


async def a_json_load(path: str, *path_elements) -> Any:
    async with await aopen(os.path.join(path, *path_elements), 'r') as f:
        return json_loads(await f.read())
