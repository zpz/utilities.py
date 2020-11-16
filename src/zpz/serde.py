import io
import json
import marshal
import os
import os.path
import pickle
import zlib

import joblib
import orjson

from .path import prepare_path
from ._functools import nogc


def _dump(func, mode, x, path, *paths) -> None:
    ff = prepare_path(path, *paths)
    with open(ff, mode) as f:
        f.write(func(x))


def _load(func, mode, path, *paths):
    with open(os.path.join(path, *paths), mode) as f:
        return func(f.read())


json_dumps = json.dumps


@nogc
def json_loads(x):
    return json.loads(x)


def json_dump(x, path: str, *path_elements) -> None:
    _dump(json_dumps, 'w', x, path, *path_elements)


def json_load(path: str, *path_elements):
    return _load(json_loads, 'r', path, *path_elements)


orjson_dumps = orjson.dumps


@nogc
def orjson_loads(x):
    return orjson.loads(x)


def orjson_dump(x, path, *path_elements):
    _dump(orjson_dumps, 'wb', x, path, *path_elements)


def orjson_load(path, *path_elements):
    return _load(orjson_loads, 'rb', path, *path_elements)


def orjson_z_dumps(x):
    return zlib.compress(orjson_dumps(x), level=3)


def orjson_z_loads(x):
    return orjson_loads(zlib.decompress(x))


def orjson_z_dump(x, path, *path_elements):
    _dump(orjson_z_dumps, 'wb', x, path, *path_elements)


def orjson_z_load(path, *path_elements):
    _load(orjson_z_loads, 'rb', path, *path_elements)


def pickle_dumps(x) -> bytes:
    return pickle.dumps(x, protocol=pickle.HIGHEST_PROTOCOL)


@nogc
def pickle_loads(x: bytes):
    return pickle.loads(x)


def pickle_dump(x, path: str, *path_elements):
    _dump(pickle_dumps, 'wb', x, path, *path_elements)


def pickle_load(path: str, *path_elements):
    return _load(pickle_loads, 'rb', path, *path_elements)


def pickle_z_dumps(x) -> bytes:
    return zlib.compress(pickle_dumps(x), level=3)


def pickle_z_loads(x: bytes):
    return pickle_loads(zlib.decompress(x))


def pickle_z_dump(x, path: str, *path_elements) -> None:
    _dump(pickle_z_dumps, 'wb', x, path, *path_elements)


def pickle_z_load(path: str, *path_elements):
    return _load(pickle_z_loads, 'rb', path, *path_elements)


marshal_dumps = marshal.dumps


@nogc
def marshal_loads(x):
    return marshal.loads(x)


def marshal_dump(x, path: str, *path_elements):
    _dump(marshal_dumps, 'wb', x, path, *path_elements)


def marshal_load(path: str, *path_elements):
    return _load(marshal_loads, 'rb', path, *path_elements)


def dump_bytes(x, compress: int = 9) -> bytes:
    """
    Serialize Python object (e.g. fitted model) `x` into a binary blob.
    """
    o = io.BytesIO()
    joblib.dump(x, o, compress=compress)
    return o.getvalue()


def load_bytes(b: bytes):
    """
    Inverse of `dump_bytes`.
    """
    return joblib.load(io.BytesIO(b))


def dump_file(x, filename: str, overwrite: bool = False,
              compress: int = 9) -> None:
    """
    Persist Python object (e.g. fitted model) `x` into disk file `filename`.
    """
    with open(filename, 'wb' if overwrite else 'xb') as f:
        f.write(dump_bytes(x, compress=compress))


def load_file(filename: str):
    """
    Inverse of `dump`.
    """
    with open(filename, 'rb') as f:
        z = f.read()
    return load_bytes(z)
