from io import BytesIO
import json
from typing import Union, Any

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def _make_schema(x) -> Union[str, dict]:
    if isinstance(x, int):
        return "int"
    if isinstance(x, float):
        return 'double'
    if isinstance(x, str):
        return 'string'
    if isinstance(x, dict):
        z = {'type': 'record'}
        fields = []
        for key, val in x.items():
            assert isinstance(key, str)
            zz = _make_schema(val)
            if not isinstance(zz, str):
                assert isinstance(zz, dict)
                zz['name'] = key
            fields.append({'name': key, 'type': zz})
        z['fields'] = fields
        return z
    if isinstance(x, list):
        assert x
        z0 = _make_schema(x[0])
        if len(x) > 1:
            for v in x[1:]:
                assert _make_schema(v) == z0
        return {'type': 'array', 'items': z0}

    raise Exception('unrecognized value of type "' + type(x).__name__ + '"')


def make_schema(name: str, value: Any, namespace: str = None) -> str:
    """
    Construct avro schema for `value` based on type inspections.

    `value` is a pure Python object with simple data structures including
    scalar, list, and dict. Some nesting is allowed.
    """
    sch = {'name': name}
    if namespace:
        sch['namespace'] = namespace
    z = _make_schema(value)
    if isinstance(z, str):
        sch['type'] = z
    else:
        sch = {**sch, **z}
    return json.dumps(sch)


def dump_bytes(schema: str, value: Any) -> bytes:
    stream = BytesIO()
    with DataFileWriter(stream, DatumWriter(),
                        avro.schema.Parse(schema)) as writer:
        writer.append(value)
        writer.flush()
        stream.seek(0)
        return stream.getvalue()


def load_bytes(b: bytes, return_schema: bool = False):
    with DataFileReader(BytesIO(b), DatumReader()) as reader:
        value = list(reader)
        if len(value) == 1:
            value = value[0]
        if return_schema:
            schema = reader.meta['avro.schema'].decode()
            return value, schema
        return value
