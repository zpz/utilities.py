from io import BytesIO
import json
from typing import Union, Any

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def _make_schema(name, x) -> Union[str, dict]:
    assert isinstance(name, str)

    if isinstance(x, int):
        return {'name': name, 'type': "int"}
    if isinstance(x, float):
        return {'name': name, 'type': 'double'}
    if isinstance(x, str):
        return {'name': name, 'type': 'string'}
    if isinstance(x, dict):
        fields = []
        for key, val in x.items():
            z = _make_schema(key, val)
            if len(z) < 3:
                fields.append(z)
            else:
                fields.append({'name': key, 'type': z})
        return {'name': name, 'type': 'record', 'fields': fields}
    if isinstance(x, list):
        assert len(x) > 0
        z0 = _make_schema(name, x[0])
        if len(x) > 1:
            for v in x[1:]:
                assert _make_schema(name, v) == z0
        if len(z0) < 3:
            items = z0['type']
        else:
            items = z0
        return {'name': name, 'type': 'array', 'items': items}

    raise Exception('unrecognized value of type "' + type(x).__name__ + '"')


def make_schema(name: str, value: Any, namespace: str = None) -> str:
    """
    Construct avro schema for `value` based on type inspections.

    `value` is a pure Python object with simple data structures including
    scalar, list, and dict. Some nesting is allowed.
    """
    sch = {'namespace': namespace, **_make_schema(name, value)}
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
    '''
    The use case is that the stream contains only one record.
    '''
    with DataFileReader(BytesIO(b), DatumReader()) as reader:
        value = list(reader)
        if len(value) == 1:
            value = value[0]
        if return_schema:
            schema = reader.meta['avro.schema'].decode()
            return value, schema
        return value
