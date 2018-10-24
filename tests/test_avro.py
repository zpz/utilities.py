import json
import numpy as np

from zpz.avro import dump_bytes, load_bytes, make_schema


def test_avro():
    data = {
        'scalars': {
            'a': 3,
            'b': 38.9,
            'c': {
                'first': [1, 3, 5],
                'second': 'this is a string'
            }
        },
        'np_scalars': {
            'int8': np.int8(8),
            'int16': np.int16(16),
            'int32': np.int32(32),
            'int64': np.int64(64),
            'uint8': np.uint8(8),
            'uint16': np.uint16(16),
            'uint32': np.uint32(32),
            'uint64': np.uint64(64),
            'float32': np.float32(3.1415),
            'double64': np.float64(2.71828)
        },
        'np_arrays': {
            'int8': np.array([1, -3, 5], np.int8),
            'int16': np.array([9, -7, 10], np.int16),
            'int32': np.array([-17, 19], np.int32),
            'int64': np.array([88, -75], np.int64),
            'uint8': np.array([1, 3, 5], np.uint8),
            'uint16': np.array([9, 7, 10], np.uint16),
            'uint32': np.array([17, 19], np.uint32),
            'uint64': np.array([88, 75], np.uint64),
            'float32': np.array([2.7, 5.2], np.float32),
            'float64': np.array([7.7, 9.2], np.float64)
        }
    }

    # print(json.dumps(json.loads(make_schema(data, 'test'), indent=2))
    b = dump_bytes(data, 'test', 'test')
    z = load_bytes(b)

    for item in data['scalars']:
        assert type(data['scalars'][item]) is type(z['scalars'][item])
        assert data['scalars'][item] == z['scalars'][item]

    for item in data['np_scalars']:
        assert type(data['np_scalars'][item]) is type(z['np_scalars'][item])
        assert data['np_scalars'][item] == z['np_scalars'][item]

    for item in data['np_arrays']:
        assert type(z['np_arrays'][item]) is type(data['np_arrays'][item])
        assert all(data['np_arrays'][item] == z['np_arrays'][item])
