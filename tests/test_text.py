import json

import numpy as np
import pandas as pd

from utilities.text import numpy_to_serializable, numpy_from_serializable


def test_pretty():
    df1 = pd.DataFrame({
        'col1': [1, 2, 3],
        'col2': [1.8, 2.5123, 3.798134],
        'col3': ['ab', 'cdf', 'pi'],
    })
    y = np.array([1, 2.5, 3])
    z = np.array(range(24)).reshape(2, 3, 4)
    df2 = pd.DataFrame(
        {
            'a': [21, 2, 3, 4],
            'b': ['a1', 'b1', 'c1', 'd1'],
            'c': [12, 9.5, 4, 300],
        },
        columns=('a', 'b', 'c'),
    )
    data = [
        df2,
        'abcd efg',
        df1,
        y,
        z,
        [df1, ('y', y, dict(numpy=z))],
        [3, 'ab', {
            'first': 'yes',
            'second': [6, 8]
        }],
        [np.int64(23), np.float64(2.8)],
        [(2.8, 'name', 96), [35], df2, {
            'new': df1
        }],
        {
            'p': 38,
            'q': df1,
            's': [5, 7]
        },
    ]

    for v in data:
        original = v
        serializable = numpy_to_serializable(original)
        from_serializable = numpy_from_serializable(serializable)
        to_json = json.dumps(serializable)
        from_json = numpy_from_serializable(json.loads(to_json))

        print('')
        print('original:')
        print(original)
        print('')
        print('serializable:')
        print(serializable)
        print('')
        print('from serializable:')
        print(from_serializable)
        print('')
        print('to json:')
        print(to_json)
        print('')
        print('from json:')
        print(from_json)
        print('')
