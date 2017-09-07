from collections import OrderedDict

import numpy as np
import pandas as pd


def round_floats(obj, ndigits=2):
    """
    Round floats to specified digits, while
    leaving the data structure and other data types intact.

    The intended use case is for pretty-printing.
    """
    if isinstance(obj, float):
        return round(obj, ndigits)
    if isinstance(obj, dict):
        return dict((k, round_floats(v, ndigits)) for k, v in obj.items())
    if isinstance(obj, (list, tuple)):
        return type(obj)(map(lambda x: round_floats(x, ndigits), obj))
    if isinstance(obj, np.ndarray):
        if issubclass(obj.dtype.type, np.floatting):
            return obj.round(ndigits)
        else:
            return obj
    if isinstance(obj, pd.Series):
        return obj.map(lambda x: round_floats(x, ndigits))
    if isinstance(obj, pd.DataFrame):
        return obj.applymap(lambda x: round_floats(x, ndigits))
    return obj


def numpy_to_serializable(obj):
    """
    Transform Numpy and pandas components of a data object so that
    the entire object is json-serializable.
    """
    if isinstance(obj, pd.DataFrame):
        index = obj.index.tolist()
        columns = OrderedDict((k, obj[k].values.tolist()) for k in obj)
        return {'pandas.DataFrame': {'index': index, 'columns': columns}}
    if isinstance(obj, pd.Series):
        index = obj.index.tolist()
        values = obj.values.tolist()
        return {'pandas.Series': {'index': index, 'values': values}}
    if isinstance(obj, np.ndarray):
        return {'numpy.ndarray': obj.tolist()}
    if isinstance(obj, (np.integer, np.floating)):
        return {'numpy.scalar': {'type': obj.dtype.type.__name__, 'value': np.asscalar(obj)}}
    if isinstance(obj, dict):
        return type(obj)((k, numpy_to_serializable(v)) for k, v in obj.items())
    if isinstance(obj, (list, tuple)):
        return type(obj)(numpy_to_serializable(v) for v in obj)
    return obj


def numpy_from_serializable(obj):
    """
    Inverse of :func:`numpy_to_serializable`.

    Examples::

        x = 'abc'
        y = np_to_serializable(x)

        z = np_from_serializable(y)
        assert z == x

        yy = json.dumps(y)
        zz = np_from_serializable(json.loads(yy))
        assert zz == x
    """
    if isinstance(obj, dict):
        if len(obj) == 1:
            key = list(obj.keys())[0]
            val = obj[key]
            if key == 'pandas.DataFrame':
                return pd.DataFrame(
                    val['columns'],
                    index=val['index'])
            if key == 'pandas.Series':
                return pd.Series(val['values'], index=val['index'])
            if key == 'numpy.ndarray':
                return np.array(val)
            if key == 'numpy.scalar':
                return eval('np.' + val['type'])(val['value'])
        return {k: numpy_from_serializable(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return type(obj)(numpy_from_serializable(v) for v in obj)
    return obj

