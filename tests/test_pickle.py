import os
from coyote.pickle import pickle_dump, pickle_load


def test_dump_load():
    x = {'a': 3, 'b': 4}
    pickle_dump(x, '/tmp', 'test_pickle.pickle')
    try:
        y = pickle_load('/tmp/test_pickle.pickle')
        assert y == x
    finally:
        os.remove('/tmp/test_pickle.pickle')
