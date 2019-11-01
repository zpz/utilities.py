import itertools
from typing import Iterable

# https://stackoverflow.com/a/54451553
# Also see recipe 'grouper' in 'itertools' doc.
def batch_iterable(x: Iterable, batch_size: int):
    assert batch_size > 1
    args = [iter(x)] * batch_size
    for group in itertools.zip_longest(*args):
        yield itertools.filterfalse(lambda x: x is None, group)

