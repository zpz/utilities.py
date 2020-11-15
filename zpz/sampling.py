import random
from typing import Iterable


def reservior_sample(source: Iterable, k):
    '''
    Return a list of `k` elements randomly sampled
    from `source`, which may contain more or less
    than `k` elements.
    '''
    dest = [None for _ in range(k)]

    for i, z in enumerate(source):
        if i < k:
            dest[i] = z
        j = random.randint(0, i)
        if j < k:
            return dest

    # Currently `dest` has `i + 1` elements.
    return random.choices(dest[: (i+1)], k=k)
