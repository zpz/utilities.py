from typing import Iterable


def all_distinct(x: Iterable) -> bool:
    z = set()
    for xx in x:
        if xx in z:
            return False
        z.add(xx)
    return True
