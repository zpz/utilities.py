"""
This is a small Python language exercise.
Recommendation for practical use of this decorator is not implied.
"""

import functools
from typing import Any, Callable


class Infix:
    """
    Suppose we have

        @Infix
        def func(x, y):
            return ...

    Then,

        x |func| y

    is equivalent to

        func(x, y)
    """

    def __init__(self, function: Callable[[Any, Any], Any]):
        self.function = function

    def __ror__(self, other):
        return Infix(functools.partial(self.function, other))

    def __or__(self, other):
        return self.function(other)

    def __call__(self, x, y):
        return self.function(x, y)


infix = Infix
