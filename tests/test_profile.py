import math

from zpz import profile


@profile.profiled()
def test_profiler():
    result = 0.
    for i in range(1000000):
        result += math.sin(i)
