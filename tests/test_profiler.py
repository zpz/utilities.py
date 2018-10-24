import math

from zpz import profiler


@profiler.profiled()
def test_profiler():
    result = 0.
    for i in range(1000000):
        result += math.sin(i)
