import math
import subprocess

from zpz import profile


@profile.profiled()
def foo():
    result = 0.
    for i in range(1000000):
        result += math.sin(i)


def test_profiler():
    foo()
    subprocess.call(['rm', '-f', 'cprofile.out'])
