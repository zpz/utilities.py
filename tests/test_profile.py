import math
import subprocess

from zpz import profile


def foo():
    result = 0.
    for i in range(1000000):
        result += math.sin(i)


def test_profiler():
    profile.profiled()(foo)()
    subprocess.call(['rm', '-f', 'cprofile.out'])


def test_line_profiler():
    profile.lineprofiled()(foo)()
    subprocess.call(['rm', '-f', 'cprofile.out'])

