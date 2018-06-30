import os
from pathlib import Path
import pytest
from utilities.notify import notify

notifydir = Path(os.environ['NOTIFYDIR'])


@notify()
def simple(x: int):
    if x > 3:
        return True
    raise Exception('wrong!')


def test_simple():
    fname = notifydir / (__name__ + '.simple')
    assert simple(4)
    assert open(fname, 'r').read().startswith('OK ')

    with pytest.raises(Exception):
        simple(2)
        assert open(fname, 'r').read().startswith('CRITICAL ')

    simple(5)
    assert open(fname, 'r').read().startswith('OK ') 

    os.remove(fname)

class MyError(Exception):
    pass


@notify(MyError)
def fancy(x: int):
    if x == 0:
        return True
    if x == 1:
        raise MyError('My error!')
    raise Exception('other error!')


def test_fancy():
    fname = notifydir / (__name__ + '.fancy')
    assert fancy(0)
    assert open(fname, 'r').read().startswith('OK ')

    with pytest.raises(MyError):
        fancy(1)
        assert open(fname, 'r').read().startswith('CRITICAL ')

    with pytest.raises(Exception):
        fancy(2)
        assert open(fname, 'r').read().startswith('OK ')

    os.remove(fname)