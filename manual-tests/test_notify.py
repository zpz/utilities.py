import os
from pathlib import Path
import pytest
import time

from zpz.notify import notify

notifydir = Path(os.environ['NOTIFYDIR'])


@notify()
def simple(x: int):
    if x > 3:
        return True
    raise Exception('simply wrong! [testing]')


def test_simple():
    fname = notifydir / (__name__ + '.simple')
    if fname.exists():
        os.remove(fname)

    assert simple(4)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is sent

    with pytest.raises(Exception):
        simple(2)
        assert open(fname, 'r').read().startswith('ERROR')
        # check slack ERROR alert is sent

    simple(5)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is sent

    os.remove(fname)

    # 2 'OK' alerts and 1 'ERROR' alert


class MyError(Exception):
    pass


@notify(MyError)
def fancy(x: int):
    if x == 0:
        return True
    if x == 1:
        raise MyError('My fancy error! [testing]')
    raise Exception('other error! [testing]')


def test_fancy():
    fname = notifydir / (__name__ + '.fancy')
    if fname.exists():
        os.remove(fname)

    assert fancy(0)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is sent

    with pytest.raises(MyError):
        fancy(1)
        assert open(fname, 'r').read().startswith('ERROR')
        # check slack ERROR alert is sent

    with pytest.raises(Exception):
        fancy(2)
        assert open(fname, 'r').read().startswith('OK')
        # check slack OK alert is sent

    os.remove(fname)

    # 2 'OK' alerts and 1 'ERROR' alert


@notify(silent_seconds=2, ok_silent_hours=4. / 3600.)
def silent(x: int):
    if x > 3:
        return True
    raise Exception('silently wrong! [testing]')


def test_silent():
    fname = notifydir / (__name__ + '.silent')
    if fname.exists():
        os.remove(fname)

    assert silent(4)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is sent

    for _ in range(5):
        assert silent(4)
        assert open(fname, 'r').read().startswith('OK')
        # check slack OK alert is NOT sent

    time.sleep(3)
    assert silent(4)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is NOT sent

    time.sleep(5)
    assert silent(4)
    assert open(fname, 'r').read().startswith('OK')
    # check slack OK alert is sent

    with pytest.raises(Exception):
        silent(2)
        assert open(fname, 'r').read().startswith('ERROR')
        # check slack ERROR alert is sent

    for _ in range(5):
        with pytest.raises(Exception):
            silent(2)
            assert open(fname, 'r').read().startswith('ERROR')
            # check slack ERROR alert is NOT sent

    time.sleep(1)
    with pytest.raises(Exception):
        silent(2)
        assert open(fname, 'r').read().startswith('ERROR')
        # check slack ERROR alert is NOT sent

    time.sleep(3)
    with pytest.raises(Exception):
        silent(2)
        assert open(fname, 'r').read().startswith('ERROR')
        # check slack ERROR alert is sent

    os.remove(fname)

    # 2 'OK' alerts and 2 'ERROR' alerts
