import os
import os.path
from shutil import rmtree

import pytest
from zpz.biglist import Biglist, ListView
from zpz.exceptions import ZpzError


PATH = os.path.join(os.environ['TMPDIR'], 'test', 'biglist')

def test_numbers():
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = Biglist(PATH, batch_size=5)

    for i in range(21):
        mylist.append(i)

    mylist.extend([21, 22, 23, 24, 25])
    mylist.extend([26, 27, 28])
    mylist.flush()

    data = list(range(len(mylist)))
    n = 0
    for x in mylist:
        assert x == data[n]
        n += 1

    assert list(mylist) == data


def test_existing_numbers():
    mylist = Biglist(PATH)
    data = list(range(len(mylist)))

    mylist.append(29)
    mylist.append(30)
    mylist.append(31)
    mylist.extend([32, 33, 34, 35, 36])

    data = list(range(len(mylist)))

    assert list(mylist) == data

    rmtree(PATH)
    

def _test_listview(datalv):
    data = list(range(20))
    assert list(datalv) == data

    assert datalv[8] == data[8]
    assert datalv[17] == data[17]

    lv = datalv[:9]
    assert isinstance(lv, ListView)
    assert list(lv) == data[:9]
    assert lv[-1] == data[8]
    assert lv[3] == data[3]

    n = 0
    for batch in lv.batches(4):
        k = len(batch)
        assert list(batch) == data[n : (n+k)]
        n += k

    lv = lv[:2:-2]
    assert list(lv) == data[8:2:-2]

    lv = datalv[10:17]
    assert lv[3] == data[13]
    assert list(lv[3:6]) == data[13:16]
    assert list(lv[-3:]) == data[14:17]
    assert list(lv[::2]) == data[10:17:2]
    assert list(lv) == data[10:17]

    lv = datalv[::-2]
    assert list(lv) == data[::-2]
    assert list(lv[:3]) == [data[-1], data[-3], data[-5]]
    assert lv[2] == data[-5]
    assert list(lv[::-3]) == data[1::6]

    n = 19
    for batch in lv.batches(3):
        k = len(batch)
        assert list(batch) == data[n : (n - 2*k + 1) : -2]
        n = n - k*2


def test_listview():
    _test_listview(ListView(list(range(20))))


def test_biglistview():
    mylist = Biglist(batch_size=7)
    mylist.extend(range(20))
    _test_listview(mylist.view)
