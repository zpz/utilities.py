import os
import os.path
from shutil import rmtree

import pytest
from zpz.biglist import Biglist, BiglistView
from zpz.path import make_temp_dir


PATH = os.path.join(os.environ.get('TMPDIR', '/tmp'), 'test', 'biglist')


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


def _test_view():
    bl = Biglist()
    bl.extend(range(20))
    bl.flush()
    datalv = bl.view()

    data = list(range(20))
    assert list(datalv) == data

    assert datalv[8] == data[8]
    assert datalv[17] == data[17]

    lv = datalv[:9]
    assert isinstance(lv, BiglistView)
    assert list(lv) == data[:9]
    assert lv[-1] == data[8]
    assert lv[3] == data[3]

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


def test_move():
    bl = Biglist(batch_size=4)
    bl.extend(range(17))

    newpath = make_temp_dir()
    rmtree(newpath)
    bl.move(newpath)

    assert bl.path == newpath
    assert list(bl) == list(range(17))

    bl.destroy()


def _test_fileview():
    bl = Biglist(batch_size=4)
    bl.extend(range(22))
    bl.flush()
    assert len(bl.file_lengths) == 6

    assert list(bl.fileview(1)) == [4, 5, 6, 7]

    vs = bl.fileviews()
    list(vs[2]) == [8, 9, 10, 11]

    vvs = vs[2][1:3]
    assert list(vvs) == [9, 10]
