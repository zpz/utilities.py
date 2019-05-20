import os
import os.path
from shutil import rmtree

import pytest
from zpz.biglist import Biglist
from zpz.exceptions import ZpzError


PATH = os.path.join(os.environ['TMPDIR'], 'test', 'biglist')

def test_numbers():
    if os.path.isdir(PATH):
        rmtree(PATH)

    mylist = Biglist(PATH, batch_size=5)

    for i in range(20):
        mylist.append(i)

    mylist.extend([20, 21, 22, 23, 24, 25])
    mylist.extend([26, 27, 28])
    mylist.flush()

    data = list(range(29))

    assert mylist[3] == data[3]
    assert mylist[22] == data[22]
    assert mylist[-1] == data[-1]

    with pytest.raises(ValueError):
        print('item[:6]', mylist[:6])
    with pytest.raises(ValueError):
        print('item[12:19]', mylist[12:19])

    assert mylist[-3:] == data[-3:]

    n = 0
    for batch in mylist.batches():
        assert batch == data[n : (n+5)]
        n += 5

    n = 0
    for batch in mylist.batches(3):
        assert batch == data[n : (n+3)]
        n += 3

    n = 0
    for batch in mylist.batches(4):
        assert batch == data[n : (n+4)]
        n += 4

    n = 0
    for x in mylist:
        assert x == data[n]
        n += 1

def test_existing_numbers():
    mylist = Biglist(PATH, append=True)
    data = list(range(29))

    n = 0
    for batch in mylist.batches():
        assert batch == data[n : (n+5)]
        n += 5

    mylist.append(29)
    mylist.append(30)
    mylist.append(31)
    mylist.extend([32, 33, 34, 35, 36])

    data = list(range(37))

    n = 0
    for batch in mylist.batches(3):
        assert batch == data[n : (n+3)]
        n += 3