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
    
    # assert mylist[3] == data[3]
    # assert mylist[22] == data[22]
    # assert mylist[-1] == data[-1]

    # assert list(mylist.view[28:23]) == data[28:23]
    # assert list(mylist.view[-8:]) == data[-8:]
    # assert list(mylist.view[-3:-11:-1]) == data[-3:-11:-1]
    # assert list(mylist.view[:6]) == data[:6]

    # n = 0
    # for batch in mylist.view.batches():
    #     batch = list(batch)
    #     assert list(batch) == data[n : (n+5)]
    #     n += 5

    # n = 0
    # for batch in mylist.view.batches(3):
    #     assert list(batch) == data[n : (n+3)]
    #     n += 3

    # n = 0
    # for batch in mylist.view.batches(7):
    #     assert list(batch) == data[n : (n+7)]
    #     n += 7

    # n = 0
    # for batch in mylist.view.batches(57):
    #     assert list(batch) == data[n : (n+57)]
    #     n += 57







    # n = 0
    # for batch in mylist.view.batches():
    #     assert list(batch) == data[n : (n + mylist.batch_size)]
    #     n += mylist.batch_size


    # n = 0
    # for batch in mylist.view.batches(3):
    #     assert list(batch) == data[n : (n+3)]
    #     n += 3