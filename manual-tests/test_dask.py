from pytest import fixture

from coyote.dask import Dask, LocalDask


# @fixture(scope='module')
# def dask():
#     return Dask()
    
    
@fixture(scope='module')
def local_dask():
    return LocalDask()


def do_work(client):
    def square(x):
        return x ** 2

    def neg(x):
        return -x

    A = client.map(square, range(10))
    B = client.map(neg, A)
    total = client.submit(sum, B)
    assert total.result() == -285


# def test(dask):
#     do_work(dask)


def test_local(local_dask):
    do_work(local_dask)
