from zpz.infix import Infix


@Infix
def add(x, y):
    return x + y


def test_infix():
    assert add(3, 4) == (3 | add | 4)
