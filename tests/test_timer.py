import logging
from random import randint
from time import sleep

from zpz.timer import timed


logger = logging.getLogger(__name__)


def myfunc():
    t = randint(1, 10)
    sleep(t)
    return t


func1 = timed()(myfunc)
func2 = timed(logger.info)(myfunc)


def test_print():
    _ = func1()


def test_log():
    _ = func2()
