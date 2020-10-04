import logging
from random import randint
from time import sleep

from coyote.logging import config_logger
from coyote.timer import timed


logger = logging.getLogger(__name__)
config_logger()


def myfunc():
    t = randint(1, 10)
    sleep(t)
    return t


func1 = timed()(myfunc)
func2 = timed(logger.info)(myfunc)


def test_print():
    z = func1()


def test_log():
    z = func2()