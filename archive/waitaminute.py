import atexit
import logging
import sys
from functools import partial
from typing import Callable, List


logger = logging.getLogger(__name__)


def register_exithook(func: Callable[..., None], *args, **kwargs) -> None:
    '''
    Register a function to be called right before the Python program
    terminates normally.

    However, test shows that this function will be called
    before both normal terminations and exception terminations.
    '''
    atexit.register(func, *args, **kwargs)


except_funcs: List[Callable[[], None]] = []


def register_excepthook(func: Callable[..., None], *args, **kwargs) -> None:
    '''
    Register a function to be called right before the Python program
    terminates due to an uncaught exception.
    '''
    except_funcs.append(partial(func, *args, **kwargs))


def except_handler(*args, **kwargs):
    for func in reversed(except_funcs):
        try:
            func()
        except BaseException as e:
            logger.error(str(e))
    sys.__excepthook__(*args, **kwargs)


sys.excepthook = except_handler


if __name__ == '__main__':
    # Tests.

    def say_hello():
        print('exit: hello')

    def say_yes():
        print('exit: yes')

    register_exithook(say_hello)
    register_exithook(say_yes)

    def say_hello2():
        print('except: hello')

    def say_yes2():
        print('except: yes')

    register_excepthook(say_hello2)
    register_excepthook(say_yes2)

    if len(sys.argv) > 1:
        x = 8 / 0

    print('all done')
