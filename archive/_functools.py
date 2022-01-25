import gc
from contextlib import contextmanager
from functools import wraps


class classproperty:
    """
    Decorator that converts a method with a single cls argument
    into a property that can be accessed directly from the class.

    See https://github.com/django/django/blob/master/django/utils/functional.py
    """

    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)

    def getter(self, method):
        self.fget = method
        return self


@contextmanager
def no_gc():
    isgc = gc.isenabled()
    if isgc:
        gc.disable()
    yield
    if isgc:
        gc.enable()


def nogc(func):
    @wraps(func)
    def deco(*args, **kwargs):
        with no_gc():
            return func(*args, **kwargs)

    return deco
