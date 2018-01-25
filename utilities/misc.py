from typing import Iterable


basestring = (str, bytes)


def all_distinct(x: Iterable) -> bool:
    z = set()
    for xx in x:
        if xx in z:
            return False
        z.add(xx)
    return True


def is_iterable(obj) -> bool:
    """Similar in nature to :func:`callable`, ``is_iterable`` returns
    ``True`` if an object is `iterable`_, ``False`` if not.
    >>> is_iterable([])
    True
    >>> is_iterable(object())
    False
    .. _iterable: https://docs.python.org/2/glossary.html#term-iterable
    """
    try:
        iter(obj)
    except TypeError:
        return False
    return True


def is_scalar(obj) -> bool:
    """A near-mirror of :func:`is_iterable`. Returns ``False`` if an
    object is an iterable container type. Strings are considered
    scalar as well, because strings are more often treated as whole
    values as opposed to iterables of 1-character substrings.
    >>> is_scalar(object())
    True
    >>> is_scalar(range(10))
    False
    >>> is_scalar('hello')
    True
    """
    return not is_iterable(obj) or isinstance(obj, basestring)


def is_collection(obj) -> bool:
    """The opposite of :func:`is_scalar`.  Returns ``True`` if an object
    is an iterable other than a string.
    >>> is_collection(object())
    False
    >>> is_collection(range(10))
    True
    >>> is_collection('hello')
    False
    """
    return is_iterable(obj) and not isinstance(obj, basestring)

