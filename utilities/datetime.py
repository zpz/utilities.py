"""
This module gathers some datetime functions.

For the very short functions in this module,
the user may want to simply copy these examples instead of ``import`` ing
this module, while keeping this module as a reference to look up.
"""

from datetime import datetime
from pytz import timezone


def tznow(tzname):
    """
    Get the current ``datetime`` in the specified timezone.

    Args:
        tzname: the ISO standard timezone name, such as ``Africa/Cairo``, ``America/Los_Angeles``.
    """
    return datetime.now(timezone(tzname))


def tzdate(tzname):
    """
    Get the current date part of ``datetime`` in the specified timezone.

    Args:
        tzname: the ISO standard timezone name, such as ``Africa/Cairo``, ``America/Los_Angeles``.
    """
    return datetime.now(timezone(tzname)).date()


def tznow_str(tzname):
    """
    Get the current ``datetime`` in the specified timezone,
    as a string formatted like '2016-09-03 18:22:03'.

    Args:
        tzname: the ISO standard timezone name, such as ``Africa/Cairo``, ``America/Los_Angeles``.
    """
    return tznow(tzname).strftime('%Y-%m-%d %H:%M:%S')


def tzdate_str(tzname):
    """
    Get the date part of current ``datetime`` in the specified timezone,
    as a string formatted like '2016-09-03'.

    Args:
        tzname: the ISO standard timezone name, such as ``Africa/Cairo``, ``America/Los_Angeles``.
    """
    return tzdate(tzname).strftime("%Y-%m-%d")
