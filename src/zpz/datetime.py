from datetime import datetime
from pytz import timezone
from typing import List, Union, Tuple

import arrow


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



def shift_day(day: str, ndays: int) -> str:
    '''
    Given a date like '2018-09-23', shift it by (positive or negative) `ndays` days, e.g.

        shift_day('2018-09-23', -1)    # '2018-09-22'
        shift_day('2018-09-23', 10)    # '2018-10-03'

    This operation is ignorant of timezone.
    '''
    if ndays == 0:
        return day
    d = arrow.get(day, 'YYYY-MM-DD')
    return d.shift(days=ndays).format('YYYY-MM-DD')


def shift_hour(day: str, hour: str, nhours: int) -> Tuple[str, str]:
    '''
        shift_hour('2018-09-20', '08', 12)    # ('2018-09-20', '20')
        shift_hour('2018-09-20', '08', -24)   # ('2018-09-19', '08')
    '''
    if nhours == 0:
        return day, hour
    if len(hour) == 1:
        hour = '0' + hour
    dt = arrow.get(day + ' ' + hour, 'YYYY-MM-DD HH')
    dt = dt.shift(hours=nhours)
    return tuple(dt.format('YYYY-MM-DD HH').split())


class DateRange:
    def __init__(self,
                 start_date: str=None,
                 end_date: str=None,
                 n_days: int=None,
                 timezone: str='UTC'):
        '''
        `start_date` and `end_date` are either one of 'today', 'yesterday', 'tomorrow',
        or in the 'YYYY-MM-DD' format.

        `timezone`: other than the default 'UTC', other common values include 'US/Pacific', 'US/Eastern'.
        '''
        ff = 'YYYY-MM-DD'
        self._format = ff

        if timezone == 'UTC':
            today = arrow.utcnow().format('YYYY-MM-DD')
        else:
            today = arrow.now(timezone).format('YYYY-MM-DD')
        yesterday = shift_day(today, -1)
        tomorrow = shift_day(today, 1)
        common_dates = {'today': today, 'yesterday': yesterday, 'tomorrow': tomorrow}

        if start_date:
            start_date = common_dates.get(start_date, start_date)
        if end_date:
            end_date = common_dates.get(end_date, end_date)

        if start_date:
            if end_date:
                assert n_days is None
                assert start_date <= end_date
            else:
                assert n_days > 0
                end_date = shift_day(start_date, n_days - 1)
        else:
            assert end_date
            assert n_days > 0
            start_date = shift_day(end_date, -(n_days - 1))

        self._start_date = start_date
        self._end_date = end_date
        self._days = [start_date]
        while start_date < end_date:
            start_date = shift_day(start_date, 1)
            self._days.append(start_date)

    @property
    def days(self) -> List[str]:
        return self._days

    @property
    def n_days(self) -> int:
        return len(self._days)

    @property
    def first(self):
        return self._days[0]

    @property
    def last(self):
        return self._days[-1]
