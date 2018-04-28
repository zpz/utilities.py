from datetime import datetime
from pytz import timezone
from typing import List, Union

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


class DateRange:
    def __init__(self,
                 start_date: str = None,
                 end_date: str = None,
                 n_days: int = None,
                 min_days: int = None,
                 today_ok: bool = False,
                 utc: bool = True):
        '''
        `last_day` is either `today` (when `today_ok` is `True`) or `yesterday` (when `today_ok` is `False`).

        Examples:

            DateRange(start_date='2018-02-23', n_days=4)    # forward from a start
            DateRange(n_days=3)    # backward from `last_day`
            DateRange(start_date='2018-02-23', end_date='2018-02-25')
            DateRange(start_date='2018-02-23', min_days=30)    # since start till `last_day`; error if not enough days
            DateRange(n_days=2)    # backward from `last_day`
            DateRange(min_days=2)    # backward from `last_day`
        '''
        f = 'YYYY-MM-DD'
        self._format = f

        last_day = arrow.utcnow() if utc else arrow.now()
        if not today_ok:
            last_day = last_day.shift(days=-1)
        last_day = last_day.floor('day')

        if start_date:
            start_date = arrow.get(start_date, f)
            if not utc:
                start_date = start_date.replace(tzinfo='local')
            start_date = start_date.floor('day')
            assert start_date <= last_day
        if end_date:
            end_date = arrow.get(end_date, f)
            if not utc:
                end_date = end_date.replace(tzinfo='local')
            end_date = end_date.floor('day')
            assert end_date <= last_day

        if min_days is not None:
            min_days = int(min_days)
            assert min_days > 0

        if n_days is not None:
            n_days = int(n_days)
            if n_days <= 0:
                n_days = None
            else:
                if min_days:
                    assert n_days >= min_days

        if start_date:
            if end_date:
                assert end_date >= start_date
                if n_days:
                    assert (end_date - start_date).days + 1 == n_days
            else:
                if n_days:
                    end_date = start_date.shift(days=n_days - 1)
                    assert end_date <= last_day
                else:
                    end_date = last_day
                    if min_days:
                        assert (end_date - start_date).days + 1 >= min_days
        else:
            if not end_date:
                end_date = last_day
            if n_days:
                start_date = end_date.shift(days=-(n_days - 1))
            else:
                assert min_days
                start_date = end_date.shift(days=-(min_days - 1))

        self._start_date = start_date
        self._end_date = end_date
        self._days = arrow.Arrow.range('day', self._start_date, self._end_date)

    @property
    def format(self) -> str:
        return self._format

    @format.setter
    def format(self, value: Union[str, None]) -> None:
        self._format = value

    @property
    def days(self) -> List[Union[str, arrow.Arrow]]:
        if self._format is None:
            return self._days
        else:
            return [v.format(self._format) for v in self._days]

    @property
    def n_days(self) -> int:
        return len(self._days)

    @property
    def first(self):
        z = self._days[0]
        if self._format is None:
            return z
        else:
            return z.format(self._format)

    @property
    def last(self):
        z = self._days[-1]
        if self._format is None:
            return z
        else:
            return z.format(self._format)

    def __iter__(self):
        return iter(self._days)