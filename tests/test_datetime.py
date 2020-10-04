import arrow
import pytest

from coyote.datetime import DateRange, shift_day, shift_hour



def test_shift_day():
    assert shift_day('2018-09-23', 0) == '2018-09-23'
    assert shift_day('2018-09-23', -1) == '2018-09-22'
    assert shift_day('2018-09-03', -5) == '2018-08-29'
    assert shift_day('2018-09-28', 6) == '2018-10-04'


def test_shift_hour():
    assert shift_hour('2018-09-20', '08', 12) == ('2018-09-20', '20')
    assert shift_hour('2018-09-20', '08', -24) == ('2018-09-19', '08')


def test_daterange():
    dr = DateRange(start_date='2018-02-03', end_date='2018-02-05')
    assert dr.n_days == 3
    assert dr.days == ['2018-02-03', '2018-02-04', '2018-02-05']

    dr = DateRange(start_date='2018-01-15', n_days=3)
    assert dr.days == ['2018-01-15', '2018-01-16', '2018-01-17']

    dr = DateRange(end_date='2018-02-22', n_days=3)
    assert dr.days == ['2018-02-20', '2018-02-21', '2018-02-22']

    last_day = arrow.utcnow().shift(days=-1).floor('day')
    dr = DateRange(n_days=3, end_date='yesterday')
    assert dr.days == [
        last_day.shift(days=i).format('YYYY-MM-DD') for i in (-2, -1, 0)
    ]

    last_day = arrow.utcnow().shift(days=-1).floor('day')
    dr = DateRange(n_days=20, end_date='yesterday')
    days = [
        last_day.shift(days=-n).format('YYYY-MM-DD')
        for n in reversed(range(20))
    ]
    for got, wanted in zip(dr.days, days):
        assert got.format('YYYY-MM-DD') == wanted