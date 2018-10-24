import arrow
import pytest

from zpz.datetime import DateRange


def test_daterange():
    dr = DateRange(start_date='2018-02-03', end_date='2018-02-05')
    assert dr.n_days == 3
    assert dr.days == ['2018-02-03', '2018-02-04', '2018-02-05']

    dr = DateRange(start_date='2018-01-15', n_days=3)
    assert dr.days == ['2018-01-15', '2018-01-16', '2018-01-17']

    dr = DateRange(end_date='2018-02-22', n_days=3)
    assert dr.days == ['2018-02-20', '2018-02-21', '2018-02-22']

    last_day = arrow.utcnow().shift(days=-1).floor('day')
    dr = DateRange(n_days=3)
    assert dr.days == [
        last_day.shift(days=i).format('YYYY-MM-DD') for i in (-2, -1, 0)
    ]
    dr = DateRange(min_days=3)
    assert dr.days == [
        last_day.shift(days=i).format('YYYY-MM-DD') for i in (-2, -1, 0)
    ]

    with pytest.raises(AssertionError) as e:
        dr = DateRange(
            start_date=last_day.shift(days=-3).format('YYYY-MM-DD'),
            min_days=10)

    last_day = arrow.utcnow().shift(days=-1).floor('day')
    dr = DateRange(n_days=20)
    days = [
        last_day.shift(days=-n).format('YYYY-MM-DD')
        for n in reversed(range(20))
    ]
    for got, wanted in zip(dr, days):
        assert got.format('YYYY-MM-DD') == wanted