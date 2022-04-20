import pandas as pd
import pytest


@pytest.mark.skip(reason='no reason')
def test_pandas_dt():
    ts_i = 1541990217796
    ts_dt = pd.to_datetime(ts_i, unit='ms')
    print(ts_dt)
    print(ts_dt.hour)
    print(ts_dt.day)
    print(ts_dt.week)
    print(ts_dt.month)
    print(ts_dt.year)
    print(ts_dt.day_of_week)
    print('just holding place')


def test_casting():
    # casting as int will work if userId is a numeric, not if it is a string
    userId = 97.0
    try:
        print(int(userId))
    except:
        assert pytest.fail('casting userId failed')
