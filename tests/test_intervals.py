from .context import druidry
from datetime import datetime, date, timedelta

import mock
import unittest


class IntervalTest(unittest.TestCase):

    def test_start_and_end_strs(self):
        interval = druidry.intervals.Interval(start='1970-01-01', end='1975-12-31')
        self.assertEqual(interval, '1970-01-01/1975-12-31')

    def test_start_and_end_dates(self):
        start = date(year=1970, month=1, day=1)
        end = date(year=1975, month=12, day=31)
        interval = druidry.intervals.Interval(start=start, end=end)
        self.assertEqual(interval, '1970-01-01/1975-12-31')

    def test_start_date_and_duration_str(self):
        start = date(year=1970, month=1, day=1)
        duration = 'P5Y'
        interval = druidry.intervals.Interval(start=start, duration=duration)
        self.assertEqual(interval, '1970-01-01/P5Y')

    def test_start_date_and_duration_delta(self):
        start = date(year=1970, month=1, day=1)
        duration = timedelta(days=7)
        interval = druidry.intervals.Interval(start=start, duration=duration)
        self.assertEqual(interval, '1970-01-01/P7D')

    def test_end_date_and_duration_str(self):
        end = date(year=1970, month=1, day=1)
        duration = 'P5Y'
        interval = druidry.intervals.Interval(end=end, duration=duration)
        self.assertEqual(interval, 'P5Y/1970-01-01')

    def test_end_date_and_duration_delta(self):
        end = date(year=1970, month=1, day=1)
        duration = timedelta(days=7)
        interval = druidry.intervals.Interval(end=end, duration=duration)
        self.assertEqual(interval, 'P7D/1970-01-01')

    def test_end_date_and_duration_kwargs(self):
        end = date(year=1970, month=1, day=1)
        interval = druidry.intervals.Interval(end=end, weeks=1)
        self.assertEqual(interval, 'P7D/1970-01-01')

    def test_duration_delta(self, *args):
        duration = timedelta(days=7)
        interval = druidry.intervals.Interval(duration=duration, end=datetime(year=2014, month=9, day=27))
        self.assertEqual(interval, 'P7D/2014-09-27T00:00:00')

    def test_duration_kwargs(self, *args):
        interval = druidry.intervals.Interval(years=5, months=3, days=25, end=datetime(year=2014, month=9, day=27))
        self.assertEqual(interval, 'P5Y3M25D/2014-09-27T00:00:00')

    def test_duration_interval(self, *args):
        interval = druidry.intervals.Interval(interval='P5Y/2014-09-27T00:00:00')
        self.assertEqual(interval, 'P5Y/2014-09-27T00:00:00')

    def test_pad_interval_day(self):
        start = datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        end = datetime(year=2014, month=9, day=30, hour=8, minute=17, second=2)
        interval = druidry.intervals.Interval(start=start, end=end)
        td = timedelta(days=12)
        self.assertEqual(interval.pad_by_timedelta(td), '2014-09-27T00:00:00/2014-10-01T00:00:00')

    def test_pad_interval_hour(self):
        start = datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        end = datetime(year=2014, month=9, day=30, hour=8, minute=17, second=2)
        interval = druidry.intervals.Interval(start=start, end=end)
        td = timedelta(hours=12)
        self.assertEqual(interval.pad_by_timedelta(td), '2014-09-27T16:00:00/2014-09-30T09:00:00')

    def test_pad_interval_minute(self):
        start = datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        end = datetime(year=2014, month=9, day=30, hour=8, minute=17, second=2)
        interval = druidry.intervals.Interval(start=start, end=end)
        td = timedelta(minutes=12)
        self.assertEqual(interval.pad_by_timedelta(td), '2014-09-27T16:22:00/2014-09-30T08:18:00')

    def test_start_date_and_implicit_end(self):
        start = date(year=1970, month=1, day=1)
        implicit_end = date(year=1975, month=12, day=31)
        with mock.patch('druidry.intervals._datetime_now', return_value=implicit_end):
            interval = druidry.intervals.Interval(start=start)
        self.assertEqual(interval, '1970-01-01/1975-12-31')

    def test_duration_str_and_implicit_end(self):
        implicit_end = date(year=1970, month=1, day=1)
        duration = 'P5Y'
        with mock.patch('druidry.intervals._datetime_now', return_value=implicit_end):
            interval = druidry.intervals.Interval(duration=duration)
        self.assertEqual(interval, 'P5Y/1970-01-01')

    def test_duration_delta_and_implicit_end(self):
        implicit_end = date(year=1970, month=1, day=1)
        duration = timedelta(days=7)
        with mock.patch('druidry.intervals._datetime_now', return_value=implicit_end):
            interval = druidry.intervals.Interval(duration=duration)
        self.assertEqual(interval, 'P7D/1970-01-01')

    def test_duration_kwargs_and_implicit_end(self):
        implicit_end = date(year=1970, month=1, day=1)
        with mock.patch('druidry.intervals._datetime_now', return_value=implicit_end):
            interval = druidry.intervals.Interval(weeks=1)
        self.assertEqual(interval, 'P7D/1970-01-01')
