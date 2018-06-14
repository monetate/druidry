
from .context import druidry
import datetime
import unittest


class DurationTest(unittest.TestCase):

    def test_invalid_duration(self):
        with self.assertRaises(ValueError):
            druidry.durations.validate_duration('PT20Z')

    def test_has_duration_kwarg(self):
        result = druidry.durations.has_duration_kwarg(foo='foo', bar='bar', years=2)
        self.assertTrue(result)

    def test_does_not_have_duration_kwarg(self):
        result = druidry.durations.has_duration_kwarg(foo='foo', bar='bar', baz='baz')
        self.assertFalse(result)

    def test_duration_kwargs_to_isoformat(self):
        result = druidry.durations.duration_kwargs_to_isoformat(foo='foo', bar='bar', years=2)
        self.assertEqual(result, 'P2Y')

    def test_floor_datetime_day_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(days=12)
        floored_dt = datetime.datetime(year=2014, month=9, day=27)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_floor_datetime_day_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(days=1)
        floored_dt = datetime.datetime(year=2014, month=9, day=27)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_floor_datetime_hour_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(hours=12)
        floored_dt = datetime.datetime(year=2014, month=9, day=27, hour=16)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_floor_datetime_hour_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(hours=1)
        floored_dt = datetime.datetime(year=2014, month=9, day=27, hour=16)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_floor_datetime_minute_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(minutes=30)
        floored_dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_floor_datetime_minute_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(minutes=1)
        floored_dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22)
        self.assertEqual(druidry.durations.floor_datetime(dt, td), floored_dt)

    def test_ceil_datetime_day_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(days=12)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=28)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_day_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(days=1)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=28)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_hour_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(hours=12)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=27, hour=17)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_hour_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(hours=1)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=27, hour=17)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_minute_multiple(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(minutes=30)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=23)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_minute_1(self):
        dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47)
        td = datetime.timedelta(minutes=1)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=27, hour=16, minute=23)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_ceil_datetime_eq(self):
        dt = datetime.datetime(year=2014, month=9, day=27)
        td = datetime.timedelta(days=1)
        ceiled_dt = datetime.datetime(year=2014, month=9, day=27)
        self.assertEqual(druidry.durations.ceil_datetime(dt, td), ceiled_dt)

    def test_get_timedelta_unit_day_multiple(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(days=12)), 'day')

    def test_get_timedelta_unit_day_1(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(days=1)), 'day')

    def test_get_timedelta_unit_hour_multiple(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(hours=12)), 'hour')

    def test_get_timedelta_unit_hour_1(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(hours=1)), 'hour')

    def test_get_timedelta_unit_minute_multiple(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(minutes=30)), 'minute')

    def test_get_timedelta_unit_minute_1(self):
        self.assertEqual(
            druidry.durations.get_timedelta_unit(datetime.timedelta(minutes=1)), 'minute')

    def test_parse_interval_duration_end(self):
        self.assertEqual(druidry.durations.parse_interval('P7DT12M/2010-01-08T12:41'), (
            datetime.datetime(year=2010, month=1, day=1, hour=12, minute=29),
            datetime.datetime(year=2010, month=1, day=8, hour=12, minute=41),
            datetime.timedelta(days=7, minutes=12)
        ))

    def test_parse_interval_start_duration(self):
        self.assertEqual(druidry.durations.parse_interval('2010-01-01T12:29/P7DT12M'), (
            datetime.datetime(year=2010, month=1, day=1, hour=12, minute=29),
            datetime.datetime(year=2010, month=1, day=8, hour=12, minute=41),
            datetime.timedelta(days=7, minutes=12)
        ))

    def test_parse_interval_start_end(self):
        self.assertEqual(druidry.durations.parse_interval('2010-01-01T12:29/2010-01-08T12:41'), (
            datetime.datetime(year=2010, month=1, day=1, hour=12, minute=29),
            datetime.datetime(year=2010, month=1, day=8, hour=12, minute=41),
            datetime.timedelta(days=7, minutes=12)
        ))

    def test_select_granularity_all(self):
        interval = 'P20DT7H48M22S/2017-02-03T11:25:29'
        self.assertEqual(
            druidry.durations.select_granularity(interval, 1), 'all')

    def test_select_granularity_exact(self):
        interval = 'P20DT7H48M22S/2017-02-03T11:25:29'
        self.assertEqual(
            druidry.durations.select_granularity(interval, 12),
            datetime.timedelta(1, 59941, 833333))

    def test_select_granularity_choices(self):
        interval = 'P20D/2017-02-03'
        choices = [
            'PT1M',
            'PT15M',
            'PT30M',
            'PT1H',
            'PT4H',
            'PT6H',
            'PT12H',
            'P1D',
            'P7D',
            'P30D'
        ]
        self.assertEqual(
            druidry.durations.select_granularity(interval, 12, choices=choices),
            'P1D')

    def test_select_granularity_choices_many(self):
        interval = 'P20D/2017-02-03'
        choices = [
            'PT1M',
            'PT15M',
            'PT30M',
            'PT1H',
            'PT4H',
            'PT6H',
            'PT12H',
            'P1D',
            'P7D',
            'P30D'
        ]
        self.assertEqual(
            druidry.durations.select_granularity(interval, 100, choices=choices),
            'PT4H')

    def test_select_granularity_resolution(self):
        interval = 'P20DT7H48M22S/2017-02-03'
        self.assertEqual(
            druidry.durations.select_granularity(interval, 12, resolution='P1D'),
            'P2D')
