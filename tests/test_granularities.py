from .context import druidry
import datetime
import unittest


class GranularityTest(unittest.TestCase):

    def test_invalid_simple_granularity(self):
        with self.assertRaises(ValueError):
            druidry.granularities.SimpleGranularity('INVALID')

    def test_valid_simple_granularity(self):
        self.assertEqual(druidry.granularities.SimpleGranularity('day'), 'day')

    def test_duration_granularity(self):
        granularity = druidry.granularities.DurationGranularity(duration=7200000)
        self.assertEqual(granularity, {
            'duration': 7200000,
            'type': 'duration'
        })

    def test_invalid_origin_duration_granularity(self):
        with self.assertRaises(ValueError):
            druidry.granularities.DurationGranularity(
                duration=7200000,
                origin='INVALID')

    def test_origin_duration_granularity(self):
        granularity = druidry.granularities.DurationGranularity(
            duration=7200000,
            origin='1970-01-01T00:00:00Z')
        self.assertEqual(granularity, {
            'duration': 7200000,
            'origin': '1970-01-01T00:00:00Z',
            'type': 'duration'
        })

    def test_period_duration_granularity(self):
        granularity = druidry.granularities.PeriodGranularity(years=2)
        self.assertEqual(granularity, {
            'type': 'period',
            'period': 'P2Y'
        })

    def test_period_granularity(self):
        granularity = druidry.granularities.PeriodGranularity(period='P2Y')
        self.assertEqual(granularity, {
            'type': 'period',
            'period': 'P2Y'
        })

    def test_period_origin_granularity(self):
        granularity = druidry.granularities.PeriodGranularity(period='P2Y', origin='1970-01-01T00:00:00Z')
        self.assertEqual(granularity, {
            'origin': '1970-01-01T00:00:00Z',
            'type': 'period',
            'period': 'P2Y'
        })

    def test_period_origin_timezone_granularity(self):
        granularity = druidry.granularities.PeriodGranularity(
            period='P2Y', origin='1970-01-01T00:00:00Z', time_zone='America/Los_Angeles')
        self.assertEqual(granularity, {
            'origin': '1970-01-01T00:00:00Z',
            'type': 'period',
            'period': 'P2Y',
            'timeZone': 'America/Los_Angeles'
        })

    def test_invalid_time_zone_granularity(self):
        with self.assertRaises(ValueError):
            druidry.granularities.PeriodGranularity(period='P2Y', time_zone='Lilliput/Mildendo')

    def test_invalid_period_granularity(self):
        with self.assertRaises(ValueError):
            druidry.granularities.PeriodGranularity(period='P2Z')

    def test_period_granularity_timedelta(self):
        period_granularity = druidry.granularities.PeriodGranularity(period='PT1H')
        self.assertEqual(period_granularity.to_timedelta(), datetime.timedelta(hours=1))

    def test_duration_granularity_timedelta(self):
        duration_granularity = druidry.granularities.DurationGranularity(duration=13987)
        self.assertEqual(duration_granularity.to_timedelta(), datetime.timedelta(seconds=13, microseconds=987000))

    def test_simple_granularity_timedelta(self):
        simple_granularity = druidry.granularities.SimpleGranularity('thirty_minute')
        self.assertEqual(simple_granularity.to_timedelta(), datetime.timedelta(minutes=30))

    def test_period_granularity_timedelta_static(self):
        period_granularity = {'type': 'period', 'period': 'PT1H'}
        self.assertEqual(
            druidry.granularities.granularity_to_timedelta(period_granularity),
            datetime.timedelta(hours=1))

    def test_duration_granularity_timedelta_static(self):
        duration_granularity = {'type': 'duration', 'duration': 13987}
        self.assertEqual(
            druidry.granularities.granularity_to_timedelta(duration_granularity),
            datetime.timedelta(seconds=13, microseconds=987000))

    def test_simple_granularity_timedelta_static(self):
        simple_granularity = 'thirty_minute'
        self.assertEqual(
            druidry.granularities.granularity_to_timedelta(simple_granularity),
            datetime.timedelta(minutes=30))
