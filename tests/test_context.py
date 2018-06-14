"""Aggregation tests."""

from .context import druidry
import datetime
import unittest


class TestContext(unittest.TestCase):

    def test_data_source_context(self):
        query = druidry.queries.Query(query_type='timeBoundary')
        with druidry.context.data_source_context('users'):
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query, {
                'queryType': 'timeBoundary',
                'dataSource': 'users'
            })

    def test_query_filter_context(self):
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        query = druidry.queries.Query(
            query_type='topN',
            dimension='browser',
            metric='count',
            data_source='users',
            threshold=10,
            granularity='P1D',
            aggregations=[aggregation],
            intervals=[druidry.intervals.Interval(years=2, end=datetime.datetime(year=2014, month=9, day=27))])
        filter_ = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        with druidry.context.query_filter_context('active_users_filter', filter_):
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query, {
                'aggregations': [
                    {
                        'fieldName': 'user_count',
                        'type': 'longSum',
                        'name': 'users_sum'
                    }
                ],
                'metric': 'count',
                'queryType': 'topN',
                'filter': {
                    'type': 'selector',
                    'dimension': 'is_active',
                    'value': 't'
                },
                'intervals': [
                    'P2Y/2014-09-27T00:00:00'
                ],
                'dataSource': 'users',
                'granularity': 'P1D',
                'threshold': 10,
                'dimension': 'browser'
            })

    def test_timeout_context(self):
        query = druidry.queries.Query(query_type='timeBoundary', data_source='users')
        with druidry.context.timeout_context(timeout=1000):
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query, {
                'queryType': 'timeBoundary',
                'context': {
                    'timeout': 1000
                },
                'dataSource': 'users'
            })

    def test_pad_interval_context_start_duration(self):
        interval = druidry.intervals.Interval(
            start=datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47),
            duration='P3DT4H59M')

        query = druidry.queries.TimeseriesQuery(
            granularity=druidry.granularities.SimpleGranularity('hour'),
            aggregations=[],
            intervals=interval)

        with druidry.context.pad_query_interval_context():
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query['intervals'], '2014-09-27T16:00:00/2014-09-30T22:00:00')

    def test_pad_interval_context_end_duration(self):
        interval = druidry.intervals.Interval(
            end=datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47),
            duration='P3DT4H59M')

        query = druidry.queries.TimeseriesQuery(
            granularity=druidry.granularities.SimpleGranularity('hour'),
            aggregations=[],
            intervals=interval)

        with druidry.context.pad_query_interval_context():
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query['intervals'], '2014-09-24T11:00:00/2014-09-27T17:00:00')

    def test_pad_interval_context_start_end(self):
        interval = druidry.intervals.Interval(
            start=datetime.datetime(year=2014, month=9, day=24, hour=11, minute=13, second=1),
            end=datetime.datetime(year=2014, month=9, day=27, hour=16, minute=22, second=47))

        query = druidry.queries.TimeseriesQuery(
            granularity=druidry.granularities.SimpleGranularity('hour'),
            aggregations=[],
            intervals=interval)

        with druidry.context.pad_query_interval_context():
            processed_query = druidry.context.process_query(query)
            self.assertEqual(processed_query['intervals'], '2014-09-24T11:00:00/2014-09-27T17:00:00')
