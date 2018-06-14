from .context import druidry
import datetime
import unittest


class QueryTestCase(unittest.TestCase):

    def test_timeseries_query(self):
        interval = druidry.intervals.Interval(start='1970-01-01', end='1970-12-31')
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.PeriodGranularity(days=1)

        expected = {
            'dataSource': 'users',
            'intervals': ['1970-01-01/1970-12-31'],
            'queryType': 'timeseries',
            'granularity': {
                'type': 'period',
                'period': 'P1D'
            },
            'aggregations': [
                {
                    'fieldName': 'user_count',
                    'type': 'longSum',
                    'name': 'users_sum'
                }
            ]
        }

        query_1 = druidry.queries.Query(
            query_type='timeseries',
            data_source='users',
            intervals=[interval],
            granularity=granularity,
            aggregations=[aggregation])
        self.assertEqual(query_1, expected)

        query_2 = druidry.queries.TimeseriesQuery(
            data_source='users',
            intervals=[interval],
            granularity=granularity,
            aggregations=[aggregation])
        self.assertEqual(query_2, expected)

    def test_top_n_query(self):
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.DurationGranularity(duration=60000)
        interval = druidry.intervals.Interval(years=2, end=datetime.datetime(year=2014, month=9, day=27))

        expected = {
            'aggregations': [
                {
                    'fieldName': 'user_count',
                    'type': 'longSum',
                    'name': 'users_sum'
                }
            ],
            'metric': 'count',
            'queryType': 'topN',
            'intervals': [
                'P2Y/2014-09-27T00:00:00'
            ],
            'dataSource': 'users',
            'granularity': {
                'duration': 60000,
                'type': 'duration'
            },
            'threshold': 10,
            'dimension': 'browser'
        }

        query_1 = druidry.queries.Query(
            query_type='topN',
            dimension='browser',
            metric='count',
            data_source='users',
            threshold=10,
            granularity=granularity,
            aggregations=[aggregation],
            intervals=[interval])
        self.assertEqual(query_1, expected)

        query_2 = druidry.queries.TopNQuery(
            dimension='browser',
            metric='count',
            data_source='users',
            threshold=10,
            granularity=granularity,
            aggregations=[aggregation],
            intervals=[interval])
        self.assertEqual(query_2, expected)

    def test_group_by_query(self):
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.DurationGranularity(duration=60000)
        interval = druidry.intervals.Interval(years=2, end=datetime.datetime(2014, month=9, day=27))
        expected = {
            'dimensions': ['browser', 'os'],
            'aggregations': [
                {
                    'fieldName': 'user_count',
                    'type': 'longSum',
                    'name': 'users_sum'
                }
            ],
            'intervals': [
                'P2Y/2014-09-27T00:00:00'
            ],
            'dataSource': 'users',
            'granularity': {
                'duration': 60000,
                'type': 'duration'
            },
            'queryType': 'groupBy'
        }

        query_1 = druidry.queries.Query(
            query_type='groupBy',
            dimensions=['browser', 'os'],
            data_source='users',
            granularity=granularity,
            aggregations=[aggregation],
            intervals=[interval])
        self.assertEqual(query_1, expected)

        query_2 = druidry.queries.GroupByQuery(
            dimensions=['browser', 'os'],
            data_source='users',
            granularity=granularity,
            aggregations=[aggregation],
            intervals=[interval])
        self.assertEqual(query_2, expected)

    def test_time_boundary_query(self):
        query_1 = druidry.queries.Query(query_type='timeBoundary')
        self.assertEqual(query_1, {'queryType': 'timeBoundary'})

        query_2 = druidry.queries.TimeBoundaryQuery()
        self.assertEqual(query_2, {'queryType': 'timeBoundary'})
