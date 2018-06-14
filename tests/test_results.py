from .context import druidry
import datetime
import pandas as pd
import unittest


class TestQueryResult(unittest.TestCase):

    def test_groupby(self):
        start = datetime.datetime(year=2014, month=9, day=24)
        end = datetime.datetime(year=2014, month=9, day=27)
        interval = druidry.intervals.Interval(start=start, end=end)
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.PeriodGranularity(days=1)
        query = druidry.queries.GroupByQuery(
            data_source='users',
            aggregations=[aggregation],
            dimensions=['browser'],
            granularity=granularity,
            intervals=interval)

        result = [
            {
                "timestamp": "2014-09-24T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Chrome", "users_sum": 3376}
            },
            {
                "timestamp": "2014-09-24T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Safari", "users_sum": 9141}
            },
            {
                "timestamp": "2014-09-25T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Chrome", "users_sum": 4644}
            },
            {
                "timestamp": "2014-09-25T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Safari", "users_sum": 6639}
            },
            {
                "timestamp": "2014-09-26T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Chrome", "users_sum": 6823}
            },
            {
                "timestamp": "2014-09-26T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Safari", "users_sum": 9985}
            },
            {
                "timestamp": "2014-09-27T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Chrome", "users_sum": 2710}
            },
            {
                "timestamp": "2014-09-27T00:00:00.000Z",
                "version": "v1",
                "event": {"browser": "Safari", "users_sum": 4571}
            }
        ]

        query_result = druidry.results.QueryResult(query, result)
        df = query_result.to_dataframe()
        date_index = pd.date_range(start=start, end=end)
        browser_index = ['Chrome', 'Safari']
        index = pd.MultiIndex.from_product([date_index, browser_index])
        expected_df = pd.DataFrame(
            [3376, 9141, 4644, 6639, 6823, 9985, 2710, 4571],
            columns=['users_sum'],
            index=index)

        df_eq = pd.DataFrame.eq(df, expected_df)
        self.assertTrue(df_eq['users_sum'].all())

    def test_timeseries(self):
        start = datetime.datetime(year=2014, month=9, day=24)
        end = datetime.datetime(year=2014, month=9, day=30)
        interval = druidry.intervals.Interval(start=start, end=end)
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.PeriodGranularity(days=1)
        query = druidry.queries.TimeseriesQuery(
            data_source='users',
            intervals=[interval],
            granularity=granularity,
            aggregations=[aggregation])

        result = [
            {'result': {'users_sum': 9530}, 'timestamp': '2014-09-24T00:00:00.000Z'},
            {'result': {'users_sum': 9215}, 'timestamp': '2014-09-25T00:00:00.000Z'},
            {'result': {'users_sum': 4382}, 'timestamp': '2014-09-26T00:00:00.000Z'},
            {'result': {'users_sum': 2804}, 'timestamp': '2014-09-27T00:00:00.000Z'},
            {'result': {'users_sum': 1719}, 'timestamp': '2014-09-28T00:00:00.000Z'},
            {'result': {'users_sum': 7299}, 'timestamp': '2014-09-29T00:00:00.000Z'},
            {'result': {'users_sum': 3525}, 'timestamp': '2014-09-30T00:00:00.000Z'}
        ]

        query_result = druidry.results.QueryResult(query, result)
        df = query_result.to_dataframe()
        expected_df = pd.DataFrame(
            [9530, 9215, 4382, 2804, 1719, 7299, 3525],
            columns=['users_sum'],
            index=pd.date_range(start=start, end=end))

        df_eq = pd.DataFrame.eq(df, expected_df)
        self.assertTrue(df_eq['users_sum'].all())

    def test_timeseries_all(self):
        start = datetime.datetime(year=2014, month=9, day=24)
        end = datetime.datetime(year=2014, month=9, day=30)
        interval = druidry.intervals.Interval(start=start, end=end)
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.SimpleGranularity('all')
        query = druidry.queries.TimeseriesQuery(
            data_source='users',
            intervals=[interval],
            granularity=granularity,
            aggregations=[aggregation])

        result = [
            {'result': {'users_sum': 39749}, 'timestamp': '2014-09-24T00:00:00.000Z'}
        ]

        query_result = druidry.results.QueryResult(query, result)
        df = query_result.to_dataframe()
        expected_df = pd.DataFrame([39749], columns=['users_sum'])

        df_eq = pd.DataFrame.eq(df, expected_df)
        self.assertTrue(df_eq['users_sum'].all())

    def test_topn(self):
        start = datetime.datetime(year=2014, month=9, day=24)
        end = datetime.datetime(year=2014, month=9, day=27)
        interval = druidry.intervals.Interval(start=start, end=end)
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        granularity = druidry.granularities.SimpleGranularity('day')
        query = druidry.queries.TopNQuery(
            aggregations=[aggregation],
            dimension='browser',
            granularity=granularity,
            intervals=interval,
            threshold=4,
            metric='users_sum')
        result = [
            {
                'result': [
                    {'browser': 'Chrome', 'users_sum': 9120},
                    {'browser': 'Safari', 'users_sum': 8121},
                    {'browser': 'Firefox', 'users_sum': 6199},
                    {'browser': 'Internet Explorer', 'users_sum': 6824}
                ],
                'timestamp': '2014-09-24T00:00:00.000Z'
            },
            {
                'result': [
                    {'browser': 'Chrome', 'users_sum': 1167},
                    {'browser': 'Safari', 'users_sum': 8892},
                    {'browser': 'Firefox', 'users_sum': 8176},
                    {'browser': 'Internet Explorer', 'users_sum': 9365}
                ],
                'timestamp': '2014-09-25T00:00:00.000Z'
            },
            {
                'result': [
                    {'browser': 'Chrome', 'users_sum': 8169},
                    {'browser': 'Safari', 'users_sum': 9382},
                    {'browser': 'Firefox', 'users_sum': 4557},
                    {'browser': 'Internet Explorer', 'users_sum': 2425}
                ],
                'timestamp': '2014-09-26T00:00:00.000Z'
            },
            {
                'result': [
                    {'browser': 'Chrome', 'users_sum': 9442},
                    {'browser': 'Safari', 'users_sum': 8723},
                    {'browser': 'Firefox', 'users_sum': 9416},
                    {'browser': 'Internet Explorer', 'users_sum': 6368}
                ],
                'timestamp': '2014-09-27T00:00:00.000Z'
            }
        ]
        query_result = druidry.results.QueryResult(query, result)
        df = query_result.to_dataframe()

        date_index = pd.date_range(start=start, end=end)
        browser_index = ['Chrome', 'Safari', 'Firefox', 'Internet Explorer']
        index = pd.MultiIndex.from_product([date_index, browser_index])
        expected_df = pd.DataFrame(
            [
                9120, 8121, 6199, 6824, 1167, 8892, 8176, 9365,
                8169, 9382, 4557, 2425, 9442, 8723, 9416, 6368
            ],
            columns=['users_sum'],
            index=index)

        df_eq = pd.DataFrame.eq(df, expected_df)
        self.assertTrue(df_eq['users_sum'].all())

    def test_timeboundary_fail(self):
        query = druidry.queries.TimeBoundaryQuery()
        result = [
            {
                'timestamp': '2013-05-09T18:24:00.000Z',
                'result': {
                    'minTime': '2013-05-09T18:24:00.000Z',
                    'maxTime': '2013-05-09T18:37:00.000Z'
                }
            }
        ]
        with self.assertRaises(ValueError):
            druidry.results.QueryResult(query, result)
