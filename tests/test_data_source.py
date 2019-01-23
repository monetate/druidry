
from .context import druidry
import datetime
import numbers
import unittest


class TestDimension(unittest.TestCase):

    def test_create_bound(self):
        dimension = druidry.data_source.Dimension(dimension='page')
        expected_value = druidry.filters.Filter(
            'bound', dimension='page', lower='a', upper='c',
            upper_strict=False, lower_strict=False, ordering='alphanumeric')
        self.assertEqual(dimension.create_bound(lower='a', upper='c'), expected_value)

    def test_create_bound_numeric(self):
        dimension = druidry.data_source.NumericDimension(dimension='count')
        expected_value = druidry.filters.Filter(
            'bound', dimension='count', lower=22, upper=42,
            upper_strict=False, lower_strict=False, ordering='numeric')

        self.assertEqual(dimension.create_bound(lower=22, upper=42), expected_value)

    def test_create_selector(self):
        dimension = druidry.data_source.Dimension(dimension='page')
        expected_value = druidry.filters.SelectorFilter(
            dimension='page', value='Home')
        self.assertEqual(dimension.create_selector('Home'), expected_value)


class TestDataSource(unittest.TestCase):

    def test_create_data_source(self):
        class WikipediaDataSource(druidry.data_source.DataSourceView):

            channel = druidry.data_source.CategoricalDimension(
                dimension='channel', choices=['email', 'web', 'mobile'],
                has_other_choice=True)

            user_count = druidry.aggregations.Aggregation(
                'longSum', field_name='count', name='count')

            anonymous_user_count = user_count.filter(
                channel.create_selector('true'), name='anonymous_user_count')

            anonymous_rate = druidry.data_source.ComplexMetric(
                metric='anonymous_rate', name='Anonymous rate',
                aggregations=[user_count, anonymous_user_count],
                post_aggregations=[anonymous_user_count / user_count]
            )

        data_source = WikipediaDataSource()

        expected_value = {
            "metrics": {
                "anonymous_rate": {
                    "metric": "anonymous_rate",
                    "name": "Anonymous rate",
                    "unit": None
                }
            },
            "dimensions": {
                "channel": {
                    "allow_multiple": True,
                    "can_split": True,
                    "name": "channel",
                    "type": "categorical",
                    "dimension": "channel",
                    "choices": [
                        {
                            "value": "email",
                            "label": "email"
                        },
                        {
                            "value": "mobile",
                            "label": "mobile"
                        },
                        {
                            "value": "other",
                            "label": "other"
                        },
                        {
                            "value": "web",
                            "label": "web"
                        }
                    ]
                }
            }
        }
        self.assertEqual(data_source.config, expected_value)

    def test_get_query(self):
        class WikipediaDataSource(druidry.data_source.DataSourceView):

            channel = druidry.data_source.CategoricalDimension(
                dimension='channel', choices=['email', 'web', 'mobile'],
                has_other_choice=True)

            is_anonymous = druidry.data_source.CategoricalDimension(
                dimension='isAnonymous', choices=['true', 'false'], name='is_anonymous')

            user_count = druidry.aggregations.Aggregation(
                'longSum', field_name='count', name='user_count')

            anonymous_user_count = user_count.filter(
                is_anonymous.create_selector('true'), name='anonymous_user_count')

            anonymous_rate = druidry.data_source.ComplexMetric(
                metric='anonymous_rate', name='Anonymous rate',
                aggregations=[user_count, anonymous_user_count],
                post_aggregations=[anonymous_user_count.divide(user_count, name='anonymous_rate')])

        data_source = WikipediaDataSource()
        expected_value = {
            "postAggregations": [
                {
                    "type": "arithmetic",
                    "fields": [
                        {
                            "type": "fieldAccess",
                            "name": "anonymous_user_count",
                            "fieldName": "anonymous_user_count"
                        },
                        {
                            "type": "fieldAccess",
                            "name": "user_count",
                            "fieldName": "user_count"
                        }
                    ],
                    "fn": "/",
                    "name": "anonymous_rate"
                }
            ],
            "aggregations": [
                {
                    "name": "anonymous_user_count",
                    "type": "filtered",
                    "aggregator": {
                        "type": "longSum",
                        "name": "anonymous_user_count",
                        "fieldName": "count"
                    },
                    "filter": {
                        "value": "true",
                        "dimension": "isAnonymous",
                        "type": "selector"
                    }
                },
                {
                    "type": "longSum",
                    "name": "user_count",
                    "fieldName": "count"
                }
            ],
            "queryType": "timeseries",
            "intervals": "P1D/2018-01-01",
            "granularity": "all"
        }

        query = data_source.get_query(
            metrics=['anonymous_rate'],
            duration={'days': 1},
            end=datetime.date(year=2018, day=1, month=1))

        self.assertEqual(query, expected_value)


class TestFilters(unittest.TestCase):

    maxDiff = None

    def test_equality_field_field_filter(self):
        input_filter = {
            "type": "==",
            "left": {"type": "field", "field": "isAnonymous"},
            "right": {"type": "field", "field": "isRobot"}
        }
        expected_filter = {
            "type": "columnComparison",
            "dimensions": ["isAnonymous", "isRobot"]
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_equality_field_value_filter(self):
        input_filter = {
            "type": "==",
            "left": {"type": "field", "field": "isAnonymous"},
            "right": {"type": "value", "value": "true"}
        }
        expected_filter = {
            "type": "selector",
            "dimension": "isAnonymous",
            "value": "true"
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_equality_value_field_filter(self):
        input_filter = {
            "type": "==",
            "left": {"type": "value", "value": "true"},
            "right": {"type": "field", "field": "isAnonymous"}
        }
        expected_filter = {
            "type": "selector",
            "dimension": "isAnonymous",
            "value": "true"
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_gt_field_str_value_filter(self):
        input_filter = {
            "type": ">",
            "left": {"type": "field", "field": "deleted"},
            "right": {"type": "value", "value": "42"}
        }
        expected_filter = {
            "ordering": "alphanumeric",
            "lower": "42",
            "type": "bound",
            "dimension": "deleted",
            "lowerStrict": True
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_gt_field_value_filter(self):
        input_filter = {
            "type": ">",
            "left": {"type": "field", "field": "deleted"},
            "right": {"type": "value", "value": 42}
        }
        expected_filter = {
            "ordering": "numeric",
            "lower": 42,
            "type": "bound",
            "dimension": "deleted",
            "lowerStrict": True
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_gte_field_value_filter(self):
        input_filter = {
            "type": ">=",
            "left": {"type": "field", "field": "deleted"},
            "right": {"type": "value", "value": 42}
        }
        expected_filter = {
            "ordering": "numeric",
            "lower": 42,
            "type": "bound",
            "dimension": "deleted",
            "lowerStrict": False
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_lt_field_value_filter(self):
        input_filter = {
            "type": "<",
            "left": {"type": "field", "field": "deleted"},
            "right": {"type": "value", "value": 42}
        }
        expected_filter = {
            "ordering": "numeric",
            "upper": 42,
            "type": "bound",
            "dimension": "deleted",
            "upperStrict": True
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_lte_field_value_filter(self):
        input_filter = {
            "type": "<=",
            "left": {"type": "field", "field": "deleted"},
            "right": {"type": "value", "value": 42}
        }
        expected_filter = {
            "ordering": "numeric",
            "upper": 42,
            "type": "bound",
            "dimension": "deleted",
            "upperStrict": False
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_gt_value_field_filter(self):
        input_filter = {
            "type": ">",
            "left": {"type": "value", "value": 42},
            "right": {"type": "field", "field": "deleted"},
        }
        expected_filter = {
            "ordering": "numeric",
            "upper": 42,
            "type": "bound",
            "dimension": "deleted",
            "upperStrict": True
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_gte_value_field_filter(self):
        input_filter = {
            "type": ">=",
            "left": {"type": "value", "value": 42},
            "right": {"type": "field", "field": "deleted"},
        }
        expected_filter = {
            "ordering": "numeric",
            "upper": 42,
            "type": "bound",
            "dimension": "deleted",
            "upperStrict": False
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_lt_value_field_filter(self):
        input_filter = {
            "type": "<",
            "left": {"type": "value", "value": 42},
            "right": {"type": "field", "field": "deleted"},
        }
        expected_filter = {
            "ordering": "numeric",
            "lower": 42,
            "type": "bound",
            "dimension": "deleted",
            "lowerStrict": True
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_inequality_lte_value_field_filter(self):
        input_filter = {
            "type": "<=",
            "left": {"type": "value", "value": 42},
            "right": {"type": "field", "field": "deleted"},
        }
        expected_filter = {
            "ordering": "numeric",
            "lower": 42,
            "type": "bound",
            "dimension": "deleted",
            "lowerStrict": False
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_filter)

    def test_complicated_filter(self):
        input_filter = {
            "type": "or",
            "filters": [
                {
                    "type": "and",
                    "filters": [
                        {
                            "type": ">=",
                            "left": {"type": "field", "field": "price"},
                            "right": {"type": "value", "value": 42}
                        },
                        {
                            "type": "in",
                            "left": {"type": "field", "field": "in_stock"},
                            "right": {"type": "value", "value": 't'}
                        }
                    ]
                },
                {
                    "type": "in",
                    "left": {"type": "field", "field": "category"},
                    "right": {"type": "value", "value": ["cat1", "cat17", "cat42"]}
                },
                {
                    "type": "startswith",
                    "left": {"type": "field", "field": "name"},
                    "right": {"type": "value", "value": "prefix"}
                }
            ]
        }
        expected_value = {
            "type": "or",
            "fields": [
                {
                    "type": "and",
                    "fields": [
                        {
                            "type": "bound",
                            "dimension": "price",
                            "lower": 42,
                            "lowerStrict": False,
                            "ordering": "numeric"
                        },
                        {
                            "type": "selector",
                            "dimension": "in_stock",
                            "value": 't'
                        }
                    ]
                },
                {
                    "type": "or",
                    "fields": [
                        {
                            "type": "selector",
                            "dimension": "category",
                            "value": "cat1"
                        },
                        {
                            "type": "selector",
                            "dimension": "category",
                            "value": "cat17"
                        },
                        {
                            "type": "selector",
                            "dimension": "category",
                            "value": "cat42"
                        }
                    ]
                },
                {
                    "type": "like",
                    "pattern": "prefix%",
                    "dimension": "name"
                }
            ]
        }
        self.assertEqual(druidry.data_source.translate_filter(input_filter), expected_value)
