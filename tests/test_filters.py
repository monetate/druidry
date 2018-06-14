from .context import druidry
import unittest


class FilterTest(unittest.TestCase):

    def test_create_invalid(self):
        with self.assertRaises(druidry.errors.DruidQueryError):
            druidry.filters.Filter('INVALID')

    def test_create(self):
        filter_ = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        self.assertEqual(filter_, {
            'type': 'selector',
            'dimension': 'is_active',
            'value': 't'
        })

    def test_negate(self):
        filter_ = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        negated_filter = filter_.negate()
        self.assertEqual(negated_filter, {
            'field': {
                'type': 'selector',
                'dimension': 'is_active',
                'value': 't'
            },
            'type': 'not'
        })

    def test_join_and(self):
        active_filter = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        browser_filter = druidry.filters.SelectorFilter(dimension='browser', value='Chrome')
        joined_filter = druidry.filters.Filter.join_filters(active_filter, browser_filter)
        self.assertEqual(joined_filter, {
            'fields': [
                {
                    'type': 'selector',
                    'dimension': 'is_active',
                    'value': 't'
                },
                {
                    'type': 'selector',
                    'dimension': 'browser',
                    'value': 'Chrome'
                }
            ],
            'type': 'and'
        })

    def test_join_or(self):
        active_filter = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        browser_filter = druidry.filters.SelectorFilter(dimension='browser', value='Chrome')
        joined_filter = druidry.filters.Filter.disjoin_filters(active_filter, browser_filter)
        self.assertEqual(joined_filter, {
            'fields': [
                {
                    'type': 'selector',
                    'dimension': 'is_active',
                    'value': 't'
                },
                {
                    'type': 'selector',
                    'dimension': 'browser',
                    'value': 'Chrome'
                }
            ],
            'type': 'or'
        })

    def test_create_selector_filter(self):
        self.assertEqual(
            druidry.filters.SelectorFilter(dimension='is_active', value='t'),
            {'type': 'selector', 'dimension': 'is_active', 'value': 't'})

    def test_create_columncomparison_filter(self):
        self.assertEqual(
            druidry.filters.ColumnComparisonFilter(dimensions=['is_active', 'is_new']),
            {
                "type": "columnComparison",
                "dimensions": [
                    "is_active",
                    "is_new"
                ]
            })

    def test_create_regex_filter(self):
        self.assertEqual(
            druidry.filters.RegexFilter(dimension='browser', pattern='Chrom[^\s]*'),
            {
                "type": "regex",
                "dimension": "browser",
                "pattern": "Chrom[^\\s]*"
            })

    def test_create_and_filter(self):
        active_filter = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        browser_filter = druidry.filters.SelectorFilter(dimension='browser', value='Chrome')
        self.assertEqual(
            druidry.filters.AndFilter(fields=[active_filter, browser_filter]),
            {
                "type": "and",
                "fields": [
                    {
                        "type": "selector",
                        "dimension": "is_active",
                        "value": "t"
                    },
                    {
                        "type": "selector",
                        "dimension": "browser",
                        "value": "Chrome"
                    }
                ]
            })

    def test_create_or_filter(self):
        active_filter = druidry.filters.SelectorFilter(dimension='is_active', value='t')
        browser_filter = druidry.filters.SelectorFilter(dimension='browser', value='Chrome')
        self.assertEqual(
            druidry.filters.OrFilter(fields=[active_filter, browser_filter]),
            {
                "type": "or",
                "fields": [
                    {
                        "type": "selector",
                        "dimension": "is_active",
                        "value": "t"
                    },
                    {
                        "type": "selector",
                        "dimension": "browser",
                        "value": "Chrome"
                    }
                ]
            })

    def test_create_not_filter(self):
        browser_filter = druidry.filters.SelectorFilter(dimension='browser', value='Chrome')
        self.assertEqual(
            druidry.filters.NotFilter(field=browser_filter),
            {
                "type": "not",
                "field": {
                    "type": "selector",
                    "dimension": "browser",
                    "value": "Chrome"
                }
            })

    def test_create_in_filter(self):
        self.assertEqual(
            druidry.filters.InFilter(dimension='browser', values=['Chrome', 'Chromium', 'Chrome Mobile']),
            {
                "type": "in",
                "dimension": "browser",
                "values": [
                    "Chrome",
                    "Chromium",
                    "Chrome Mobile"
                ]
            })

    def test_create_like_filter(self):
        self.assertEqual(
            druidry.filters.LikeFilter(dimension='browser', pattern='Chrom%'),
            {
                "type": "like",
                "dimension": "browser",
                "pattern": "Chrom%"
            })

    def test_create_bound_filter(self):
        self.assertEqual(
            druidry.filters.BoundFilter(dimension='duration', lower=0, upper=10, lower_strict=True, upper_strict=False, ordering='numeric'),
            {
                "type": "bound",
                "ordering": "numeric",
                "dimension": "duration",
                "lower": 0,
                "upper": 10,
                "lowerStrict": True,
                "upperStrict": False
            })

    def test_create_interval_filter(self):
        self.assertEqual(
            druidry.filters.IntervalFilter(dimension='time', intervals=['P1D/2014-09-27']),
            {
                "type": "interval",
                "dimension": "time",
                "intervals": [
                    "P1D/2014-09-27"
                ]
            })
