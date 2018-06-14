"""Aggregation tests."""

from .context import druidry
import unittest


class TestAggregation(unittest.TestCase):

    def test_create_invalid_type(self):
        with self.assertRaises(druidry.errors.DruidQueryError):
            druidry.aggregations.Aggregation('INVALID')

    def test_create_missing_aggregator_filtered(self):
        with self.assertRaises(druidry.errors.DruidQueryError):
            druidry.aggregations.Aggregation('filtered')

    def test_create_mathematical_aggregation(self):
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        self.assertEqual(aggregation, {
            'fieldName': 'user_count',
            'type': 'longSum',
            'name': 'users_sum'
        })

    def test_create_filtered_aggregation(self):
        aggregation = druidry.aggregations.Aggregation(
            'filtered',
            aggregator=druidry.aggregations.Aggregation('longSum', field_name='user_count', name='users_sum'),
            filter=druidry.filters.SelectorFilter(dimension='is_active', value='t'))
        self.assertEqual(aggregation, {
            'filter': {
                'type': 'selector',
                'dimension': 'is_active',
                'value': 't'
            },
            'aggregator': {
                'type': 'longSum',
                'fieldName': 'user_count',
                'name': 'users_sum'
            },
            'type': 'filtered'
        })

    def test_filter_simple_aggregation(self):
        aggregation = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum').filter(
            druidry.filters.SelectorFilter(dimension='is_active', value='t'))
        self.assertEqual(aggregation, {
            'filter': {
                'type': 'selector',
                'dimension': 'is_active',
                'value': 't'
            },
            'aggregator': {
                'type': 'longSum',
                'fieldName': 'user_count',
                'name': 'users_sum'
            },
            'type': 'filtered'
        })

    def test_filter_filtered_aggregation(self):
        filtered_aggregation = druidry.aggregations.FilteredAggregation(
            druidry.filters.SelectorFilter(dimension='is_active', value='t'),
            aggregator=druidry.aggregations.Aggregation(
                'longSum', field_name='user_count', name='users_sum'))

        twice_filtered_aggregation = filtered_aggregation.filter(
            druidry.filters.SelectorFilter(dimension='is_new_user', value='f'),
            name='active_returning_users')

        self.assertEqual(twice_filtered_aggregation, {
            'filter': {
                'fields': [
                    {
                        'type': 'selector',
                        'dimension': 'is_active',
                        'value': 't'
                    },
                    {
                        'type': 'selector',
                        'dimension': 'is_new_user',
                        'value': 'f'
                    }
                ],
                'type': 'and'
            },
            'aggregator': {
                'fieldName': 'user_count',
                'type': 'longSum',
                'name': 'active_returning_users'
            },
            'type': 'filtered',
            'name': 'active_returning_users'
        })

    def test_to_field_access(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        users_postagg = users_agg.to_field_access()
        self.assertEqual(users_postagg, {
            "fieldName": "users_sum",
            "name": "users_sum",
            "type": "fieldAccess"
        })


class TestPostAggregation(unittest.TestCase):

    def test_create_invalid_type(self):
        with self.assertRaises(druidry.errors.DruidQueryError):
            druidry.aggregations.PostAggregation('INVALID')

    def test_create_arithmetic_missing_fields(self):
        with self.assertRaises(druidry.errors.DruidQueryError):
            druidry.aggregations.PostAggregation('arithmetic')

    def test_create_field_access_post_agg(self):
        post_agg = druidry.aggregations.PostAggregation('fieldAccess', fieldName='users_sum')
        self.assertEqual(post_agg, {
            'fieldName': 'users_sum',
            'type': 'fieldAccess',
            'name': 'users_sum'
        })

    def test_create_arithmetic_post_agg(self):
        post_agg = druidry.aggregations.PostAggregation(
            'arithmetic',
            fields=[
                druidry.aggregations.PostAggregation('fieldAccess', fieldName='new_users_sum'),
                druidry.aggregations.PostAggregation('fieldAccess', fieldName='users_sum')
            ],
            fn='/', name='new_user_rate')
        self.assertEqual(post_agg, {
            'fields': [
                {
                    'fieldName': 'new_users_sum',
                    'type': 'fieldAccess',
                    'name': 'new_users_sum'
                },
                {
                    'fieldName': 'users_sum',
                    'type': 'fieldAccess',
                    'name': 'users_sum'
                }
            ],
            'type': 'arithmetic',
            'name': 'new_user_rate',
            'fn': '/'
        })

    def test_create_rate_post_agg(self):
        aggregation = druidry.aggregations.RatePostAggregation(
            name='new_user_rate',
            numerator='new_users_sum',
            denominator='users_sum')
        self.assertEqual(aggregation, {
            'fields': [
                {
                    'fieldName': 'new_users_sum',
                    'type': 'fieldAccess',
                    'name': 'new_users_sum'
                },
                {
                    'fieldName': 'users_sum',
                    'type': 'fieldAccess',
                    'name': 'users_sum'
                }
            ],
            'type': 'arithmetic',
            'name': 'new_user_rate',
            'fn': '/'
        })

    def test_agg_div(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        active_users_agg = users_agg.filter(
            druidry.filters.SelectorFilter(dimension='is_active', value='t'),
            name='active_users_sum')
        active_rate_postagg = active_users_agg / users_agg
        self.assertEqual(active_rate_postagg, {
            "fields": [
                {
                    "fieldName": "active_users_sum",
                    "type": "fieldAccess",
                    "name": "active_users_sum"
                },
                {
                    "fieldName": "users_sum",
                    "type": "fieldAccess",
                    "name": "users_sum"
                }
            ],
            "type": "arithmetic",
            "name": "active_users_sum__div__users_sum",
            "fn": "/"
        })

    def test_agg_div_constant(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        half_users_postagg = users_agg / 2
        self.assertEqual(half_users_postagg, {
            "fields": [
                {
                    "fieldName": "users_sum",
                    "type": "fieldAccess",
                    "name": "users_sum"
                },
                {
                    "name": "constant__2",
                    "type": "constant",
                    "value": 2
                }
            ],
            "type": "arithmetic",
            "name": "users_sum__div__constant__2",
            "fn": "/"
        })

    def test_agg_rdiv_constant(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        users_reciprocal = 1 / users_agg
        self.assertEqual(users_reciprocal, {
            "fields": [
                {
                    "name": "constant__1",
                    "type": "constant",
                    "value": 1
                },
                {
                    "fieldName": "users_sum",
                    "type": "fieldAccess",
                    "name": "users_sum"
                }
            ],
            "type": "arithmetic",
            "name": "constant__1__div__users_sum",
            "fn": "/"
        })

    def test_agg_add(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        phone_users_agg = users_agg.filter(
            druidry.filters.SelectorFilter(dimension='device', value='phone'),
            name='phone_users_sum')
        tablet_users_agg = users_agg.filter(
            druidry.filters.SelectorFilter(dimension='device', value='tablet'),
            name='tablet_users_sum')

        mobile_users_postagg = phone_users_agg + tablet_users_agg

        self.assertEqual(mobile_users_postagg, {
            "fields": [
                {
                    "fieldName": "phone_users_sum",
                    "type": "fieldAccess",
                    "name": "phone_users_sum"
                },
                {
                    "fieldName": "tablet_users_sum",
                    "type": "fieldAccess",
                    "name": "tablet_users_sum"
                }
            ],
            "type": "arithmetic",
            "name": "phone_users_sum__add__tablet_users_sum",
            "fn": "+"
        })

    def test_from_aggregation(self):
        users_agg = druidry.aggregations.Aggregation(
            'longSum', field_name='user_count', name='users_sum')
        users_postagg = druidry.aggregations.PostAggregation.from_aggregation(users_agg)
        self.assertEqual(users_postagg, {
            "fieldName": "users_sum",
            "name": "users_sum",
            "type": "fieldAccess"
        })


if __name__ == '__main__':
    unittest.main()
