"""
Simple wrappers around dicts for working with aggregations.

Because all of these objects are dicts, they serialize to JSON like dicts.

See http://druid.io/docs/latest/querying/aggregations.html and
http://druid.io/docs/latest/querying/post-aggregations.html
"""
from . import caseconversion
from . import typeddict
from .filters import Filter

import numbers
import sys

if sys.version_info >= (3, 0):
    basestring = str


MATHEMATICAL_AGGREGATION_TYPES = (
    'doubleMin',
    'doubleMax',
    'doubleSum',
    'hyperUnique',
    'longMin',
    'longMax',
    'longSum'
)


def remove_duplicates(*aggregations):
    """Return the unique aggregations which are unique by name."""
    return list({
        Aggregation.get_aggregation_aggregator(aggregation)['name']: aggregation
        for aggregation in aggregations
    }.values())


fn_names = {
    '/': 'div',
    '+': 'add',
    '-': 'sub',
    '*': 'mul'
}


def agg_arithmetic(fn, operand_l, operand_r, name=None):
    """Turn two aggs into field access post aggs, then combine them with specified operator."""
    return postagg_arithmetic(fn, to_postagg(operand_l), to_postagg(operand_r), name=name)


def postagg_arithmetic(fn, operand_l, operand_r, name=None):
    """Take two postaggs, return a new one that combines them with the specified operator."""
    assert fn in fn_names, 'Operator must be one of: {}'.format(fn_names.keys())
    name = name if name is not None else '{}__{}__{}'.format(
        operand_l['name'], fn_names[fn], operand_r['name'])
    return PostAggregation(
        'arithmetic', fields=[operand_l, operand_r], fn=fn, name=name)


def to_postagg(value, name=None):
    if isinstance(value, numbers.Number):
        constant_name = "constant__{}".format(value) if name is None else name
        return PostAggregation('constant', value=value, name=constant_name)
    elif isinstance(value, Aggregation):
        return value.to_field_access(name=name)
    raise ValueError('Value must be a numeric type or an Aggregation')


class Aggregation(typeddict.TypedDict):
    """
    Base class for all aggregations.

    Provides an instance method for applying a filter to an aggregation.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, type_, **kwargs):
        """
        Assign various keys from the kwargs depending on the aggregation type.

        To make Druid's implicit convenience explicit, use the fieldName as the
        name for mathematical and filtered aggregations.
        """
        self['type'] = type_
        super(Aggregation, self).__init__(type_, **kwargs)

        # http://druid.io/docs/latest/querying/aggregations.html#filtered-aggregator
        if self['type'] == 'filtered':
            name = self['aggregator']['name'] if 'name' in self['aggregator'] else self['aggregator']['fieldName']
            self['aggregator'] = dict(self['aggregator'], name=self.get('name', name))

    def _get_lookup_type(self):
        if self['type'] in MATHEMATICAL_AGGREGATION_TYPES:
            return 'mathematical'
        return self['type']

    optional_fields = {
        'filtered': {
            'name': basestring
        },
        'mathematical': {
            'name': basestring
        }
    }

    required_fields = {
        # http://druid.io/docs/latest/querying/aggregations.html#cardinality-aggregator
        'cardinality': {
            'byRow': bool,
            'fieldNames': list,
            'name': basestring
        },
        # http://druid.io/docs/latest/querying/aggregations.html#count-aggregator
        'count': {
            'name': basestring
        },
        # http://druid.io/docs/latest/querying/aggregations.html#hyperunique-aggregator
        'hyperUnique': {
            'fieldName': basestring
        },
        'filtered': {
            'filter': dict,
            'aggregator': dict
        },
        # http://druid.io/docs/latest/querying/aggregations.html#javascript-aggregator
        'javascript': {
            'fieldNames': list,
            'fnAggregate': basestring,
            'fnCombine': basestring,
            'fnReset': basestring,
            'name': basestring
        },
        # http://druid.io/docs/latest/querying/aggregations.html#sum-aggregators
        # http://druid.io/docs/latest/querying/aggregations.html#min-max-aggregators
        'mathematical': {
            'fieldName': basestring
        }
    }

    type_error_message = 'Invalid aggregation type: {type}'

    @staticmethod
    def get_aggregation_aggregator(aggregation):
        """
        Get the standard aggregation dict with type, name and fieldName properties.

        This utility method allows callers to treat a nested aggregation in a
        filtered aggregation the same as a regular aggregation. For more info,
        see http://druid.io/docs/latest/querying/aggregations.html
        """
        is_filtered = Aggregation.is_filtered_aggregation(aggregation)
        return aggregation['aggregator'] if is_filtered else aggregation

    @staticmethod
    def get_aggregation_name(aggregation):
        """Get the name for an aggregation, filtered or not."""
        return Aggregation.get_aggregation_aggregator(aggregation)['name']

    @staticmethod
    def set_aggregation_name(aggregation, name):
        """Set the name for an aggregation, filtered or not."""
        if Aggregation.is_filtered_aggregation(aggregation):
            aggregator = Aggregation.get_aggregation_aggregator(aggregation)
            return dict(aggregation, aggregator=dict(aggregator, name=name))
        return dict(aggregation, name=name)

    @staticmethod
    def is_filtered_aggregation(aggregation):
        """Return true if the aggregation is a complex filtered type."""
        return aggregation['type'] == 'filtered'

    @staticmethod
    @caseconversion.camel_case_kwargs
    def filter_aggregation(aggregation, *filters, **kwargs):
        """Apply an arbitrary number of filters to an aggregator."""
        aggregator = Aggregation.get_aggregation_aggregator(aggregation)
        filters_ = Filter.join_filters(aggregation.get('filter'), *filters)
        return FilteredAggregation(filters_, aggregator=aggregator, **kwargs)

    @caseconversion.camel_case_kwargs
    def filter(self, *filters, **kwargs):
        """Apply an arbitrary number of filters to an aggregator instance."""
        return Aggregation.filter_aggregation(self, *filters, **kwargs)

    def divide(self, other, name=None):
        return agg_arithmetic('/', self, other, name=name)

    def multiply(self, other, name=None):
        return agg_arithmetic('*', self, other, name=name)

    def add(self, other, name=None):
        return agg_arithmetic('+', self, other, name=name)

    def subtract(self, other, name=None):
        return agg_arithmetic('-', self, other, name=name)

    __div__ = lambda self, other: agg_arithmetic('/', self, other)
    __rdiv__ = lambda self, other: agg_arithmetic('/', other, self)
    __rtruediv__ = lambda self, other: agg_arithmetic('/', other, self)
    __truediv__ = lambda self, other: agg_arithmetic('/', self, other)

    __mul__ = lambda self, other: agg_arithmetic('*', self, other)
    __rmul__ = lambda self, other: agg_arithmetic('*', other, self)

    __add__ = lambda self, other: agg_arithmetic('+', self, other)
    __radd__ = lambda self, other: agg_arithmetic('+', other, self)

    __sub__ = lambda self, other: agg_arithmetic('-', self, other)
    __rsub__ = lambda self, other: agg_arithmetic('-', other, self)

    def get_aggregator(self):
        """Return the aggregator whether nested or otherwise."""
        return Aggregation.get_aggregation_aggregator(self)

    def get_name(self):
        """Return the aggregation name."""
        return Aggregation.get_aggregation_name(self)

    def is_filtered(self):
        """Return true if the aggregatio is filtered."""
        return Aggregation.is_filtered_aggregation(self)

    def set_name(self, name):
        """Return a new aggregation with the supplied name."""
        return Aggregation.set_aggregation_name(self, name)

    def to_field_access(self, name=None):
        """Create a field accessor postagg from an aggregation."""
        return PostAggregation.from_aggregation(self, name=name)


class FilteredAggregation(Aggregation):
    """An aggregation with a filter applied."""

    @caseconversion.camel_case_kwargs
    def __init__(self, filter_, **kwargs):
        """Assign to filter and delegate to superclass."""
        super(FilteredAggregation, self).__init__('filtered', filter=filter_, **kwargs)


class PostAggregation(typeddict.TypedDict):
    """Base class for post-aggregations."""

    @caseconversion.camel_case_kwargs
    def __init__(self, type_, **kwargs):
        """
        Assign various keys from the kwargs depending on the post-agg type.

        To make Druid's implicit convenience explicit, use the fieldName as the
        name for mathematical and filtered aggregations.
        """
        self['type'] = type_
        super(PostAggregation, self).__init__(type_, **kwargs)

        # http://druid.io/docs/latest/querying/aggregations.html#filtered-aggregator
        if self['type'] in ('fieldAccess', 'hyperUniqueCardinality') and not self.get('name'):
            self['name'] = self['fieldName']

    required_fields = {
        # http://druid.io/docs/latest/querying/post-aggregations.html#arithmetic-post-aggregator
        'arithmetic': {
            'name': basestring,
            'fields': list,
            'fn': basestring
        },
        # http://druid.io/docs/latest/querying/post-aggregations.html#constant-post-aggregator
        'constant': {
            'name': basestring,
            'value': None
        },
        # http://druid.io/docs/latest/querying/post-aggregations.html#field-accessor-post-aggregator
        'fieldAccess': {
            'fieldName': basestring
        },
        # http://druid.io/docs/latest/querying/post-aggregations.html#hyperunique-cardinality-post-aggregator
        'hyperUniqueCardinality': {
            'fieldName': basestring
        },
        # http://druid.io/docs/latest/querying/post-aggregations.html#javascript-post-aggregator
        'javascript': {
            'fieldNames': list,
            'function': basestring,
            'name': basestring
        }
    }

    optional_fields = {
        'fieldAccess': {
            'name': basestring
        },
        'hyperUniqueCardinality': {
            'name': basestring
        }
    }

    @staticmethod
    def from_aggregation(aggregation, name=None):
        """Create a field accessor postagg from an aggregation."""
        field_name = Aggregation.get_aggregation_name(aggregation)
        name = field_name if name is None else name
        return PostAggregation('fieldAccess', field_name=name, name=name)

    def divide(self, other, name=None):
        return postagg_arithmetic('/', self, other, name=name)

    def multiply(self, other, name=None):
        return postagg_arithmetic('*', self, other, name=name)

    def add(self, other, name=None):
        return postagg_arithmetic('+', self, other, name=name)

    def subtract(self, other, name=None):
        return postagg_arithmetic('-', self, other, name=name)

    __div__ = lambda self, other: postagg_arithmetic('/', self, other)
    __rdiv__ = lambda self, other: postagg_arithmetic('/', other, self)

    __mul__ = lambda self, other: postagg_arithmetic('*', self, other)
    __rmul__ = lambda self, other: postagg_arithmetic('*', other, self)

    __add__ = lambda self, other: postagg_arithmetic('+', self, other)
    __radd__ = lambda self, other: postagg_arithmetic('+', other, self)

    __sub__ = lambda self, other: postagg_arithmetic('-', self, other)
    __rsub__ = lambda self, other: postagg_arithmetic('-', other, self)


class RatePostAggregation(PostAggregation):
    """Convenience class for a post-agg which is the diviosn of two fields."""

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):
        """Allow field to be passed as an aggregation or as a string."""
        fields = [
            PostAggregation(
                'fieldAccess', fieldName=field) if isinstance(field, basestring) else field
            for field in [kwargs.pop('numerator'), kwargs.pop('denominator')]
        ]
        super(RatePostAggregation, self).__init__(
            'arithmetic', fn='/', fields=fields, **kwargs)
