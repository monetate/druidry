"""
Wrappers around dicts for working with filters.

Because all of these objects are dicts, they serialize to JSON like dicts.
"""
import numbers
import sys

from . import caseconversion
from . import typeddict

if sys.version_info >= (3, 0):
    basestring = str


class Filter(typeddict.TypedDict):
    """
    Base class for all aggregations.

    Provides instance methods for negating and combining filters.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, type, **kwargs):
        """Require different keys on the filter depending on the type."""
        self['type'] = type
        super(Filter, self).__init__(type, **kwargs)

    required_fields = {
        'and': {
            'fields': list
        },
        'bound': {
            'dimension': basestring
        },
        'columnComparison': {
            'dimensions': list
        },
        'in': {
            'dimension': basestring,
            'values': list
        },
        'interval': {
            'dimension': basestring,
            'intervals': list
        },
        'javascript': {
            'dimension': basestring,
            'function': basestring
        },
        'like': {
            'dimension': basestring,
            'pattern': basestring
        },
        'not': {
            'field': dict
        },
        'or': {
            'fields': list
        },
        'regex': {
            'dimension': basestring,
            'pattern': basestring
        },
        'search': {
            'dimension': basestring,
            'query': dict
        },
        'selector': {
            'dimension': basestring,
            'value': None
        }
    }

    optional_fields = {
        'bound': {
            'extractionFn': dict,
            'ordering': basestring,
            'lower': (basestring, numbers.Number),
            'lowerStrict': bool,
            'upper': (basestring, numbers.Number),
            'upperStrict': bool
        },
        'like': {
            'escape': basestring,
            'extractionFn': dict
        },
        'selector': {
            'extractionFn': dict
        }
    }

    @staticmethod
    def negate_filter(filter_):
        """Negate a filter."""
        return Filter('not', field=filter_)

    @staticmethod
    def combine_filters(type_, *filters):
        """Take a variable number of filters and join them."""
        fields = [filter_ for filter_ in filters if filter_]
        if len(fields) == 1:
            return fields[0]
        return Filter(type_, fields=fields)

    @staticmethod
    def join_filters(*filters):
        """Take a variable number of filters and join them with an and."""
        return Filter.combine_filters('and', *filters)

    @staticmethod
    def disjoin_filters(*filters):
        """Take a variable number of filters and join them with an and."""
        return Filter.combine_filters('or', *filters)

    def negate(self):
        """Negate a filter instance."""
        return Filter.negate_filter(self)


class SelectorFilter(Filter):
    """
    Convenience class for creating a selector filter.

    See http://druid.io/docs/latest/querying/filters.html#selector-filter

    Required keyword arguments:
        dimenson: string
            The dimension to select.
        value: any type (depending upon dimension)
            The value to select or the extraction function.
            See http://druid.io/docs/latest/querying/filters.html#filtering-with-extraction-functions.
    Optional keyword arguments:
        extractionFn: dict
            Extraction function to apply to the dimension
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, value=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        super(SelectorFilter, self).__init__(
            type='selector', dimension=dimension, value=value, **kwargs)

    __init__.__doc__ = __doc__


class ColumnComparisonFilter(Filter):
    """
    Convenience class for creating a column comparison filter.

    See http://druid.io/docs/latest/querying/filters.html#column-comparison-filter

    Required keyword arguments:
        dimensons: list
            The (two) dimensions to compare.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimensions=None, **kwargs):
        if not isinstance(dimensions, list) or len(dimensions) != 2:
            raise ValueError("`dimensions` is required to be a 2-length list")
        super(ColumnComparisonFilter, self).__init__(
            type='columnComparison', dimensions=dimensions, **kwargs)

    __init__.__doc__ = __doc__


class RegexFilter(Filter):
    """
    Convenience class for creating a regular expression filter.

    See http://druid.io/docs/latest/querying/filters.html#regular-expression-filter

    Required keyword arguments:
        dimenson: str
            The (two) dimensions to compare.
        pattern: str
            The pattern to match.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, pattern=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        if not isinstance(pattern, basestring):
            raise ValueError("`pattern` is required to be a string")
        super(RegexFilter, self).__init__(
            type='regex', dimension=dimension, pattern=pattern, **kwargs)

    __init__.__doc__ = __doc__


class AndFilter(Filter):
    """
    Convenience class for creating an and filter.

    See http://druid.io/docs/latest/querying/filters.html#and

    Required keyword arguments:
        fields: list
            The fields to and-join
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, fields=None, **kwargs):
        if not isinstance(fields, list):
            raise ValueError("`fields` is required to be a list")
        super(AndFilter, self).__init__(
            type='and', fields=fields, **kwargs)

    __init__.__doc__ = __doc__


class OrFilter(Filter):
    """
    Convenience class for creating an or filter.

    See http://druid.io/docs/latest/querying/filters.html#or

    Required keyword arguments:
        fields: list
            The fields to or-join
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, fields=None, **kwargs):
        if not isinstance(fields, list):
            raise ValueError("`fields` is required to be a list")
        super(OrFilter, self).__init__(
            type='or', fields=fields, **kwargs)

    __init__.__doc__ = __doc__


class NotFilter(Filter):
    """
    Convenience class for creating a not filter.

    See http://druid.io/docs/latest/querying/filters.html#not

    Required keyword arguments:
        field: dict or Filter
            The field to negate
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, field=None, **kwargs):
        if not isinstance(field, dict):
            raise ValueError("`field` is required to be a dict or Filter")
        super(NotFilter, self).__init__(
            type='not', field=field, **kwargs)

    __init__.__doc__ = __doc__


class InFilter(Filter):
    """
    Convenience class for creating an in filter.

    See http://druid.io/docs/latest/querying/filters.html#in

    Required keyword arguments:
        dimenson: string
            The dimension to select.
        values: list
            The values to select or the extraction functions.
            See http://druid.io/docs/latest/querying/filters.html#filtering-with-extraction-functions.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, values=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        if values is None:
            raise ValueError("`values` cannot be None")
        super(InFilter, self).__init__(
            type='in', dimension=dimension, values=values, **kwargs)

    __init__.__doc__ = __doc__


class LikeFilter(Filter):
    """
    Convenience class for creating a like filter.

    See http://druid.io/docs/latest/querying/filters.html#like-filter

    Required keyword arguments:
        dimenson: str
            The (two) dimensions to compare.
        pattern: str
            The pattern to match.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, pattern=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        if not isinstance(pattern, basestring):
            raise ValueError("`pattern` is required to be a string")
        super(LikeFilter, self).__init__(
            type='like', dimension=dimension, pattern=pattern, **kwargs)

    __init__.__doc__ = __doc__


class BoundFilter(Filter):
    """
    Convenience class for creating a bound filter.

    See http://druid.io/docs/latest/querying/filters.html#bound-filter

    Required keyword arguments:
        dimenson: string or number
            The (two) dimensions to compare.
    Optional keyword arguments:
        lower: string or number
            The lower bound for the filter
        upper: string or number
            The upper bound for the filter
            upper   String  The upper bound for the filter  no
        lowerStrict: boolean
            Whether to use strict equality, ie., > vs >=
        upperStrict: boolean
            Whether to use strict equality, ie., < vs <=
        ordering: string
            One of "lexicographic", "alphanumeric", "numeric", "strlen"
        extractionFn: dict
            Extraction function to apply to the dimension
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, pattern=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        super(BoundFilter, self).__init__(
            type='bound', dimension=dimension, **kwargs)

    __init__.__doc__ = __doc__


class IntervalFilter(Filter):
    """
    Convenience class for creating an in filter.

    See http://druid.io/docs/latest/querying/filters.html#interval-filter

    Required keyword arguments:
        dimenson: string
            The dimension to select.
        intervals: list
            The intervals to select.
    Optional keyword arguments:
        extractionFn: dict
            Extraction function to apply to the dimension
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, dimension=None, intervals=None, **kwargs):
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        if not isinstance(intervals, list):
            raise ValueError("`intervals` is required to be a list")
        super(IntervalFilter, self).__init__(
            type='interval', dimension=dimension, intervals=intervals, **kwargs)

    __init__.__doc__ = __doc__
