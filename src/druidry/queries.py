"""
Container class for building query objects.

This does not validate context or dataSource on __init__, unlike the other
classes, because those are intended to be overriden later by query contexts.
"""

import json
import sys

from . import errors
from . import filters
from . import caseconversion
from . import typeddict

if sys.version_info >= (3, 0):
    basestring = str


VALID_QUERY_TYPES = (
    'dataSourceMetadata',
    'groupBy',
    'segmentMetadata',
    'timeBoundary',
    'timeseries',
    'topN',
)


class Query(typeddict.TypedDict):
    """Wrapper for a query object."""

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):
        """Get the required properties, deferring validation of dataSource."""
        self['queryType'] = kwargs['queryType']
        super(Query, self).__init__(kwargs['queryType'], **kwargs)

        if 'context' in kwargs:
            self['context'] = kwargs['context']
        if 'dataSource' in kwargs:
            self['dataSource'] = kwargs['dataSource']

    def _get_lookup_type(self):
        return self['queryType']

    required_fields = {
        'dataSourceMetadata': {},
        'timeseries': {
            'granularity': [basestring, dict],
            'aggregations': list,
            'intervals': [list, basestring]
        },
        'groupBy': {
            'dimensions': list,
            'granularity': [basestring, dict],
            'aggregations': list,
            'intervals': [list, basestring]
        },
        'scan': {
            'intervals': [list, basestring]
        },
        'segmentMetadata': {},
        'timeBoundary': {},
        'topN': {
            'aggregations': list,
            'dimension': basestring,
            'granularity': [basestring, dict],
            'metric': basestring,
            'intervals': [list, basestring],
            'threshold': int
        }
    }

    optional_fields = {
        'dataSourceMetadata': {},
        'timeseries': {
            'descending': bool,
            'filter': dict,
            'postAggregations': list
        },
        'topN': {
            'filter': dict,
            'postAggregations': list
        },
        'groupBy': {
            'filter': dict,
            'postAggregations': list,
            'having': dict,
            'limitSpec': dict
        },
        'scan': {
            'batchSize': int,
            'limit': int,
            'resultFormat': basestring,
            'columns': list
        },
        'segmentMetadata': {
            'analysisTypes': list,
            'intervals': [list, basestring],
            'lenientAggregatorMerge': bool,
            'merge': bool,
            'toInclude': list,
        },
        'timeBoundary': {
            'bound': basestring
        }
    }

    @staticmethod
    def validate_data_source(query):
        """
        Validate that the dataSource property.

        It must exist and be a string.
        """
        if not isinstance(query.get('dataSource'), basestring) and not Query.get_subquery(query):
            return 'Invalid dataSource: {}'.format(query.get('dataSource'))

    @staticmethod
    def validate_query_json(query):
        """Validate that the query is JSON serializable."""
        try:
            json.dumps(query)
        except TypeError as e:
            return 'Druid queries must be JSON serializable. {e.message}'.format(e=e)

    @staticmethod
    def validate_query_type(query):
        """Validate that the queryType is one of the valid types."""
        if query.get('queryType') not in VALID_QUERY_TYPES:
            return (
                'Invalid queryType "{query_type}". '
                'Valid query types: {query_types}'
            ).format(
                query_type=query.get('queryType'),
                query_types=', '.join(VALID_QUERY_TYPES))

    @staticmethod
    def validate_query(query):
        """Validate a query object."""
        validators = [
            Query.validate_query_json,
            Query.validate_data_source,
            Query.validate_query_type
        ]

        query_errors = [validator(query) for validator in validators if validator(query)]
        if query_errors:
            raise errors.DruidQueryError(query_errors, query=query)

    @caseconversion.camel_case_kwargs
    def extend(self, **kwargs):
        """Return a new query with new or overriden keys as specified."""
        return Query(**dict(self, **kwargs))

    @staticmethod
    def get_subquery(query):
        """Return the subquery if there is one."""
        data_source = query.get('dataSource')
        if type(data_source) == dict and data_source.get('query'):
            return data_source['query']

    def validate(self):
        """Validate a query instance."""
        return Query.validate_query(self)


class DataSourceMetadataQuery(Query):
    """
    Convenience class for creating a dataSourceMetadata query.

    See http://druid.io/docs/latest/querying/datasourcemetadataquery.html

    No required or optional arguments.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):  # NOQA
        super(DataSourceMetadataQuery, self).__init__(query_type='dataSourceMetadata', **kwargs)


class SegmentMetadataQuery(Query):
    """
    Convenience class for creating a segmentMetadata query.

    See http://druid.io/docs/latest/querying/segmentmetadataquery.html

    No required arguments.

    Optional keyword arguments:
        analysisTypes: list
        intervals: list or string
        lenientAggregatorMerge: bool
        merge: bool
        toInclude: list
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):  # NOQA
        super(SegmentMetadataQuery, self).__init__(query_type='segmentMetadata', **kwargs)


class DataQuery(Query):

    def filter(self, filter_):
        existing = self.pop('filter', None)
        if existing is not None:
            new_filter = filters.Filter.join_filters(existing, filter_)
        else:
            new_filter = filter_
        return self.extend(filter=new_filter)

    @classmethod
    def filter_query(cls, query, filter_):
        new_query = query.copy()
        existing = new_query.pop('filter', None)
        if existing is not None:
            new_filter = filters.Filter.join_filters(existing, filter_)
        else:
            new_filter = filter_
        return cls(filter=new_filter, **new_query)


class TimeseriesQuery(DataQuery):
    """
    Convenience class for creating a timeseries query.

    See http://druid.io/docs/latest/querying/timeseriesquery.html

    Required keyword arguments:
        aggregations: list of dicts (see druidry.aggregations)
            Specifications of the data to aggregate in the result.
            See http://druid.io/docs/latest/querying/aggregations.html
        granularity: dict or string (see druidry.granularities)
            The time duration covered by each result bucket.
            See http://druid.io/docs/latest/querying/granularities.html
        intervals: one or more strings or interval objects (see druidry.intervals)
            ISO-8601 time interval string or druidry.intervals object.

    Optional keyword arguments:
        descending: bool
            If supplied and true, results will be reverse chronological order.
        filter: dict
            Specifications of the data to include or omit from the result.
            See http://druid.io/docs/latest/querying/filters.html
        post_aggregations: list
            Specifications of the data to aggregate in the result.
            Can reference aggregations.
            See http://druid.io/docs/latest/querying/post-aggregations.html
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, aggregations=None, granularity=None, intervals=None, **kwargs):
        if not isinstance(aggregations, list):
            raise ValueError("`aggregations` is required to be a list")
        if not isinstance(granularity, basestring) and not isinstance(granularity, dict):
            raise ValueError("`granularity` is required to be a string")
        if not isinstance(intervals, basestring) and not isinstance(intervals, list):
            raise ValueError("`intervals` must be a string or a list of strings")
        super(TimeseriesQuery, self).__init__(
            query_type='timeseries', aggregations=aggregations,
            granularity=granularity, intervals=intervals, **kwargs)

    __init__.__doc__ = __doc__


class GroupByQuery(DataQuery):
    """
    Convenience class for creating a groupBy query.

    See http://druid.io/docs/latest/querying/groupbyquery.html

    Required keyword arguments:
        aggregations: list of dicts (see druidry.aggregations)
            Specifications of the data to aggregate in the result.
            See http://druid.io/docs/latest/querying/aggregations.html
        dimensions: list of strings
            The dimensions by which to group.
        granularity: dict or string (see druidry.granularities)
            The time duration covered by each result bucket.
            See http://druid.io/docs/latest/querying/granularities.html
        intervals: one or more strings or interval objects (see druidry.intervals)
            ISO-8601 time interval string or druidry.intervals object.

    Optional keyword arguments:
        descending: bool
            If supplied and true, results will be reverse chronological order.
        filter: dict
            Specifications of the data to include or omit from the result.
            See http://druid.io/docs/latest/querying/filters.html
        having: dict
            Like filter, but may reference aggregated fields.
            See http://druid.io/docs/latest/querying/having.html
        limitSpec: dict
            Specification of how to limit the returned results.
            See http://druid.io/docs/latest/querying/limitspec.html
        post_aggregations: list
            Specifications of the data to aggregate in the result.
            Can reference aggregations.
            See http://druid.io/docs/latest/querying/post-aggregations.html
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, aggregations=None, dimensions=None, granularity=None, intervals=None, **kwargs):
        if not isinstance(aggregations, list):
            raise ValueError("`aggregations` is required to be a list")
        if not isinstance(dimensions, list):
            raise ValueError("`dimensions` is required to be a list")
        if not isinstance(granularity, basestring) and not isinstance(granularity, dict):
            raise ValueError("`granularity` is required to be a string")
        if not isinstance(intervals, basestring) and not isinstance(intervals, list):
            raise ValueError("`intervals` must be a string or a list of strings")
        super(GroupByQuery, self).__init__(
            query_type='groupBy', aggregations=aggregations, dimensions=dimensions,
            granularity=granularity, intervals=intervals, **kwargs)

    __init__.__doc__ = __doc__


class ScanQuery(Query):
    """
    Convenience class for creating a scab query.

    See http://druid.io/docs/latest/querying/scanquery.html

    Required keyword arguments:
        intervals: one or more strings or interval objects (see druidry.intervals)
            ISO-8601 time interval string or druidry.intervals object.

    Optional keyword arguments:
        filter: dict
            Specifications of the data to include or omit from the result.
            See http://druid.io/docs/latest/querying/filters.html
        post_aggregations: list
            Specifications of the data to aggregate in the result.
            Can reference aggregations.
            See http://druid.io/docs/latest/querying/post-aggregations.html
    """

    @caseconversion.camel_case_kwargs
    def __init__(
            self, intervals=None, **kwargs):
        if not isinstance(intervals, basestring) and not isinstance(intervals, list):
            raise ValueError("`intervals` must be a string or a list of strings")
        super(ScanQuery, self).__init__(
            query_type='scan', intervals=intervals, **kwargs)


class TimeBoundaryQuery(Query):
    """
    Convenience class for creating a timeBoundary query.

    See http://druid.io/docs/latest/querying/timeboundaryquery.html

    No required arguments.

    Optional arguments:
        bound: string
            Either 'maxTime' or 'minTime'.
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):  # NOQA
        super(TimeBoundaryQuery, self).__init__(query_type='timeBoundary', **kwargs)


class TopNQuery(DataQuery):
    """
    Convenience class for creating a groupBy query.

    See http://druid.io/docs/latest/querying/topnquery.html

    Required keyword arguments:
        aggregations: list of dicts (see druidry.aggregations)
            Specifications of the data to aggregate in the result.
            See http://druid.io/docs/latest/querying/aggregations.html
        dimension: string
            The dimension for which the top is taken.
            See http://druid.io/docs/latest/querying/dimensionspecs.html
        granularity: dict or string (see druidry.granularities)
            The time duration covered by each result bucket.
            See http://druid.io/docs/latest/querying/granularities.html
        intervals: one or more strings or interval objects (see druidry.intervals)
            ISO-8601 time interval string or druidry.intervals object.
        metric: string
            The metric by which to rank the top N.
        threshold: integer
            How many top results ordered by metric.

    Optional keyword arguments:
        filter: dict
            Specifications of the data to include or omit from the result.
            See http://druid.io/docs/latest/querying/filters.html
        post_aggregations: list
            Specifications of the data to aggregate in the result.
            Can reference aggregations.
            See http://druid.io/docs/latest/querying/post-aggregations.html
    """

    @caseconversion.camel_case_kwargs
    def __init__(
            self, aggregations=None, dimension=None, granularity=None,
            intervals=None, metric=None, threshold=None, **kwargs):
        if not isinstance(aggregations, list):
            raise ValueError("`aggregations` is required to be a list")
        if not isinstance(dimension, basestring):
            raise ValueError("`dimension` is required to be a string")
        if not isinstance(granularity, basestring) and not isinstance(granularity, dict):
            raise ValueError("`granularity` is required to be a string")
        if not isinstance(intervals, basestring) and not isinstance(intervals, list):
            raise ValueError("`intervals` must be a string or a list of strings")
        if not isinstance(metric, basestring):
            raise ValueError("`metric` must be a string")
        if not isinstance(threshold, int):
            raise ValueError("`threshold` must be an int")
        super(TopNQuery, self).__init__(
            query_type='topN', aggregations=aggregations, dimension=dimension,
            granularity=granularity, intervals=intervals, metric=metric,
            threshold=threshold, **kwargs)

    __init__.__doc__ = __doc__
