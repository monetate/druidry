"""
QueryContext facilitates the creation of thread-local overrides to executed queries.

Within a `with` block of a query context, the provided function will be used to
pre-process all queries before they're excuted.

QueryContexts use Ordered dict so that they are applied in the order that they
are assigned.
"""
import collections
import functools
import threading

from .filters import Filter
from .intervals import Interval
from .queries import Query
from . import granularities


thread_context = threading.local()

DRUID_QUERY_CONTEXT_KEY = 'druid_query_context'


def get_query_processors():
    """Get the thread-local query context processors."""
    query_context = getattr(thread_context, DRUID_QUERY_CONTEXT_KEY, None)
    if query_context is None:
        query_context = collections.OrderedDict()
        setattr(thread_context, DRUID_QUERY_CONTEXT_KEY, query_context)
    return query_context


class QueryContext(object):
    """QueryContext assigns a pre-processor for all queries executed in a thread."""

    def __init__(self, context_key, context_func):
        """Assign the params to the instance, but don't bind them to the local thread yet."""
        self.context_key = context_key
        self.context_func = context_func

    def __enter__(self):
        """
        When we've entered the with block, we assign to the local thread.

        It will fail upon duplicate query context keys.
        """
        processors = get_query_processors()
        if self.context_key in processors:
            raise Exception(
                'Duplicate context key: {c.context_key}'.format(c=self))

        processors[self.context_key] = self.context_func

    def __exit__(self, *args):
        """Clean up by removing the context key from the local thread."""
        get_query_processors().pop(self.context_key)


def process_query(query):
    """Apply the processors to the query and return the result."""
    processors = get_query_processors().values()
    processor = functools.reduce(lambda f, g: lambda v: f(g(v)), processors, lambda x: x)
    return Query.wrap(processor(query))


def data_source_context(data_source):
    """Convenience function for creating a query context that dictates a dataSource."""
    def add_data_source(query):
        provided_data_source = query.get('dataSource')
        if type(provided_data_source) == dict and provided_data_source.get('type') == 'query':
            query['dataSource']['query']['dataSource'] = data_source
        else:
            query['dataSource'] = data_source
        return query
    return QueryContext('dataSource', add_data_source)


def query_filter_context(filter_name, filter_, **kwargs):
    """Convenience function for creating a query context that applies a filter."""
    return QueryContext(filter_name, lambda query: query.extend(
        filter=Filter.join_filters(query.get('filter'), filter_)), **kwargs)


def timeout_context(timeout=0, **kwargs):
    """Convenience function for create a query context that dictates a timeout."""
    return QueryContext('timeout', lambda query: query.extend(
        context=dict(query.get('context', {}), timeout=timeout)), **kwargs)


def _pad_query_interval(query):
    delta = granularities.granularity_to_timedelta(query['granularity'])
    if delta is None:
        return query

    if type(query['intervals']) == list:
        intervals = [
            Interval.pad_interval_by_timedelta(interval, delta)
            for interval in query['intervals']
        ]
    else:
        intervals = Interval.pad_interval_by_timedelta(query['intervals'], delta)

    return Query.extend(query, intervals=intervals)


def pad_query_interval_context():
    """Create a query context that pads that interval by the granularity."""
    return QueryContext('interval-padding', _pad_query_interval)
