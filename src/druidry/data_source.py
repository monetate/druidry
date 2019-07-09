"""
Classes for describing a data source to facilitate building queries against it.

Provided in this module:

Dimension
    For modeling a dimension (those used to filter and group a query) of a data
    source. Facilitates creation of selector and alphanumeric-ordered filters.
NumericDimension
    For modeling a numeric dimension of a data source. Facilitates creation of
    numeric-ordered bound filters.
CategoricalDimension
    For modeling text dimensions with some provided categories. Facilitates
    creation of filters for each provided value and filters for values not
    matching the provided categories (eg other) as well as functionality for
    naming the provided values.
GroupedCategoricalDimension
    For modeling text dimensions in which several multiple provided categories
    can be grouped togethe. Facilitates the creation of joined or-filters on
    those groups of values.
Metric
    For modeling a metric (those used to aggregate and return query results) of
    a data source.
ComplexMetric
    For modeling metrics that result from multiple aggregations and/or
    post-aggregations. Facilitates building complex queries.
DataSourceView
    An abstract class a la django.db.models.Model. When populated with
    dimensions and metrics, provides a simplified interface for building
    queries.
"""
from . import aggregations
from .filters import Filter, AndFilter, BoundFilter, ColumnComparisonFilter, LikeFilter, OrFilter, SelectorFilter, NotFilter
from .intervals import Interval
from .queries import GroupByQuery, TimeseriesQuery
from .results import QueryResult

import datetime
import itertools
import numbers


DEFAULT_SEPARATOR = '___'


class Dimension(object):

    def __init__(
            self, dimension=None, name=None, ranges=None,
            name_separator=None, name_prefix=None, name_suffix=None,
            is_multi_valued=False, can_split=True):
        if dimension is None:
            raise ValueError('The keyword argument `dimension` must not be None.')

        self.can_split = can_split
        self.dimension = dimension
        self.is_multi_valued = is_multi_valued
        self.name = self.dimension if name is None else name
        self.ranges = ranges

        self.name_separator = DEFAULT_SEPARATOR if name_separator is None else name_separator
        self.name_prefix = self.dimension if name_prefix is None else name_prefix
        self.name_suffix = '' if name_suffix is None else name_suffix

    def create_bound(
            self, lower=None, upper=None, lower_strict=False,
            upper_strict=False, ordering='alphanumeric'):
        return BoundFilter(
            dimension=self.dimension, lower=lower, upper=upper,
            lower_strict=lower_strict, upper_strict=upper_strict,
            ordering=ordering)

    def format_filter_name(self, choice, operator='eq'):
        return (
            '{self.name_prefix}{self.name_separator}'
            '{operator}{self.name_separator}'
            '{choice}{self.name_suffix}'
        ).format(self=self, choice=choice, operator=operator)

    def create_selector(self, value):
        return SelectorFilter(dimension=self.dimension, value=value)

    def range_selectors(self):
        if self.ranges is None:
            return []
        return [
            (
                (lower, upper),
                (
                    self.format_filter_name(','.join((lower, upper)), operator='in'),
                    self.create_bound(lower=lower, upper=upper, upper_strict=True)
                )
            )
            for lower, upper in self.ranges
        ]

    @property
    def config(self):
        return {
            'allow_multiple': False,
            'can_split': self.can_split,
            'choices': None,
            'dimension': self.dimension,
            'name': self.name,
            'type': self.type
        }


class NumericDimension(Dimension):

    type = 'numeric'

    def create_bound(self, **kwargs):
        return super(NumericDimension, self).create_bound(ordering='numeric', **kwargs)


class CategoricalDimension(Dimension):

    def __init__(self, choices=None, has_other_choice=None, allow_multiple=True, **kwargs):
        self._choices = choices
        self.allow_multiple = allow_multiple
        self.has_other_choice = has_other_choice
        super(CategoricalDimension, self).__init__(**kwargs)

    type = 'categorical'

    @property
    def choices(self):
        choices = self._choices() if callable(self._choices) else self._choices
        return choices.keys() if type(choices) == dict else choices

    @property
    def choice_labels(self):
        choices = self._choices() if callable(self._choices) else self._choices
        if type(choices) == dict:
            return choices
        return {choice: choice for choice in choices}

    @property
    def choice_values(self):
        return self.choices

    @property
    def config(self):
        base_config = Dimension.config.fget(self)
        choices = [
            {
                'value': key,
                'label': self.choice_labels.get(key, key)
            }
            for key in self.all_choices
        ]
        return dict(base_config, allow_multiple=self.allow_multiple, choices=choices)

    @property
    def choice_selectors(self):
        return [choice for choice in self.choices if choice != 'other']

    @property
    def other_choice_selectors(self):
        if self.has_other_choice:
            return ['other']
        return []

    @property
    def all_choices(self):
        if self.choices:
            return sorted(self.choice_selectors + self.other_choice_selectors)
        return []

    def select_all_choices(self):
        return OrFilter(fields=[
            self.create_selector(value)
            for value in self.all_choices
        ])


class GroupedCategoricalDimension(CategoricalDimension):

    def __init__(self, **kwargs):
        super(GroupedCategoricalDimension, self).__init__(**kwargs)

    @property
    def choices(self):
        choices = self._choices()
        return [k for k, v in choices.items()]

    @property
    def choice_labels(self):
        choices = self._choices()
        return {k: v['label'] for k, v in choices.items()}

    @property
    def choice_values(self):
        choices = self._choices()
        return {k: v['group'] for k, v in choices.items()}

    @property
    def choice_selectors(self):
        return [choice for choice in self.choices]

    def create_selector(self, key):
        return OrFilter([
            SelectorFilter(dimension=self.dimension, value=value)
            for value in self.choice_values[key]
        ])

    @property
    def config(self):
        base_config = CategoricalDimension.config.fget(self)
        choices = [
            {
                'value': self.choice_values[key],
                'label': self.choice_labels[key]
            }
            for key in self.all_choices
        ]
        return dict(
            base_config, choices=choices,
            allow_multiple=self.allow_multiple)


class Metric(object):
    def __init__(self, metric):
        self.metric = metric


class ComplexMetric(Metric):

    def __init__(self, metric, aggregations=None, name=None, post_aggregations=None, unit=None):
        self.metric = metric
        self.name = metric if name is None else name
        self.aggregations = [] if aggregations is None else aggregations
        self.post_aggregations = [] if post_aggregations is None else post_aggregations
        self.unit = unit

    @property
    def config(self):
        return {
            'metric': self.metric,
            'name': self.name,
            'unit': self.unit
        }


def equality_filter(f):
    left, right = f['left'], f['right']
    if left['type'] == 'value' and right['type'] == 'value':
        raise ValueError('Druid does not support constant comparisons.')
    if left['type'] == 'field' and right['type'] == 'field':
        result = ColumnComparisonFilter(dimensions=[left['field'], right['field']])
    elif left['type'] == 'field' and right['type'] == 'value':
        result = SelectorFilter(dimension=left['field'], value=right['value'])
    elif left['type'] == 'value' and right['type'] == 'field':
        result = SelectorFilter(dimension=right['field'], value=left['value'])
    else:
        raise ValueError('Invalid filter.')
    return result if f['type'] == '==' else result.negate()


def inequality_filter(f):
    left, right = f['left'], f['right']
    if left['type'] == 'field' and right['type'] == 'field':
        raise ValueError('Druid does not support column-comparison inequalities.')
    if left['type'] == 'value' and right['type'] == 'value':
        raise ValueError('Druid does not support constant comparisons.')

    value_side, field_side = ('left', 'right') if left['type'] == 'value' else ('right', 'left')
    value, field = f[value_side]['value'], f[field_side]['field']

    ordering = 'numeric' if isinstance(value, numbers.Number) else 'alphanumeric'
    strictness = f['type'] in ('<', '>')

    is_left = field_side == 'left'
    is_lower = f['type'] in ('<', '<=')
    if (is_lower and is_left) or (not is_lower and not is_left):
        return BoundFilter(
            dimension=field, upper=value, ordering=ordering, upper_strict=strictness)
    else:
        return BoundFilter(
            dimension=field, lower=value, ordering=ordering, lower_strict=strictness)


def contains_filter(f):
    if f['left']['type'] != 'field' or f['right']['type'] != 'value':
        raise ValueError('Druid does not support dynamic containment checks.')
    fields = [
        SelectorFilter(
            dimension=f['left']['field'],
            value=value)
        for value in f['right']['value']
    ]
    if len(fields) == 0:
        return None
    if len(fields) == 1:
        filter_ = fields[0]
    else:
        filter_ = OrFilter(fields=fields)

    if f['type'] == 'in':
        return filter_
    else:
        return filter_.negate()


def like_filter(f):
    if f['left']['type'] != 'field' or f['right']['type'] != 'value':
        raise ValueError('Druid does not support dynamic like patterns.')
    affix_start = f['type'] == 'startswith'
    pattern = ('{}%' if affix_start else '%{}').format(f['right']['value'])
    return LikeFilter(
        dimension=f['left']['field'], pattern=pattern)


def combine_filter(f):
    filters = [translate_filter(sf) for sf in f['filters'] if translate_filter(sf)]
    if not filters:
        return None
    if f['type'] == 'and':
        return AndFilter(fields=filters)
    else:
        return OrFilter(fields=filters)


def negate_filter(f):
    translated_filter = translate_filter(f['filter'])
    if not translated_filter:
        return None
    return NotFilter(field=translated_filter)


FILTER_TYPES = {
    '==': equality_filter,
    '!=': equality_filter,
    '>=': inequality_filter,
    '>': inequality_filter,
    '<': inequality_filter,
    '<=': inequality_filter,
    'in': contains_filter,
    'not in': contains_filter,
    'endwith': like_filter,
    'startswith': like_filter,
    'and': combine_filter,
    'or': combine_filter,
    'not': negate_filter
}


def translate_filter(f):
    if f is None:
        return f
    return FILTER_TYPES[f['type']](f)


class DataSourceView(object):

    def __init__(self):
        pass

    @classmethod
    def dimensions(cls):
        items = [getattr(cls, attr) for attr in dir(cls)]
        return [item for item in items if isinstance(item, Dimension)]

    @classmethod
    def metrics(cls):
        items = [getattr(cls, attr) for attr in dir(cls)]
        return [item for item in items if isinstance(item, Metric)]

    @classmethod
    def aggregations(cls):
        items = [getattr(cls, attr) for attr in dir(cls)]
        return [item for item in items if isinstance(item, aggregations.Aggregation)]

    @classmethod
    def post_aggregations(cls):
        items = [getattr(cls, attr) for attr in dir(cls)]
        return [item for item in items if isinstance(item, aggregations.PostAggregation)]

    def get_filters(self, filters):
        return translate_filter(filters)

    @property
    def config(self):
        dimensions = {d.dimension: d.config for d in self.dimensions()}
        metrics = {m.metric: m.config for m in self.metrics()}
        return {'dimensions': dimensions, 'metrics': metrics}

    def get_metric(self, name):
        return next((m for m in self.metrics() if m.metric == name), None)

    def get_dimension(self, name):
        return next((d for d in self.dimensions() if d.dimension == name), None)

    def get_aggregations(self, metrics):
        metrics_aggs = itertools.chain.from_iterable(
            self.get_metric(m).aggregations for m in metrics)

        return sorted(
            aggregations.remove_duplicates(*metrics_aggs),
            key=aggregations.Aggregation.get_aggregation_name)

    def get_post_aggregations(self, metrics):
        metrics_postaggs = itertools.chain.from_iterable(
            self.get_metric(m).post_aggregations for m in metrics)

        return sorted(
            aggregations.remove_duplicates(*metrics_postaggs),
            key=aggregations.Aggregation.get_aggregation_name)

    def get_filter_for_dimension_choices(self, dimension):
        return OrFilter(fields=[
            dimension.create_selector(choice)
            for choice in dimension.choices
        ])

    def get_split_exclusion_filter(self, splits):
        dimensions = [self.get_dimension(split) for split in splits]
        if dimensions:
            return AndFilter(fields=[
                self.get_filter_for_dimension_choices(dimension)
                for dimension in dimensions
            ])
        return None

    def get_query(
            self, end=None, start=None, metrics=None, splits=None,
            duration=None, granularity='all', filters=None, intervals=None,
            exclude_other_splits=False):

        filter_ = self.get_filters(filters)

        split_exclusion_filter = None
        if splits and exclude_other_splits:
            split_exclusion_filter = self.get_split_exclusion_filter(splits)

        if split_exclusion_filter and filter_:
            filter_ = AndFilter(fields=[filter_, split_exclusion_filter])
        elif split_exclusion_filter:
            filter_ = split_exclusion_filter

        aggs = self.get_aggregations(metrics)
        postaggs = self.get_post_aggregations(metrics)

        if intervals is None:
            if end is None:
                end = datetime.datetime.now()
            if start is None:
                intervals = Interval(end=end, **duration)
            else:
                intervals = Interval(end=end, start=start)

        kwargs = {
            'aggregations': aggs,
            'granularity': granularity,
            'intervals': intervals,
            'post_aggregations': postaggs
        }

        query = GroupByQuery(dimensions=splits, **kwargs) if splits else TimeseriesQuery(**kwargs)
        return query.filter(filter_) if filter_ else query

    def filter_result(self, result, metric):
        return {k: v for k, v in result.items() if k.startswith(metric)}

    def execute_query(self, executor, **kwargs):
        query = self.get_query(**kwargs)
        result = executor(query)
        return QueryResult(query, result)
