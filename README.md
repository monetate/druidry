# Druidry

`druidry` is a lightweight Python library for building and executing [Druid](druid.io) queries.

Its focus is on providing early feedback to the user when they've structured a query incorrectly, and on providing convenience methods for composing the components of a query (eg., filtering an aggregation or joining multiple filters).

All query-building classes, like `Query` or `Aggregation`, subclass `dict`, making it easy to gradually replace existing code with the query builder.

## Why would I use `druidry` instead of `pydruid`?

The Druid maintainers graciously provide [Python bindings](https://github.com/druid-io/pydruid). `druidry` has a different philosophy than `pydruid`: it aims to be more pythonic, to facilitate the gradual replacement of existing query-building code, to map as closely as possible to the JSON query format, and to solve common pain points around interval and granularity logic which were unaddressed in `pydruid`.

```python
import druidry
```

## client

`druidry.client.Client` enables users to configure a connection to a Druid broker, request the metadata from that broker and execute queries against.

Only the first two arguments, `host` and `port`, are required.


```python
client = druidry.client.Client('localhost', '8080')

client.endpoint
# http://localhost:8080/druid/v2
```


If `data_source` is supplied, the `fetch_schema` method can be used to get the dimensions and metrics of the `data_source`.


```python
client_with_data_source = druidry.client.Client(
    DRUID_BROKER_HOST, '8080', data_source=DATA_SOURCE)

client_with_data_source.fetch_schema()
# ([u'browser', u'os', u'device'], [u'count', u'length_of_visit'])
```

Likewise, one can pass `fetch_schema=True` to populate those properties on `client` off the bat.


```python
client_with_data_source = druidry.client.Client(
    DRUID_BROKER_HOST, '8080', data_source=DATA_SOURCE, fetch_schema=True)

# Equivalent to above
client_with_data_source.dimensions, client_with_data_source.metrics
# ([u'browser', u'os', u'device'], [u'count', u'length_of_visit'])
```

Finally, one can pass an integer as the `timeout` keyword argument to wrap all queries in a timeout.


```python
client_with_timeout = druidry.client.Client(
    DRUID_BROKER_HOST, '8080', timeout=10)

client_with_timeout.timeout
# 10
```


`client.Client` objects have a method called `execute_query` which processes all the `context`s, validates the passed query, executes the query and validates the response.


```python
client_with_data_source.execute_query({'queryType': 'timeBoundary'})
# [{u'result': {u'maxTime': u'2016-08-17T14:19:00.000Z',
#       u'minTime': u'2016-08-10T15:00:00.000Z'},
#       u'timestamp': u'2016-08-10T15:00:00.000Z'}]
```


```python
try:
    client_with_data_source.execute_query({'queryType': 'NOT_A_REAL_QUERY_TYPE'})
except druidry.errors.DruidQueryError as e:
    print("Unsurprisingly, that is not a real query type.")

# Unsurprisingly, that is not a real query type.
```

## queries

The `druidry.queries.Query` class helps in building queries by converting the case of its keyword arguments and performing validation.

```python
query = druidry.queries.Query(query_type='timeBoundary')
```

Camel case or snake case both work.

```python
query_1 = druidry.queries.Query(query_type='timeBoundary')
query_2 = druidry.queries.Query(queryType='timeBoundary')

print('Are the queries equal?', query_1 == query_2)
# Are the queries equal? True
```

Convenience classes exist for the following query types: `druidry.queries.DataSourceMetadataQuery`, `druidry.queries.SegmentMetadataQuery`, `druidry.queries.TimeseriesQuery`, `druidry.queries.GroupByQuery`, `druidry.queries.ScanQuery`, `druidry.queries.TimeBoundaryQuery` and `druidry.queries.TopNQuery`. 

`Query` will validate the existence and type of required properties.


```python
try:
    druidry.queries.TimeseriesQuery()
except druidry.errors.DruidQueryError as e:
    print(e)
#     Invalid Druid query:
#      Missing field: intervals required for type: timeseries
```


```python
try:
    druidry.queries.TimeBoundaryQuery(bound=False)
except druidry.errors.DruidQueryError as e:
    print(e)
#    Invalid Druid query:
#      Field bound has mismatched type (expecting <type 'basestring'>, found <type 'bool'>)
```


`Query` provides static methods for validation that make it easy to upgrade code using `dict` objects.


```python
try:
    druidry.queries.Query.validate_query({'queryType': 'timeBoundary'})
except druidry.errors.DruidQueryError as e:
    print(e)
#    Invalid Druid query:
#      Invalid dataSource: None
```



```python
try:
    druidry.queries.Query.validate_query({'queryType': 'WHY???'})
except druidry.errors.DruidQueryError as e:
    print(e)
#     Invalid Druid query:
#      Invalid dataSource: None
#      Invalid queryType "WHY???". Valid query types: groupBy, timeBoundary, timeseries, topN
```


## context

`druidry.context` uses thread-local variables to register pre-processors which can alter all queries executed within a `with` block.


```python
query = druidry.queries.TimeBoundaryQuery()

# Client.execute_query calls process_query behind the scenes.

print('Before', druidry.context.process_query(query))

with druidry.context.data_source_context('test-data-source'):
    print('During', druidry.context.process_query(query))

print('After', druidry.context.process_query(query))
#    Before {'queryType': 'timeBoundary'}
#    During {'queryType': 'timeBoundary', 'dataSource': 'test-data-source'}
#    After {'queryType': 'timeBoundary'}
```


Two other `QueryContext`s come out of the box: `query_filter_context`, which adds (or joins) a filter to all queries in the block, and `timeout_context`, which adds a timeout to all queries in the block.

One can also create their own `QueryContext`s by passing a unique name and a function which accepts a query and returns a query.


```python
def add_granularity(query):
    return query.extend(granularity='P1D')

day_granularity_context = druidry.context.QueryContext('day-granularity', add_granularity)
```

## aggregations

The `druidry.aggregations.Aggregation` assists in building and validating Druid aggregations.


```python
try:
    druidry.aggregations.Aggregation('count')
except druidry.errors.DruidQueryError as e:
    print(e)

#    Invalid Druid query:
#      Missing field: name required for type: count
```



```python
druidry.aggregations.Aggregation('longSum', field_name='time_on_site')
#    {'fieldName': 'time_on_site', 'type': 'longSum'}
```


Aggregations have a filter method.


```python
druidry.aggregations.Aggregation('longSum', field_name='time_on_site').filter(
    druidry.filters.SelectorFilter(dimension='is_returning_user', value='t'))
#    {'aggregator': {'fieldName': 'time_on_site',
#      'name': 'time_on_site',
#      'type': 'longSum'},
#     'filter': {'dimension': 'is_returning_user',
#      'type': 'selector',
#      'value': 't'},
#     'type': 'filtered'}
```


`druidry.aggregations.PostAggregation` is the equivalent for post-aggregations.


```python
druidry.aggregations.PostAggregation('fieldAccess', field_name='count')
#    {'fieldName': 'count', 'name': 'count', 'type': 'fieldAccess'}
```

Post-aggregations can be created from 

`druidry.aggregations.remove_duplicates` removes aggregations with duplicate names.


## filters

`druidry.filters.Filter` facilitates the creation and validation of filters, for queries, aggregations and post-aggregations.


```python
druidry.filters.Filter('regex', dimension='browser', pattern='Chrom*')
#    {'dimension': 'browser', 'pattern': 'Chrom*', 'type': 'regex'}
```

Filters can be joined with `join_filters`.


```python
druidry.filters.Filter.join_filters(
    druidry.filters.Filter('regex', dimension='browser', pattern='Chrom.*'),
    druidry.filters.Filter('regex', dimension='os', pattern='Windows.*'))
#     {'fields': [{'dimension': 'browser', 'pattern': 'Chrom.*', 'type': 'regex'},
#      {'dimension': 'os', 'pattern': 'Windows.*', 'type': 'regex'}],
#     'type': 'and'}
```



Filters can be negated.


```python
druidry.filters.Filter('regex', dimension='browser', pattern='Chrom.*').negate()
#     {'field': {'dimension': 'browser', 'pattern': 'Chrom.*', 'type': 'regex'},
#     'type': 'not'}
```

Convenience classes exist for the following filter types: `druidry.filters.SelectorFilter`, `druidry.filters.ColumnComparisonFilter`, `druidry.filters.RegexFilter`, `druidry.filters.AndFilter`, `druidry.filters.OrFilter`, `druidry.filters.NotFilter`, `druidry.filters.InFilter`, `druidry.filters.LikeFilter`, `druidry.filters.BoundFilter` and `druidry.filters.IntervalFilter`.


## granularities

`druidry.granularities.SimpleGranularity`, `druidry.granularities.DurationGranularity`, `druidry.granularities.PeriodGranularity` facilitate creating and validating granularities.


```python
(
    druidry.granularities.SimpleGranularity('all'),
    druidry.granularities.SimpleGranularity('day'),
    druidry.granularities.SimpleGranularity('hour')
)
# ('all', 'day', 'hour')

try:
    druidry.granularities.SimpleGranularity('NOT REAL OBVIOUSLY')
except ValueError as e:
    print(e)
#    Invalid granularity: NOT REAL OBVIOUSLY

druidry.granularities.DurationGranularity(duration=43200000)
# {'duration': 43200000, 'type': 'duration'}

try:
    druidry.granularities.DurationGranularity(duration=43200000, origin='wow, *so* not an origin')
except ValueError as e:
    print(e)
#    Invalid origin: wow, *so* not an origin

druidry.granularities.DurationGranularity(duration=43200000, origin='2016-01-05T04:19:56.240773')
#    {'duration': 43200000,
#     'origin': '2016-01-05T04:19:56.240773',
#     'type': 'duration'}

druidry.granularities.PeriodGranularity(period='P2M3D')
#     {'period': 'P2M3D', 'type': 'period'}
```



```python
try:
    druidry.granularities.PeriodGranularity(period='P2Z')
except ValueError as e:
    print(e)
#     Invalid period: P2Z
```


`druidry.granularities.PeriodGranularity` can create a period granularity either from a specified period or from any of a number of time delta args (eg days, weeks, months).


```python
druidry.granularities.PeriodGranularity(months=2, days=3)
#    {'period': 'P2M3D', 'type': 'period'}

try:
    druidry.granularities.PeriodGranularity(period='P2Y', time_zone='Lilliput/Mildendo')
except ValueError as e:
    print(e)
#    Invalid timeZone: Lilliput/Mildendo
```


## intervals

`druidry.intervals.Interval` facilitates interval creation in much the same way as granularities: it validates input and allows flexibility in using time arguments.


```python
import datetime

druidry.intervals.Interval(
    start='1970-01-01', end='1975-12-31')
#  '1970-01-01/1975-12-31'

druidry.intervals.Interval(
    start=datetime.date(year=1970, month=1, day=1),
    end=datetime.date(year=1975, month=12, day=31))
#  '1970-01-01/1975-12-31'

druidry.intervals.Interval(
    start=datetime.date(year=1970, month=1, day=1), duration='P5Y')
#  '1970-01-01/P5Y'

druidry.intervals.Interval(
    start=datetime.date(year=1970, month=1, day=1),
    duration=datetime.timedelta(days=7))
#  '1970-01-01/P7D'

druidry.intervals.Interval(
    end=datetime.date(year=1970, month=1, day=1), duration='P5Y')
#  'P5Y/1970-01-01'

druidry.intervals.Interval(
    end=datetime.date(year=1970, month=1, day=1),
    duration=datetime.timedelta(days=7))
#  'P7D/1970-01-01'

druidry.intervals.Interval(
    end=datetime.date(year=1970, month=1, day=1), weeks=1)
#  'P7D/1970-01-01'

druidry.intervals.Interval(duration=datetime.timedelta(days=7))
#  'P7D'

druidry.intervals.Interval(years=5, months=3, days=25)
#  'P5Y3M25D'

druidry.intervals.Interval(interval='P5Y')
#  'P5Y'
```

### Interval padding

It can be tricky to create timeseries with consistent and non-partial granule buckets. Say you want to display a timeseries chart with the last 24 hours of data, with hour granularity, up to the current moment. You might use an interval like this: `P1D/2017-09-27T14:50:23` (where `14:50` is the current time). But now, every time you view the page, the buckets shift because they start at a different second or minute. Naturally you set an origin. But now you get a partial leading bucket.

To solve this problem, `druidry` provides methods which transform intervals of all kinds to start/end intervals with a floored start (relative to a provided timedelta, which can be generated from a granularity) and ceiled end.

```python
interval = druidry.intervals.Interval(
    duration=datetime.timedelta(days=1),
    start=datetime.datetime(
        year=2017, month=9, day=27,
        hour=14, minute=50, second=23))
print(interval.pad_by_timedelta(datetime.timedelta(hours=1)))
# 2017-09-27T14:00:00/2017-09-28T15:00:00
```
