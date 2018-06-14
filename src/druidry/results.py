"""Utilites for post-processing results into dataframes."""
import pandas as pd


class QueryResult(object):
    """Wrapper class for facilitating post-process of results."""

    def __init__(self, query, result):
        """Accept the query and result. Check for dependencies and validity."""
        if pd is None:
            raise Exception('QueryResult requires pandas.')
        if query['queryType'] == 'timeBoundary':
            raise ValueError('timeBoundary is not supported for QueryResult objects.')
        self.query = query
        self.result = result

    def __iter__(self):
        """Facilitate using these results like a normal JSON-parsed result."""
        return iter(self.result)

    @property
    def index(self):
        """Inspect the query to find the index for the dataframe."""
        if self.query['queryType'] == 'scan':
            if not self.query.get('columns') or '__time' in self.query['columns']:
                return ['__time']
            return []
        if self.query['queryType'] in {'groupBy', 'topN', 'timeseries'}:
            index_fields = [] if self.query['granularity'] == 'all' else ['timestamp']
        if self.query['queryType'] == 'groupBy':
            return index_fields + self.query['dimensions']
        elif self.query['queryType'] == 'topN':
            return index_fields + [self.query['dimension']]
        elif self.query['queryType'] == 'timeseries':
            return index_fields

    @property
    def record_key(self):
        """Inspect the query to find the expected key of the data."""
        if self.query['queryType'] == 'groupBy':
            return 'event'
        elif self.query['queryType'] in ('timeseries', 'topN'):
            return 'result'

    @property
    def rows(self):
        """
        Iterate the rows in the result.

        Flat-map the topN, since there are N results per time bucket.
        Otherwise, same as iterating the object itself.
        """
        if self.query['queryType'] == 'topN':
            for row in self.result:
                for r in row['result']:
                    yield {'timestamp': row['timestamp'], 'result': r}
        elif self.query['queryType'] == 'scan':
            for batch in self.result:
                for event in batch['events']:
                    yield (
                        dict(zip(batch['columns'], event))
                        if isinstance(event, list) else event
                    )
        else:
            for row in self:
                yield row

    @property
    def records(self):
        """Coerce the rows to records for pandas."""
        return [self.row_to_record(row) for row in self.rows]

    def row_to_record(self, row):
        """Coerce a single row to a pandas record."""
        record = row if not self.record_key else row[self.record_key]
        if 'timestamp' in row:
            return dict(
                timestamp=pd.to_datetime(row['timestamp']), **record)
        if '__time' in record:
            return dict(record, __time=pd.to_datetime(record['__time']))
        return record

    def to_dataframe(self):
        """Transform the result object into a dataframe."""
        if not self.index:
            return pd.DataFrame.from_records(self.records)
        else:
            return pd.DataFrame.from_records(self.records, index=self.index)
