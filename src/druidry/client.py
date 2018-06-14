"""
Client facilitates executing requests to Druid over HTTP.

The purpose of this module is just to validate and execute queries.
"""
import contextlib
import json
import requests

from . import context
from . import errors

from .queries import Query


@contextlib.contextmanager
def _null_context():
    yield


class Client(object):
    """Client facilitates executing requests to Druid over HTTP."""

    def __init__(self, host, port, path='druid/v2', timeout=0, data_source=None, fetch_schema=False):
        """
        Configure the client.

        Host and port are required; path defaults to "druid/v2".

        If timeout is provided, a timeout context is added to all executed queries.

        If data_source is not provided, the fetch_schema method will not work.

        If fetch_schema is provided, the properties `dimensions` and `metrics`
        will be populated.
        """
        self.host = host
        self.port = port
        self.path = path
        self.timeout = timeout
        self.data_source = data_source

        if fetch_schema:
            self.dimensions, self.metrics = self.fetch_schema()

    dimensions = None

    metrics = None

    @property
    def broker_metadata_endpoint(self):
        """Return the configured URL with which to make a metadata request."""
        return '{c.endpoint}/datasources'.format(c=self)

    @property
    def data_source_metadata_endpoint(self):
        """Return the configured URL with which to make a metadata request."""
        return '{c.broker_metadata_endpoint}/{c.data_source}'.format(c=self)

    @property
    def endpoint(self):
        """Return the configured URL with which to make Druid queries."""
        return 'http://{c.host}:{c.port}/{c.path}'.format(c=self)

    def fetch_schema(self):
        """Fetch the schema from the Broker for the specified dataSource."""
        if not self.data_source:
            raise errors.DruidQueryError('You must specify data_source in the Client constructor to fetch the schema.')

        # It seems that the API always returns 200 with JSON content,
        # regardless of the whether the dataSource exists.
        response = requests.get(self.data_source_metadata_endpoint)
        response_json = response.json()
        if type(response_json) == dict:
            return response_json.get('dimensions', []), response_json.get('metrics', [])
        return [], []

    def issue_request(self, query):
        """
        Execute the query by making an HTTP request.

        All Druid queries are expected to return well-formed JSON, so we raise
        an execution error if this is not the case.
        """
        response = requests.post(self.endpoint, data=json.dumps(query))
        try:
            response_json = response.json()
        except ValueError:
            raise errors.DruidExecutionError(
                'No JSON object could be decoded from the Druid response.',
                query=query, response=response.content)

        # Raise an exception if the status code does not indicate success.
        if response.status_code != 200:
            if response_json['error'] == 'Query timeout':
                raise errors.DruidTimeoutError(
                    response.elapsed, query['context']['timeout'],
                    query=query, response=response_json)
            raise errors.DruidExecutionError(
                'Druid responded with non-200 status code.',
                query=query, response=response_json)

        return response_json

    @property
    def _data_source_context(self):
        if self.data_source:
            return context.data_source_context(self.data_source)
        return _null_context()

    @property
    def _timeout_context(self):
        if self.timeout:
            return context.timeout_context(self.timeout)
        return _null_context()

    def execute_query(self, query_obj):
        """
        Execute a query by making an HTTP request.

        The query is first validated and then executed; the response is
        inspected for validity then parsed and returned.
        """
        with self._timeout_context, self._data_source_context:
            query = context.process_query(Query(**query_obj))

            # Validate the structure and format of the query before executing.
            # We validate inside these context managers because otherwise we
            # might get a missing dataSource exception.
            query.validate()

            return self.issue_request(query)
