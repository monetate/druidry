"""Minimal error classes that contain the query and the response."""
import sys

if sys.version_info >= (3, 0):
    basestring = str


class DruidError(Exception):
    """Base class for errors related to creating or executing Druid queries."""

    def __init__(self, error, query=None, response=None):
        """Save the query and the response to the exception for introspection."""
        self.query = query
        self.response = response
        super(DruidError, self).__init__(error)


class DruidQueryError(DruidError):
    """Class for errors related to creating Druid queries."""

    def __init__(self, errors, query=None, response=None):
        """Create a message for a variable number of Druid query errors."""
        if isinstance(errors, basestring):
            errors = [errors]
        error = 'Invalid Druid query:\n  {}'.format(
            errors[0] if len(errors) == 1 else '\n  '.join(errors))
        super(DruidQueryError, self).__init__(
            error, query=query, response=response)


class DruidExecutionError(DruidError):
    """Class for errors related to executing Druid queries."""

    pass


class DruidTimeoutError(DruidExecutionError):
    """Class for errors caused by Druid queries timing out."""

    def __init__(self, elapsed=None, timeout=None, query=None, response=None):
        """Create a message displaying the relevant data for timeouts."""
        error = 'Druid timeout error. Elapsed request time: {}; specified timeout: {}'.format(
            elapsed, timeout)
        super(DruidExecutionError, self).__init__(
            error, query=query, response=response)
