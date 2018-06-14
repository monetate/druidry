"""
Utilities for creating and validating granularities.

See http://druid.io/docs/latest/querying/granularities.html#period-granularities
"""

import datetime
import isodate
import math
import pytz
import sys

from . import durations
from . import caseconversion

if sys.version_info >= (3, 0):
    basestring = str


SIMPLE_GRANULARITIES = (
    'all',
    'day',
    'fifteen_minute',
    'hour',
    'minute',
    'none',
    'second',
    'thirty_minute',
    'week',
    'year'
)


SIMPLE_GRANULARITIES_TIMEDELTAS = {
    'all': None,
    'day': datetime.timedelta(days=1),
    'fifteen_minute': datetime.timedelta(minutes=15),
    'hour': datetime.timedelta(hours=1),
    'minute': datetime.timedelta(minutes=1),
    'month': datetime.timedelta(days=30),
    'none': None,
    'second': datetime.timedelta(seconds=1),
    'thirty_minute': datetime.timedelta(minutes=30),
    'week': datetime.timedelta(weeks=1),
    'year': datetime.timedelta(days=365)
}


class SimpleGranularity(str):
    """SimpleGranularity subclasses str to add validation but not complicate JSON encoding."""

    def __new__(cls, granularity):
        """Validate that the granularity is an allowed value and create a string."""
        if granularity not in SIMPLE_GRANULARITIES:
            raise ValueError('Invalid granularity: {}'.format(granularity))
        return str.__new__(cls, granularity)

    @staticmethod
    def granularity_to_timedelta(granularity):
        """Return the timedelta between subsequent buckets."""
        return SIMPLE_GRANULARITIES_TIMEDELTAS[granularity]

    def to_timedelta(self):
        """Return the timedelta between subsequent buckets."""
        return SimpleGranularity.granularity_to_timedelta(self)


class Granularity(dict):
    """Abstract base class with a validation method."""

    def __init__(self):
        """Unimplemented."""
        raise NotImplementedError(
            'Granularity is an abstract base class. '
            'Use DurationGranularity or PeriodGranularity.')

    @staticmethod
    def validate_origin(origin):
        """Validate that the origin string is an ISO-8601 datetime."""
        try:
            isodate.parse_datetime(origin)
        except isodate.ISO8601Error:
            raise ValueError('Invalid origin: {}'.format(origin))


class DurationGranularity(Granularity):
    """
    Duration is a value in milliseconds and origin is an optional zero time reference.

    See http://druid.io/docs/latest/querying/granularities.html#duration-granularities
    """

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):
        """Create a duration from a number of miliseconds."""
        self['type'] = 'duration'
        self['duration'] = kwargs['duration']

        if 'origin' in kwargs:
            DurationGranularity.validate_origin(kwargs['origin'])
            self['origin'] = kwargs['origin']

    @staticmethod
    def duration_to_timedelta(duration):
        """Return the timedelta between subsequent buckets."""
        return datetime.timedelta(
            seconds=math.floor(duration / 1000),
            microseconds=(duration % 1000) * 1000)

    def to_timedelta(self):
        """Return the timedelta between subsequent buckets."""
        return DurationGranularity.duration_to_timedelta(self['duration'])


class PeriodGranularity(Granularity):
    """
    Period is specified as a ISO8601 duration and origin is an optional zero time reference.

    See http://druid.io/docs/latest/querying/granularities.html#period-granularities
    """

    @staticmethod
    def period_to_timedelta(period):
        """Return the timedelta between subsequent buckets."""
        return isodate.parse_duration(period)

    @staticmethod
    def validate_period(period):
        """Validate that the period string is an ISO-8601 duration."""
        try:
            isodate.parse_duration(period)
        except isodate.ISO8601Error:
            raise ValueError('Invalid period: {}'.format(period))

    @staticmethod
    def validate_timezone(timezone):
        """Validate that the timeZone string is an IANA timezone."""
        try:
            pytz.timezone(timezone)
        except pytz.UnknownTimeZoneError:
            raise ValueError('Invalid timeZone: {}'.format(timezone))

    @caseconversion.camel_case_kwargs
    def __init__(self, **kwargs):
        """
        Create a granularity from period and an optional origin and timezone.

        For flexiblity, callers can specify keywords arguments with units
        ranging from miliseconds to years, which will used to be create an
        ISO-8601 duration string for the period. Otherwise, a period kwarg
        is exepcted to be supplied as an ISO-8601 duration string.

        Validate the timezone using pytz.
        """
        self['type'] = 'period'

        # Look for duration kwargs (eg. seconds, week, years) and if one
        # exists, use it to create an ISO-8601 duration string. Otherwise,
        # expect a period kwarg and validate that it is ISO-8601.
        if durations.has_duration_kwarg(**kwargs):
            self['period'] = durations.duration_kwargs_to_isoformat(**kwargs)
        else:
            PeriodGranularity.validate_period(kwargs['period'])
            self['period'] = kwargs['period']

        if 'origin' in kwargs:
            PeriodGranularity.validate_origin(kwargs['origin'])
            self['origin'] = kwargs['origin']

        if 'timeZone' in kwargs:
            PeriodGranularity.validate_timezone(kwargs['timeZone'])
            self['timeZone'] = kwargs['timeZone']

    def to_timedelta(self):
        """Return the timedelta between subsequent buckets."""
        return PeriodGranularity.period_to_timedelta(self['period'])


def granularity_to_timedelta(granularity):
    """Return the timedelta between subsequent buckets."""
    if isinstance(granularity, basestring):
        return SimpleGranularity.granularity_to_timedelta(granularity)
    if isinstance(granularity, SimpleGranularity):
        return granularity.to_timedelta(granularity)
    if isinstance(granularity, dict):
        if granularity['type'] == 'period':
            return PeriodGranularity.period_to_timedelta(granularity['period'])
        if granularity['type'] == 'duration':
            return DurationGranularity.duration_to_timedelta(granularity['duration'])
    return None
