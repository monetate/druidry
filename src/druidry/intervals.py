"""Creates ISO-8601 dates from a variety of inputs."""

import datetime
import isodate
import sys

from . import durations

if sys.version_info >= (3, 0):
    basestring = str


def _datetime_now():
    return datetime.datetime.now()


class Interval(str):
    """Interval subclasses str to allow flexibility but not complicate JSON encoding."""

    def __new__(cls, interval=None, start=None, end=None, duration=None, **kwargs):
        """
        Create a string interval either from a handful of possible formulations.

        Like ISO-8601 intervals, this allows specifying start and end, start
        and duration, duration and end, just duration or just interval.
        """
        # First, check to see if duration kwargs (eg weeks=3, days=2) are specified
        # without a duration string and if so, create a duration string.
        if durations.has_duration_kwarg(**kwargs) and not duration:
            duration = durations.duration_kwargs_to_isoformat(**kwargs)

        # Now we have a duration, whether specified via kwargs or explicitly.
        # Check the remaining args to figure out how to proceed.
        # If start or end is specified, we have three possibilities:
        #    1. start/end
        #    2. start/duration
        #    3. duration/end
        # All are handled in `_create_two_part_interval`. Otherwise, we just
        # want to make sure that if duration or interval are passed as dates,
        # we coerce them to ISO-8601 strings.
        interval_str = None
        if start or duration:
            interval_str = cls._create_two_part_interval(cls, start, end, duration)
        elif interval:
            interval_str = cls._create_date_str(interval)
        else:
            raise ValueError(
                'Invalid interval arguments: '
                'interval={interval}, start={start}, '
                'end={end}, duration={duration}'.format(
                    interval=interval, start=start, end=end, duration=duration))

        return str.__new__(cls, interval_str)

    @staticmethod
    def _create_two_part_interval(cls, start, end, duration):
        if start and end:
            parts = (cls._create_date_str(start), cls._create_date_str(end))
        elif start and duration:
            parts = (cls._create_date_str(start), cls._create_date_str(duration))
        elif end and duration:
            parts = (cls._create_date_str(duration), cls._create_date_str(end))
        elif start:
            parts = (cls._create_date_str(start), cls._create_date_str(_datetime_now()))
        elif duration:
            parts = (cls._create_date_str(duration), cls._create_date_str(_datetime_now()))

        return '/'.join(parts)

    @staticmethod
    def _create_date_str(date_or_str):
        """Turn a datetime or timedelta into a date str."""
        if isinstance(date_or_str, basestring):
            return date_or_str
        if type(date_or_str) in (datetime.datetime, datetime.date):
            return date_or_str.isoformat()
        if type(date_or_str) == datetime.timedelta:
            return isodate.duration_isoformat(date_or_str)

        raise ValueError('Invalid value for interval: {}'.format(date_or_str))

    @staticmethod
    def pad_interval_by_timedelta(interval, td):
        """
        Given an interval and a timedelta, pad the interval by the timedelta.

        For example, if the timedelta's largest unit is hours, we floor the
        start to the nearest hour and ceil the end to the nearest hour.
        """
        start, end, _ = durations.parse_interval(interval)
        start_floor = durations.floor_datetime(start, td)
        end_ceil = durations.ceil_datetime(end, td)
        return Interval(start=start_floor, end=end_ceil)

    def pad_by_timedelta(self, td):
        """Given a timedelta, pad this interval by the timedelta."""
        return Interval.pad_interval_by_timedelta(self, td)
