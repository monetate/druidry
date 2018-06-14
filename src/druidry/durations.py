"""Duration and interval building utility methods."""

import datetime
import isodate
import sys

if sys.version_info >= (3, 0):
    basestring = str

DURATION_KWARGS = (
    'years',
    'months',
    'weeks',
    'days',
    'hours',
    'minutes',
    'seconds',
    'milliseconds'
)

TIME_DELTA_UNITS = ['days', 'hours', 'minutes', 'seconds']

DATE_TIME_UNITS = ['year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond']

TIME_DELTA_THRESHOLDS = list(zip(TIME_DELTA_UNITS, DATE_TIME_UNITS[2:]))


def validate_duration(duration):
    """Validate that the duration string is an ISO-8601 duration."""
    isodate.parse_duration(duration)


def has_duration_kwarg(**kwargs):
    """True if any of the supplied kwargs are duration kwargs."""
    return any(kwarg in kwargs for kwarg in DURATION_KWARGS)


def duration_kwargs_to_isoformat(**kwargs):
    """Take kwargs, filter to just duration kwargs, get an isoduration str."""
    duration = isodate.duration.Duration(
        **{kwarg: kwargs[kwarg] for kwarg in DURATION_KWARGS if kwarg in kwargs})
    return isodate.duration_isoformat(duration)


def get_timedelta_unit(td):
    """
    Given a timedelta, return the name of the largest unit for a datetime.

    So if a timedelta is 12 days, return 'day'. If a timedelta is 1 day, return
    'day'. If a timedelta is 45 minutes, return 'minute'.
    """
    for td_unit, dt_unit in TIME_DELTA_THRESHOLDS:
        if td >= datetime.timedelta(**{td_unit: 1}):
            return dt_unit


def get_unitary_timedelta(td):
    """
    Given a timedelta, return a timedelta with the largest unit set to 1.

    So if a timedelta is 12 days return days=1. If a timedelta is 1 day, return
    days=1. If a timedelta is 45 minutes, return minutes=1.
    """
    for td_unit, _ in TIME_DELTA_THRESHOLDS:
        unitary_timedelta = datetime.timedelta(**{td_unit: 1})
        if td >= unitary_timedelta:
            return unitary_timedelta


def floor_datetime(dt, td):
    """
    Given a datetime and a timedelta, floor the datetime by that timedelta.

    So if given a timedelta where the largest unit is days, set hours, minutes
    and smaller units to 0. If given a timedelta where the largest unit is
    minutes, set seconds and microseconds to 0.
    """
    unit = get_timedelta_unit(td)
    units = DATE_TIME_UNITS[DATE_TIME_UNITS.index(unit) + 1:]
    return dt.replace(**{unit: 0 for unit in units})


def ceil_datetime(dt, td):
    """Same as floor_datetime but round up if the smaller units are not 0."""
    floored_datetime = floor_datetime(dt, td)
    if floored_datetime == dt:
        return dt
    return floored_datetime + get_unitary_timedelta(td)


def parse_interval_part(interval_part):
    """
    Given either half of an ISO-8601 interval, parse it.

    ISO-8601 intervals consist of either two ISO-8601 date/times or else one
    ISO-8601 date/time and one ISO-8601 duration, so here we try to parse
    durations, then datetimes, then dates.
    """
    parsers = [
        isodate.parse_duration,
        isodate.parse_datetime,
        isodate.parse_date
    ]
    for parser in parsers:
        try:
            return parser(interval_part)
        except isodate.ISO8601Error:
            pass
    return None


def parse_interval(interval):
    """
    Parse a full ISO-8601 interval.

    Use parse_interval_part to parse each half, determine whether the provided
    halves are start/end, start/duration, or duration/end, then return start,
    end and duration.
    """
    left, right = interval.split('/', 2)
    left_value = parse_interval_part(left)
    right_value = parse_interval_part(right)

    # interval/date eg 'P7D/2010-01-01'
    if type(left_value) == datetime.timedelta:
        duration, end = left_value, right_value
        start = end - duration
    # date/interval eg '2010-01-01/P7D'
    elif type(right_value) == datetime.timedelta:
        start, duration = left_value, right_value
        end = start + duration
    # date/date eg '2010-01-01/2010-01-08'
    else:
        start, end = left_value, right_value
        duration = end - start

    return start, end, duration


def interval_or_delta(duration):
    """Accept a timedelta or an ISO-8601 interval and return a timedelta."""
    if isinstance(duration, basestring):
        return parse_interval(duration)[2]
    elif type(duration) == datetime.timedelta:
        return duration
    raise ValueError(
        'Invalid duration value: {}. '
        'Must be ISO-8601 interval or timedelta'.format(duration))


def duration_or_delta(duration):
    """Accept a timedelta or an ISO-8601 duration and return a timedelta."""
    if isinstance(duration, basestring):
        return isodate.parse_duration(duration)
    elif type(duration) == datetime.timedelta:
        return duration
    raise ValueError(
        'Invalid duration value: {}. '
        'Must be ISO-8601 interval or timedelta'.format(duration))


def divide_durations(d1, d2):
    """Divide one timedelta by another."""
    return d1.total_seconds() / d2.total_seconds()


def round_duration(duration, resolution):
    """Round a timedelta to the units specified by resolution."""
    resolution_duration = duration_or_delta(resolution)
    seconds = round(divide_durations(duration, resolution_duration)) * resolution_duration.total_seconds()
    duration = datetime.timedelta(seconds=seconds)
    return isodate.duration_isoformat(duration)


def find_closest_duration(duration, choices):
    """Find a duration in a list closest to a given duration."""
    durations = [duration_or_delta(choice) for choice in choices]
    ratios = [divide_durations(duration_choice, duration) for duration_choice in durations]
    distances = [abs(ratio - 1) for ratio in ratios]
    idx_min = sorted(enumerate(distances), key=lambda pair: pair[1])[0][0]
    return choices[idx_min]


def select_granularity(interval, n_buckets, resolution=None, choices=None):
    """
    Select a granularity that divides interval into n_buckets as closely as possible.

    If only 1 bucket is desired, return 'all'.

    If no other parameters are specified, divide it exactly.

    If choices are provided, select the choice that comes closest to dividing
    into n_buckets.

    If resolution is provided, divide it exactly and then round to that resolution.
    """
    if n_buckets == 1:
        return 'all'

    total_duration = interval_or_delta(interval)
    exact_result = total_duration / n_buckets

    if choices is not None:
        return find_closest_duration(exact_result, choices)

    if resolution is not None:
        return round_duration(exact_result, resolution)

    return exact_result
