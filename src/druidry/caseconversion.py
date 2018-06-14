"""Utility functions for making camel and snake case interchangable for kwarg simplicity."""
import decorator


def _snake_to_camel(snake_str):
    return ''.join(
        word.capitalize() if index > 0 else word
        for index, word in enumerate(snake_str.split('_')))


@decorator.decorator
def camel_case_kwargs(inner_fn, *args, **kwargs):
    """
    A simple decorator that converts the case of the kwargs in the callers.

    This allows usage like Query(query_type='timeseries') without having to
    resort to un-pythonic camel-cased keyword arguments.
    """
    return inner_fn(*args, **{
        _snake_to_camel(kwarg_key): kwarg_value
        for kwarg_key, kwarg_value in kwargs.items()
    })
