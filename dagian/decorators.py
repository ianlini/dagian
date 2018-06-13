from __future__ import print_function, division, absolute_import, unicode_literals
import re

from past.builtins import basestring
from frozendict import frozendict

from .data_definition import RequirementDefinition


DATA_KEY_PATTERN = re.compile(r'^[_a-zA-Z][_a-zA-Z0-9]*$')


class Argument(object):
    """Represent how the argument is passed from downstream to upstream.

    Parameters
    ----------
    parameter : str
        The name of the parameter from downstream data definition.
    callable : Optional[Callable]
        The callable to be applied on the downstream argument. If None, use identity function.
    """
    def __init__(self, parameter, callable=None, template=None):
        self.parameter = parameter
        self.callable = callable


def require(data_key, **kwargs):
    def require_decorator(func):
        # pylint: disable=protected-access
        if not hasattr(func, '_dagian_requirements'):
            func._dagian_requirements = []
        func._dagian_requirements.append(RequirementDefinition(data_key, kwargs))
        return func
    return require_decorator


def will_generate(data_handler, output_keys, **handler_kwargs):
    """
    Parameters
    ----------
    output_keys: Union[List[str], str]
    """
    if isinstance(output_keys, basestring):
        output_keys = (output_keys,)

    def will_generate_decorator(func):
        # pylint: disable=protected-access
        if not hasattr(func, '_dagian_output_configs'):
            func._dagian_output_configs = []
        for output_key in output_keys:
            matched = DATA_KEY_PATTERN.match(output_key)
            if matched is None:
                raise ValueError("output_key %s doesn't match the pattern %s."
                                 % (output_key, DATA_KEY_PATTERN.pattern))
            func._dagian_output_configs.append({
                'handler': data_handler,
                'key': output_key,
                'handler_kwargs': handler_kwargs,
            })
        return func
    return will_generate_decorator


def params(*parameters):
    def params_decorator(func):
        # pylint: disable=protected-access
        if not hasattr(func, '_dagian_parameters'):
            func._dagian_parameters = []
        func._dagian_parameters.extend(parameters)
        return func
    return params_decorator
