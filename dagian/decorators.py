from __future__ import print_function, division, absolute_import, unicode_literals
import re

from past.builtins import basestring

from .data_definition import RequirementDefinition, Argument


DATA_KEY_PATTERN = re.compile(r'^[_a-zA-Z][_a-zA-Z0-9]*$')


def require(*args, **kwargs):
    if len(args) == 1:
        data_key = args[0]
        data_name = None
    elif len(args) == 2:
        data_key, data_name = args
    else:
        raise ValueError("require() only accepts 1 or 2 positional arguments.")

    if data_name is None:
        if isinstance(data_key, Argument):
            data_name = data_key.parameter
        elif isinstance(data_key, basestring):
            data_name = data_key
        else:
            raise ValueError("Data key can only be str or Argument.")

    def require_decorator(func):
        # pylint: disable=protected-access
        if not hasattr(func, '_dagian_requirements'):
            func._dagian_requirements = []
        func._dagian_requirements.append(RequirementDefinition(data_key, kwargs, data_name))
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
