import re

from past.builtins import basestring


DATA_KEY_PATTERN = re.compile(r'^[a-zA-Z][_a-zA-Z]*$')


def require(data_key):
    def require_decorator(func):
        # pylint: disable=protected-access
        if not hasattr(func, '_dagian_requirements'):
            func._dagian_requirements = []
        func._dagian_requirements.append(data_key)
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
