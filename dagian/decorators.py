from past.builtins import basestring


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
            func._dagian_output_configs.append({
                'handler': data_handler,
                'key': output_key,
                'handler_kwargs': handler_kwargs,
            })
        return func
    return will_generate_decorator
