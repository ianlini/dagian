from __future__ import print_function, division, absolute_import, unicode_literals
import collections

import six
from six.moves import zip
from past.builtins import basestring
from .utils.frozen_dict import OrderedFrozenDict, SortedFrozenDict


class DataDefinition(OrderedFrozenDict):
    def __init__(self, key, args=None, name=None):
        assert isinstance(key, (basestring, Argument, tuple)), \
            "Data key can only be str or Argument."
        assert name is None or isinstance(name, basestring), "Data name can only be str."
        self._key = key
        if args is None:
            self._args = SortedFrozenDict()
        else:
            self._args = SortedFrozenDict.recursively_froze(args)
        self._name = name
        super(DataDefinition, self).__init__((('key', key), ('args', self._args)))

    @property
    def key(self):
        return self._key

    @property
    def args(self):
        return self._args

    @property
    def name(self):
        return self._name

    def replace(self, key=None, args=None, name=None):
        if key is None:
            key = self._key
        if args is None:
            args = self._args
        if name is None:
            name = self._name
        return DataDefinition(key=key, args=args, name=name)

    def __str__(self):
        class_name = type(self).__name__
        if len(self._args) == 0:
            return "%s(%r)" % (class_name, self._key)
        return "%s(%r, %s)" % (class_name, self._key, self._args)

    def __repr__(self):
        return str(self)


class Argument(object):
    """Represent how the argument is passed from downstream to upstream.

    Parameters
    ----------
    parameter : str
        The name of the parameter from downstream data definition.
    callable : Optional[Callable]
        The callable to be applied on the downstream argument. If None, use identity function.
    """

    def __init__(self, parameter, callable=None):
        self.parameter = parameter
        self.callable = callable

    def eval(self, args):
        if self.callable is None:
            return args[self.parameter]
        return self.callable(args[self.parameter])

    def __str__(self):
        class_name = type(self).__name__
        return "%s(%r)" % (class_name, self.parameter)

    def __repr__(self):
        return str(self)

    def __gt__(self, other):
        if isinstance(other, basestring):
            return self.parameter > other
        elif isinstance(other, Argument):
            return self.parameter > other.parameter
        return NotImplemented

    def __lt__(self, other):
        if isinstance(other, basestring):
            return self.parameter < other
        elif isinstance(other, Argument):
            return self.parameter < other.parameter
        return NotImplemented


class RequirementDefinition(DataDefinition):
    def eval_data_definition(self, args):
        # TODO: refactor
        # evaluate key
        if isinstance(self._key, Argument):
            raw_data_def = self._key.eval(args)
            if isinstance(raw_data_def, collections.Mapping):
                new_keys = [raw_data_def['key']]
                new_args = [dict(raw_data_def.get('args', {}))]
            elif isinstance(raw_data_def, basestring):
                new_keys = [raw_data_def]
                new_args = [{}]
            elif isinstance(raw_data_def, collections.Sequence):
                new_keys = []
                new_args = []
                for _raw_data_def in raw_data_def:
                    if isinstance(_raw_data_def, basestring):
                        new_key = _raw_data_def
                        new_arg = {}
                    else:
                        new_key = _raw_data_def['key']
                        new_arg = dict(_raw_data_def.get('args', {}))
                    new_keys.append(new_key)
                    new_args.append(new_arg)
            else:
                raise ValueError("Evaluated arguments only support dict, list or str.")
        elif isinstance(self._key, basestring):
            new_keys = [self._key.format(**args)]
            new_args = [{}]
        else:
            raise ValueError("RequirementDefinition.key can only be Argument or str.")

        # evaluate arguments
        for key, arg in six.viewitems(self._args._dict):
            if isinstance(arg, Argument):
                arg = arg.eval(args)
            elif isinstance(arg, basestring):
                arg = arg.format(**args)
            elif not isinstance(arg, collections.Hashable):
                raise ValueError(
                    "The values in RequirementDefinition.args can only be Argument or hashable.")
            for new_arg in new_args:
                new_arg[key] = arg

        data_definitions = [DataDefinition(key, arg, self._name)
                            for key, arg in zip(new_keys, new_args)]
        return data_definitions
