from __future__ import print_function, division, absolute_import, unicode_literals
from collections import OrderedDict
import json

import six
from past.builtins import basestring
from frozendict import frozendict


class DataDefinition(frozendict):
    def __init__(self, key, args=None):
        self._key = key
        if args is None:
            self._args = frozendict()
        else:
            self._args = frozendict(args)
        super(DataDefinition, self).__init__(key=key, args=self._args)

    @property
    def key(self):
        return self._key

    @property
    def args(self):
        return self._args

    def replace(self, key=None, args=None):
        if key is None:
            key = self._key
        if args is None:
            args = self._args
        return DataDefinition(key=key, args=args)

    def __str__(self):
        class_name = type(self).__name__
        if len(self._args) == 0:
            return "{}({})".format(class_name, repr(self._key))
        arg_strs = ["%s: %s" % (repr(key), repr(self._args[key]))
                    for key in sorted(six.viewkeys(self._args._dict))]
        args_str = "{%s}" % ", ".join(arg_strs)
        return "{}({}, {})".format(class_name, repr(self._key), args_str)

    def __repr__(self):
        return str(self)

    def json(self):
        ordered_args = ((key, self._args[key]) for key in sorted(six.viewkeys(self._args._dict)))
        ordered_data_def = OrderedDict((('key', self._key),
                                        ('args', OrderedDict(ordered_args))))
        json_str = json.dumps(ordered_data_def)
        return json_str

    def __lt__(self, other):
        if isinstance(other, DataDefinition):
            return (self.key, self.args) < (other.key, other.args)
        return NotImplemented


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
        return "Argument(%s)" % repr(self.parameter)

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
        # evaluate key
        if isinstance(self.key, Argument):
            new_key = self.key.eval(args)
        elif isinstance(self.key, basestring):
            new_key = self.key.format(**args)
        else:
            raise ValueError("RequirementDefinition.key can only be Argument or str.")

        # evaluate arguments
        new_args = {}
        for key, arg in six.viewitems(self.args._dict):
            if isinstance(arg, Argument):
                new_args[key] = arg.eval(args)
            elif isinstance(arg, basestring):
                new_args[key] = arg.format(**args)
            else:
                raise ValueError(
                    "The values in RequirementDefinition.args can only be Argument or str.")
        data_definition = DataDefinition(new_key, new_args)
        return data_definition
