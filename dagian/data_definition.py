from __future__ import print_function, division, absolute_import, unicode_literals
from collections import OrderedDict
import json

import six
from frozendict import frozendict


class DataDefinition(object):
    def __init__(self, key, args=None):
        self._key = key
        if args is None:
            self._args = frozendict()
        else:
            self._args = frozendict(args)

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


class RequirementDefinition(DataDefinition):
    pass
