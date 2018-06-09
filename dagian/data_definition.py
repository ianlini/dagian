from __future__ import print_function, division, absolute_import, unicode_literals
from collections import OrderedDict
import json

import six
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
        if len(self._args) == 0:
            return "DataDefinition({})".format(repr(self._key))
        arg_strs = ["%s: %s" % (repr(key), repr(self._args[key]))
                    for key in sorted(six.viewkeys(self._args._dict))]
        args_str = "{%s}" % ", ".join(arg_strs)
        return "DataDefinition({}, {})".format(repr(self._key), args_str)

    def __repr__(self):
        return str(self)

    def json(self):
        ordered_args = ((key, self._args[key]) for key in sorted(six.viewkeys(self._args._dict)))
        ordered_data_def = OrderedDict((('key', self._key),
                                        ('args', OrderedDict(ordered_args))))
        json_str = json.dumps(ordered_data_def)
        return json_str
