from __future__ import print_function, division, absolute_import, unicode_literals
import collections
from collections import OrderedDict
import copy

import six


class FrozenDict(collections.Mapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)
        self._hash = None

    def __getitem__(self, key):
        return self._dict[key]

    def __contains__(self, key):
        return key in self._dict

    def __iter__(self):
        return iter(self._dict)

    def __len__(self):
        return len(self._dict)

    def __repr__(self):
        class_name = type(self).__name__
        return "%s(%s)" % (class_name, repr(self._dict))

    def __str__(self):
        return str(self._dict)

    def __hash__(self):
        if self._hash is None:
            h = 0
            for key_value in six.viewitems(self._dict):
                h ^= hash(key_value)
            self._hash = h
        return self._hash

    def keys(self):
        return six.viewkeys(self._dict)

    def values(self):
        for val in six.viewvalues(self._dict):
            yield copy.deepcopy(val)

    def items(self):
        for key, val in six.viewitems(self._dict):
            yield key, copy.deepcopy(val)

    def copy(self):
        return copy.copy(self)

    def replace(self, *args, **kwargs):
        new_dict = copy.deepcopy(self._dict)
        new_dict.update(*args, **kwargs)
        return type(self)(new_dict)


class SortedFrozenDict(FrozenDict):
    def __init__(self, *args, **kwargs):
        unsorted_dict = dict(*args, **kwargs)
        self._dict = OrderedDict((k, unsorted_dict[k])
                                 for k in sorted(six.viewkeys(unsorted_dict)))
        self._hash = None
