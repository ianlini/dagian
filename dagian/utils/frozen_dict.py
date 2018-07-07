from __future__ import print_function, division, absolute_import, unicode_literals
import collections
from collections import OrderedDict
import copy
import json

import six


class FrozenDictJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, FrozenDict):
            return obj._dict
        return super(FrozenDictJSONEncoder, self).default(obj)


class FrozenDict(collections.Mapping):
    def __init__(self, *args, **kwargs):
        self._dict = dict(*args, **kwargs)

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
        if hasattr(self, '_hash'):
            return self._hash
        h = 0
        for key_value in six.viewitems(self._dict):
            h ^= hash(key_value)
        self._hash = h
        return h

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

    def to_json(self):
        return json.dumps(self, cls=FrozenDictJSONEncoder)

    def __lt__(self, other):
        if isinstance(other, FrozenDict):
            return str(self) < str(other)
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, FrozenDict):
            return str(self) <= str(other)
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, FrozenDict):
            return str(self) > str(other)
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, FrozenDict):
            return str(self) >= str(other)
        return NotImplemented

    @classmethod
    def recursively_froze(cls, value):
        if (isinstance(value, collections.Mapping)
                and not isinstance(value, collections.Hashable)):
            value = cls._recursively_froze_mapping(value)
        elif (isinstance(value, collections.Sequence)
                and not isinstance(value, collections.Hashable)):
            value = cls._recursively_froze_sequence(value)
        return value

    @classmethod
    def _recursively_froze_sequence(cls, sequence):
        return tuple(cls.recursively_froze(val) for val in sequence)

    @classmethod
    def _recursively_froze_mapping(cls, mapping):
        return SortedFrozenDict((key, cls.recursively_froze(val))
                                for key, val in six.viewitems(mapping))


class OrderedFrozenDict(FrozenDict):
    def __init__(self, *args, **kwargs):
        self._dict = OrderedDict(*args, **kwargs)

    def __str__(self):
        s = ', '.join('%r: %r' % (k, v) for k, v in six.viewitems(self._dict))
        s = '{%s}' % s
        return s


class SortedFrozenDict(OrderedFrozenDict):
    def __init__(self, *args, **kwargs):
        unsorted_dict = dict(*args, **kwargs)
        self._dict = OrderedDict((k, unsorted_dict[k])
                                 for k in sorted(six.viewkeys(unsorted_dict)))
