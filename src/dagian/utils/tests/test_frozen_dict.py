from __future__ import print_function, division, absolute_import, unicode_literals
import unittest
from collections import OrderedDict
import json

import six

from dagian.utils.frozen_dict import FrozenDict, SortedFrozenDict, OrderedFrozenDict


class FrozenDictTest(unittest.TestCase):
    def setUp(self):
        self.original_dict = {'123': '456', '789': '000'}
        self.frozen_dict = FrozenDict(self.original_dict)

    def test_getitem(self):
        for key, val in six.viewitems(self.original_dict):
            self.assertEqual(val, self.frozen_dict[key])

    def test_contains(self):
        self.assertEqual('a' in self.original_dict, 'a' in self.frozen_dict)
        self.assertEqual('123' in self.original_dict, '123' in self.frozen_dict)
        self.assertEqual('789' in self.original_dict, '789' in self.frozen_dict)
        self.assertEqual(789 in self.original_dict, 789 in self.frozen_dict)

    def test_iter(self):
        original_keys = sorted(self.original_dict)
        frozen_keys = sorted(self.frozen_dict)
        self.assertListEqual(original_keys, frozen_keys)

    def test_len(self):
        self.assertEqual(len(self.original_dict), len(self.frozen_dict))

    def test_str(self):
        self.assertIsInstance(str(self.frozen_dict), str)

    def test_repr(self):
        self.assertEqual('FrozenDict(%s)' % repr(self.frozen_dict._dict), repr(self.frozen_dict))

    def test_hash(self):
        self.assertIsInstance(hash(self.frozen_dict), int)

    def test_keys(self):
        original_keys = sorted(six.viewkeys(self.original_dict))
        frozen_keys = sorted(self.frozen_dict.keys())
        self.assertListEqual(original_keys, frozen_keys)

    def test_values(self):
        original_values = sorted(six.viewvalues(self.original_dict))
        frozen_values = sorted(self.frozen_dict.values())
        self.assertListEqual(original_values, frozen_values)

    def test_items(self):
        original_items = sorted(six.viewitems(self.original_dict))
        frozen_items = sorted(self.frozen_dict.items())
        self.assertListEqual(original_items, frozen_items)

    def test_copy(self):
        new_frozen_dict = self.frozen_dict.copy()
        self.assertDictEqual(dict(new_frozen_dict), dict(self.frozen_dict))

    def test_replace(self):
        new_frozen_dict = self.frozen_dict.replace({'123': 123}, a=2)
        self.assertDictEqual(dict(self.original_dict), dict(self.frozen_dict))
        self.assertEqual(123, new_frozen_dict['123'])
        self.assertEqual(2, new_frozen_dict['a'])

    def test_to_json(self):
        self.assertDictEqual(self.original_dict, json.loads(self.frozen_dict.to_json()))

    def test_operators(self):
        self.assertTrue(self.frozen_dict == self.frozen_dict)
        self.assertFalse(self.frozen_dict != self.frozen_dict)
        self.assertFalse(self.frozen_dict < self.frozen_dict)
        self.assertTrue(self.frozen_dict <= self.frozen_dict)
        self.assertFalse(self.frozen_dict > self.frozen_dict)
        self.assertTrue(self.frozen_dict >= self.frozen_dict)


class OrderedFrozenDictTest(FrozenDictTest):
    def setUp(self):
        self.original_dict = OrderedDict((('123', '456'), ('789', '000')))
        self.frozen_dict = OrderedFrozenDict(self.original_dict)

    def test_repr(self):
        self.assertEqual('OrderedFrozenDict(%s)' % self.original_dict, repr(self.frozen_dict))

    def test_iter(self):
        original_keys = list(self.original_dict)
        frozen_keys = list(self.frozen_dict)
        self.assertListEqual(original_keys, frozen_keys)

    def test_keys(self):
        original_keys = list(six.viewkeys(self.original_dict))
        frozen_keys = list(self.frozen_dict.keys())
        self.assertListEqual(original_keys, frozen_keys)

    def test_values(self):
        original_values = list(six.viewvalues(self.original_dict))
        frozen_values = list(self.frozen_dict.values())
        self.assertListEqual(original_values, frozen_values)

    def test_items(self):
        original_items = list(six.viewitems(self.original_dict))
        frozen_items = list(self.frozen_dict.items())
        self.assertListEqual(original_items, frozen_items)

    def test_to_json(self):
        self.assertEqual(json.dumps(self.original_dict), self.frozen_dict.to_json())


class SortedFrozenDictTest(OrderedFrozenDictTest):
    def setUp(self):
        self.original_dict = OrderedDict((('123', '456'), ('789', '000')))
        unsorted_dict = OrderedDict((('789', '000'), ('123', '456')))
        self.frozen_dict = SortedFrozenDict(unsorted_dict)

    def test_repr(self):
        self.assertEqual('SortedFrozenDict(%s)' % self.original_dict, repr(self.frozen_dict))
