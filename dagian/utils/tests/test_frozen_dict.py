from __future__ import print_function, division, absolute_import, unicode_literals
import unittest

import six

from dagian.utils.frozen_dict import FronzenDict


class FrozenDictTest(unittest.TestCase):
    def setUp(self):
        self.original_dict = {'123': '456', '789': '000'}
        self.frozen_dict = FronzenDict(self.original_dict)

    def test_getitem(self):
        for key, val in six.viewitems(self.original_dict):
            self.assertEqual(val, self.frozen_dict[key])

    def test_contains(self):
        self.assertEqual('a' in self.original_dict, 'a' in self.frozen_dict)
        self.assertEqual('123' in self.original_dict, '123' in self.frozen_dict)
        self.assertEqual('789' in self.original_dict, '789' in self.frozen_dict)
        self.assertEqual(789 in self.original_dict, 789 in self.frozen_dict)

    def test_iter(self):
        original_keys = list(self.original_dict)
        frozen_keys = list(self.frozen_dict)
        self.assertListEqual(original_keys, frozen_keys)

    def test_len(self):
        self.assertEqual(len(self.original_dict), len(self.frozen_dict))

    def test_str_and_repr(self):
        str(self.frozen_dict)
        repr(self.frozen_dict)

    def test_hash(self):
        self.assertIsInstance(hash(self.frozen_dict), int)

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

    def test_copy(self):
        new_frozen_dict = self.frozen_dict.copy()
        self.assertDictEqual(dict(new_frozen_dict), dict(self.frozen_dict))

    def test_replace(self):
        new_frozen_dict = self.frozen_dict.replace({'123': 123}, a=2)
        self.assertDictEqual(dict(self.original_dict), dict(self.frozen_dict))
        self.assertEqual(123, new_frozen_dict['123'])
        self.assertEqual(2, new_frozen_dict['a'])
