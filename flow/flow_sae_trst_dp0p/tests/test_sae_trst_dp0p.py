#!/usr/bin/env python3

from unittest import TestCase

from flow_sae_trst_dp0p.shared_functions import get_nth_bit, get_range_bits, get_nth_bit_opposite


class SaeTrstDp0pTest(TestCase):

    def setUp(self):
        self.number = 61503  #1111 0000 0011 1111

    def test_get_nth_bit(self):
        message = "nth bit was not retrieved correctly"
        self.assertEqual(get_nth_bit(self.number, 4), 1, message)
        self.assertEqual(get_nth_bit(self.number, 6), 0, message)

    def test_get_range_bits(self):
        message = "data from range bits were not retrieved correctly"
        self.assertEqual(get_range_bits(self.number, 4, 5), 3, message)

    def test_get_nth_bit_opposite(self):
        message = "nth bit should be retrieved and reversed"
        self.assertEqual(get_nth_bit_opposite(self.number, 4), 0, message)
        self.assertEqual(get_nth_bit_opposite(self.number, 6), 1, message)



