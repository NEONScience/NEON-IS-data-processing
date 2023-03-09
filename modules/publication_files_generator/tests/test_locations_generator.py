#!/usr/bin/env python3
import unittest

from pyfakefs.fake_filesystem_unittest import TestCase

from publication_files_generator.locations_generator import generate_locations_file


@unittest.skip('Not implemented.')
class LocationsGeneratorTest(TestCase):

    def setUp(self) -> None:
        self.setUpPyfakefs()

    @staticmethod
    def test_locations_generator(self):
        generate_locations_file()
