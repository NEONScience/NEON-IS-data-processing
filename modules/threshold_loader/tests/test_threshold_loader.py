#!/usr/bin/env python3
"""
Test suite for threshold_loader module.

This test suite validates the threshold loading functionality with support for multiple contexts.

UNIT TESTS (run by default with pytest):
- test_write_file: Basic threshold loading and JSON serialization
- test_exact_context_matching: Validates exact context matching (excludes partial/superset matches)
- test_multiple_contexts_filtering: Tests filtering with multiple context sets
- test_empty_context_handling: Tests 'none' context (wildcard — matches all thresholds regardless of context)
- test_duplicate_threshold_handling: Verifies duplicate thresholds are deduplicated

INTEGRATION TESTS (skipped by default, require database access):
- test_main: Backward compatibility with single CTXT environment variable
- test_main_new: New multi-context functionality (CTXT_1, CTXT_2, CTXT_3, ...)

"""
import os
import json
from pathlib import Path
import unittest
from typing import Iterator

from data_access.tests.database_test import DatabaseBackedTest
from data_access.types.threshold import Threshold
import threshold_loader.threshold_loader_main as threshold_loader_main
from threshold_loader.threshold_loader import load_thresholds


class ThresholdLoaderTest(DatabaseBackedTest):

    def setUp(self):
        self.setUpPyfakefs()
        self.out_path = Path('/out')
        self.fs.create_dir(self.out_path)

    def tearDown(self):
        for key in ('TERM', 'CTXT', 'CTXT_1', 'CTXT_2', 'CTXT_3', 'OUT_PATH', 'LOG_LEVEL'):
            os.environ.pop(key, None)

    def test_write_file(self):

        def get_thresholds(term) -> Iterator[Threshold]:
            """
            Mock function for getting thresholds.

            :return: A threshold.
            """
            yield Threshold(threshold_name='threshold_name',
                            term_name=term,
                            location_name='CPER',
                            context=['context1', 'context2'],
                            start_date='start_date',
                            end_date='end_date',
                            is_date_constrained=True,
                            start_day_of_year=1,
                            end_day_of_year=365,
                            number_value=10,
                            string_value='value')

        load_thresholds(get_thresholds, self.out_path, 'term_name', ['context1|context2'])
        expected_path = self.out_path.joinpath('thresholds.json')
        self.assertTrue(expected_path.exists())
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            threshold = json_data['thresholds'][0]
            threshold_name: str = threshold['threshold_name']
            term_name: str = threshold['term_name']
            location_name: str = threshold['location_name']
            context: list = threshold['context']
            start_date: str = threshold['start_date']
            end_date: str = threshold['end_date']
            is_date_constrained: bool = threshold['is_date_constrained']
            start_day_of_year = threshold['start_day_of_year']
            end_day_of_year = threshold['end_day_of_year']
            number_value = threshold['number_value']
            string_value = threshold['string_value']
            self.assertTrue(threshold_name == 'threshold_name')
            self.assertTrue(term_name == 'term_name')
            self.assertTrue(location_name == 'CPER')
            self.assertTrue(context[0] == 'context1')
            self.assertTrue(context[1] == 'context2')
            self.assertTrue(start_date == 'start_date')
            self.assertTrue(end_date == 'end_date')
            self.assertTrue(is_date_constrained)
            self.assertTrue(start_day_of_year == 1)
            self.assertTrue(end_day_of_year == 365)
            self.assertTrue(number_value == 10)
            self.assertTrue(string_value == 'value')
            print(json.dumps(json_data, indent=2, sort_keys=False))

    def test_exact_context_matching(self):
        """Test that only thresholds with exact context matches are returned."""
        
        def get_thresholds(term) -> Iterator[Threshold]:
            # Exact match: soil|ion-content
            yield Threshold(
                threshold_name='exact_match',
                term_name=term,
                location_name='CPER',
                context=['soil', 'ion-content'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=100.0,
                string_value=None
            )
            
            # Partial match: only soil (should be excluded)
            yield Threshold(
                threshold_name='partial_match',
                term_name=term,
                location_name='CPER',
                context=['soil'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=200.0,
                string_value=None
            )
            
            # Superset: soil|ion-content|extra (should be excluded)
            yield Threshold(
                threshold_name='superset_match',
                term_name=term,
                location_name='CPER',
                context=['soil', 'ion-content', 'extra'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=300.0,
                string_value=None
            )
        
        load_thresholds(get_thresholds, self.out_path, 'term_name', ['soil|ion-content'])
        expected_path = self.out_path.joinpath('thresholds.json')
        
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            thresholds = json_data['thresholds']
            
            # Should have exactly 1 threshold
            self.assertEqual(len(thresholds), 1)
            self.assertEqual(thresholds[0]['threshold_name'], 'exact_match')

    def test_multiple_contexts_filtering(self):
        """Test filtering with multiple context sets."""
        
        def get_thresholds(term) -> Iterator[Threshold]:
            # Matches first context
            yield Threshold(
                threshold_name='match_context1',
                term_name=term,
                location_name='CPER',
                context=['soil', 'ion-content'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=100.0,
                string_value=None
            )
            
            # Matches second context
            yield Threshold(
                threshold_name='match_context2',
                term_name=term,
                location_name='CPER',
                context=['soil', 'water-content', 'factory'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=200.0,
                string_value=None
            )
            
            # Matches neither
            yield Threshold(
                threshold_name='no_match',
                term_name=term,
                location_name='CPER',
                context=['water', 'generic'],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=999.0,
                string_value=None
            )
        
        load_thresholds(get_thresholds, self.out_path, 'term_name', 
                       ['soil|ion-content', 'soil|water-content|factory'])
        expected_path = self.out_path.joinpath('thresholds.json')
        
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            thresholds = json_data['thresholds']
            
            # Should have exactly 2 thresholds
            self.assertEqual(len(thresholds), 2)
            threshold_names = [t['threshold_name'] for t in thresholds]
            self.assertIn('match_context1', threshold_names)
            self.assertIn('match_context2', threshold_names)
            self.assertNotIn('no_match', threshold_names)

    def test_empty_context_handling(self):
        """Test handling of 'none' context — acts as a wildcard matching all thresholds regardless of context."""
        
        def get_thresholds(term) -> Iterator[Threshold]:
            # No context
            yield Threshold(
                threshold_name='no_context',
                term_name=term,
                location_name='CPER',
                context=[],
                start_date='2024-01-01',
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=100.0,
                string_value=None
            )
            
            # Has context (should also be included — 'none' is a wildcard)
            yield Threshold(
                threshold_name='has_context',
                term_name=term,
                location_name='CPER',
                context=['soil'],
                start_date='2024-01-01', 
                end_date=None,
                is_date_constrained=False,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=200.0,
                string_value=None
            )
        
        load_thresholds(get_thresholds, self.out_path, 'term_name', ['none'])
        expected_path = self.out_path.joinpath('thresholds.json')
        
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            thresholds = json_data['thresholds']
            
            # 'none' is a wildcard — should match all thresholds regardless of context
            self.assertEqual(len(thresholds), 2)
            threshold_names = [t['threshold_name'] for t in thresholds]
            self.assertIn('no_context', threshold_names)
            self.assertIn('has_context', threshold_names)

    def test_duplicate_threshold_handling(self):
        """Test that duplicate thresholds are handled correctly."""
        
        def get_thresholds(term) -> Iterator[Threshold]:
            # Same threshold multiple times
            for i in range(3):
                yield Threshold(
                    threshold_name='duplicate_threshold',
                    term_name=term,
                    location_name='CPER',
                    context=['soil', 'ion-content'],
                    start_date='2024-01-01',
                    end_date='2024-12-31',
                    is_date_constrained=True,
                    start_day_of_year=1,
                    end_day_of_year=365,
                    number_value=100.0,
                    string_value=None
                )
        
        load_thresholds(get_thresholds, self.out_path, 'term_name', ['soil|ion-content'])
        expected_path = self.out_path.joinpath('thresholds.json')
        
        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            thresholds = json_data['thresholds']
            
            # Should only have 1 threshold (duplicates removed)
            self.assertEqual(len(thresholds), 1)
            self.assertEqual(thresholds[0]['threshold_name'], 'duplicate_threshold')

    def test_same_key_different_context_not_deduplicated(self):
        """Test that thresholds identical in all fields except context are both kept."""

        def get_thresholds(term) -> Iterator[Threshold]:
            yield Threshold(
                threshold_name='shared_threshold',
                term_name=term,
                location_name='CPER',
                context=['soil', 'water-content', 'factory'],
                start_date='2024-01-01',
                end_date='2024-12-31',
                is_date_constrained=True,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=100.0,
                string_value=None
            )
            yield Threshold(
                threshold_name='shared_threshold',
                term_name=term,
                location_name='CPER',
                context=['soil', 'water-content', 'soil-specific'],
                start_date='2024-01-01',
                end_date='2024-12-31',
                is_date_constrained=True,
                start_day_of_year=1,
                end_day_of_year=365,
                number_value=100.0,
                string_value=None
            )

        load_thresholds(
            get_thresholds,
            self.out_path,
            'term_name',
            ['soil|water-content|factory', 'soil|water-content|soil-specific']
        )
        expected_path = self.out_path.joinpath('thresholds.json')

        with open(expected_path, 'r') as threshold_file:
            json_data = json.load(threshold_file)
            thresholds = json_data['thresholds']

        # Both context variants must be present, not collapsed into one
        self.assertEqual(len(thresholds), 2)
        contexts_in_output = [tuple(sorted(t['context'])) for t in thresholds]
        self.assertIn(('factory', 'soil', 'water-content'), contexts_in_output)
        self.assertIn(('soil', 'soil-specific', 'water-content'), contexts_in_output)

    # ========== INTEGRATION TESTS (Database access required) ==========
    # These tests are skipped by default to avoid requiring database credentials.
    # To run them, remove the skip logic, install requirements, provide database credentials via environment variables, then run: 
    # python3 -m pytest threshold_loader/tests/test_threshold_loader.py -v

    @unittest.skip("Integration test - requires database access. See comment above for how to run.")
    def test_main(self):
        """Test backward compatibility with single CTXT environment variable."""
        self.configure_mount()
        os.environ['TERM'] = 'veloXaxs|veloYaxs|veloZaxs|veloSoni|tempSoni'
        os.environ['CTXT'] = '3Dwind'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        threshold_loader_main.main()
        expected_path = Path(self.out_path, 'thresholds.json')
        self.assertTrue(expected_path.exists())

    @unittest.skip("Integration test - requires database access. See comment above for how to run.")
    def test_main_new(self):
        """Test new multi-context functionality with CTXT_1, CTXT_2, CTXT_3 variables."""
        self.configure_mount()
        os.environ['TERM'] = 'VSWCfactoryDepth02|VSICDepth02|VSWCsoilSpecificDepth02'
        os.environ['CTXT_1'] = 'soil|ion-content'
        os.environ['CTXT_2'] = 'soil|water-content|factory'
        os.environ['CTXT_3'] = 'soil|water-content|soil-specific'
        os.environ['OUT_PATH'] = str(self.out_path)
        os.environ['LOG_LEVEL'] = 'DEBUG'
        threshold_loader_main.main()
        expected_path = Path(self.out_path, 'thresholds.json')
        self.assertTrue(expected_path.exists())