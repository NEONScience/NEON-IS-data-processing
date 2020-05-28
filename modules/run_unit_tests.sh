#!/bin/bash
python3 -B -m unittest \
calibrated_location_group.tests.test_calibrated_location_group \
context_filter.tests.test_context_filter \
data_access.tests.test_data_access \
data_calibration_group.tests.test_data_calibration_group \
data_calibration_group.tests.test_data_calibration_group_no_calibration \
data_location_group.tests.test_data_location_group \
date_gap_filler.tests.test_date_gap_filler \
date_gap_filler_linker.tests.test_date_gap_filler_linker \
directory_filter.tests.test_directory_filter \
egress.tests.test_egress \
event_asset_loader.tests.test_event_asset_loader \
event_location_group.tests.test_event_location_group \
file_joiner.tests.test_file_joiner \
file_joiner.tests.test_file_joiner_app \
grouper.tests.test_grouper \
joiner.tests.test_joiner \
lib.tests.test_data_filename \
lib.tests.test_date_formatter \
lib.tests.test_file_linker \
lib.tests.test_merged_data_filename \
location_active_dates.tests.test_location_active_dates \
location_asset.tests.test_location_asset \
location_group_path.tests.test_location_group_path \
padded_timeseries_analyzer.tests.test_padded_timeseries_analyzer \
parquet_linkmerge.tests.test_parquet_linkmerge \
qaqc_regularized_flag_group.tests.test_qaqc_regularized_flag_group \
related_location_group.tests.test_related_location_group \
timeseries_padder.tests.test_padder_util \
timeseries_padder.tests.test_timeseries_padder \
threshold_loader.tests.test_threshold_loader