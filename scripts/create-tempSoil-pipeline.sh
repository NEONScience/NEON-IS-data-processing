#!/bin/bash

# Generate the ../pipe/tempSoil pipeline. The prt pipeline must exist before creating this pipeline.

pachctl start transaction

pachctl create pipeline -f ../pipe/tempSoil_context_group/tempSoil_context_group.json
pachctl create pipeline -f ../pipe/tempSoil_threshold_filter/tempSoil_threshold_filter.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_uncertainty_data.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_locations.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_calibrated_data.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_calibrated_flags.json
pachctl create pipeline -f ../pipe/tempSoil_threshold_select/tempSoil_threshold_select.json
pachctl create pipeline -f ../pipe/tempSoil_regularized/tempSoil_regularized_data.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_uncertainty_coefficients.json
pachctl create pipeline -f ../pipe/tempSoil_threshold_regularized_group/tempSoil_threshold_regularized_group.json
pachctl create pipeline -f ../pipe/tempSoil_regularized/tempSoil_regularized_uncertainty_data.json
pachctl create pipeline -f ../pipe/tempSoil_timeseries_padder/tempSoil_timeseries_padder.json
pachctl create pipeline -f ../pipe/tempSoil_padded_timeseries_analyzer/tempSoil_padded_timeseries_analyzer.json
pachctl create pipeline -f ../pipe/tempSoil_regularized/tempSoil_regularized_flags.json
pachctl create pipeline -f ../pipe/tempSoil_qaqc_plausibility/tempSoil_qaqc_plausibility.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_qaqc_data.json
pachctl create pipeline -f ../pipe/tempSoil_directory_filter/tempSoil_qaqc_flags.json
pachctl create pipeline -f ../pipe/tempSoil_statistics_uncertainty_group/tempSoil_statistics_uncertainty_group.json
pachctl create pipeline -f ../pipe/tempSoil_qaqc_regularized_flag_group/tempSoil_qaqc_regularized_flag_group.json
pachctl create pipeline -f ../pipe/tempSoil_statistics/tempSoil_statistics.json
pachctl create pipeline -f ../pipe/tempSoil_quality_metrics/tempSoil_quality_metrics.json
pachctl create pipeline -f ../pipe/tempSoil_level1_group/tempSoil_level1_group.json

pachctl finish transaction