#!/bin/bash

# Generate the tempAirSingle pipeline. The prt pipeline must be created before this pipeline.

# Prerequisite pipelines: data_source_windobserverii, data_source_prt, calibration, data_source_dualfan,
# avro_schemas, threshold

pachctl start transaction

pachctl create pipeline -f ../pipe/windobserverii_data_location_group/windobserverii_data_location_group.json
pachctl create pipeline -f ../pipe/heater_data_location_group/heater_data_location_group.json
pachctl create pipeline -f ../pipe/dualfan_data_location_group/dualfan_data_location_group.json
pachctl create pipeline -f ../pipe/windobserverii_location_filter/windobserverii_location_filter.json
pachctl create pipeline -f ../pipe/dualfan_location_filter/dualfan_location_filter.json
pachctl create pipeline -f ../pipe/windobserverii_structure_repo_by_location/windobserverii_structure_repo_by_location.json
pachctl create pipeline -f ../pipe/dualfan_structure_repo_by_location/dualfan_structure_repo_by_location.json
pachctl create pipeline -f ../pipe/windobserverii_merge_data_by_location/windobserverii_merge_data_by_location.json
pachctl create pipeline -f ../pipe/dualfan_merge_data_by_location/dualfan_merge_data_by_location.json
pachctl create pipeline -f ../pipe/tempAirSingle_prt_group_path/tempAirSingle_prt_group_path.json
pachctl create pipeline -f ../pipe/tempAirSingle_windobserverii_group_path/tempAirSingle_windobserverii_group_path.json
pachctl create pipeline -f ../pipe/tempAirSingle_heater_group_path/tempAirSingle_heater_group_path.json
pachctl create pipeline -f ../pipe/tempAirSingle_dualfan_group_path/tempAirSingle_dualfan_group_path.json
pachctl create pipeline -f ../pipe/tempAirSingle_related_location_group/tempAirSingle_related_location_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_dualfan.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_prt.json
pachctl create pipeline -f ../pipe/tempAirSingle_threshold_filter/tempAirSingle_threshold_filter.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_locations.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_calibrated_data.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_calibrated_flags.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_uncertainty_fdas.json
pachctl create pipeline -f ../pipe/tempAirSingle_threshold_select/tempAirSingle_threshold_select.json
pachctl create pipeline -f ../pipe/tempAirSingle_regularized/tempAirSingle_regularized_data.json
pachctl create pipeline -f ../pipe/tempAirSingle_threshold_regularized_group/tempAirSingle_threshold_regularized_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_windobserverii.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_heater.json
pachctl create pipeline -f ../pipe/tempAirSingle_timeseries_padder/tempAirSingle_timeseries_padder.json
pachctl create pipeline -f ../pipe/tempAirSingle_qaqc_specific_group/tempAirSingle_qaqc_specific_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_uncertainty_coefficients.json
pachctl create pipeline -f ../pipe/tempAirSingle_padded_timeseries_analyzer/tempAirSingle_padded_timeseries_analyzer.json
pachctl create pipeline -f ../pipe/tempAirSingle_qaqc_specific/tempAirSingle_qaqc_specific.json
pachctl create pipeline -f ../pipe/tempAirSingle_regularized/tempAirSingle_regularized_uncertainty_fdas.json
pachctl create pipeline -f ../pipe/tempAirSingle_qaqc_plausibility/tempAirSingle_qaqc_plausibility.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_qaqc_specific_flags.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_qaqc_specific_data.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_qaqc_plausibility_flags.json
pachctl create pipeline -f ../pipe/tempAirSingle_regularized/tempAirSingle_regularized_flags.json
pachctl create pipeline -f ../pipe/tempAirSingle_directory_filter/tempAirSingle_qaqc_plausibility_data.json
pachctl create pipeline -f ../pipe/tempAirSingle_qaqc_flags_group/tempAirSingle_qaqc_flags_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_qaqc_data_group/tempAirSingle_qaqc_data_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_quality_metrics/tempAirSingle_quality_metrics.json
pachctl create pipeline -f ../pipe/tempAirSingle_merge_qaqc_data/tempAirSingle_merge_qaqc_data.json
pachctl create pipeline -f ../pipe/tempAirSingle_statistics_uncertainty_group/tempAirSingle_statistics_uncertainty_group.json
pachctl create pipeline -f ../pipe/tempAirSingle_statistics/tempAirSingle_statistics.json
pachctl create pipeline -f ../pipe/tempAirSingle_level1_group/tempAirSingle_level1_group.json

pachctl finish transaction
