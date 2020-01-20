pachctl start transaction

pachctl create pipeline -f ../pipe/prt_data_calibration_group/prt_data_calibration_group.json
pachctl create pipeline -f ../pipe/prt_calibration_filter/prt_calibration_filter.json
pachctl create pipeline -f ../pipe/prt_calibration_conversion/prt_calibration_conversion.json
pachctl create pipeline -f ../pipe/prt_calibrated_location_group/prt_calibrated_location_group.json
pachctl create pipeline -f ../pipe/prt_location_filter/prt_location_filter.json
pachctl create pipeline -f ../pipe/prt_structure_repo_by_location/prt_structure_repo_by_location.json
pachctl create pipeline -f ../pipe/prt_merge_data_by_location/prt_merge_data_by_location.json
pachctl create pipeline -f ../pipe/prt_fill_date_gaps_by_location/prt_fill_date_gaps_by_location.json

pachctl finish transaction
