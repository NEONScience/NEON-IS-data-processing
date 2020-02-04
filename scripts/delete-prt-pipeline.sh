pachctl start transaction

pachctl delete pipeline  prt_fill_date_gaps_by_location
pachctl delete pipeline  prt_merge_data_by_location
pachctl delete pipeline  prt_structure_repo_by_location
pachctl delete pipeline  prt_location_filter
pachctl delete pipeline  prt_calibration_location_group
pachctl delete pipeline  prt_calibration_conversion
pachctl delete pipeline  prt_calibration_filter
pachctl delete pipeline  prt_data_calibration_group

pachctl finish transaction
