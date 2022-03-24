# Define paths
data_path='/scratch/pfs' # Where base repos like avro_schemas, empty_files, etc. are stored
git_path='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir'
source_type='pqs1'
product='parWaterSurface'

# Define paths based on base paths and product information above 
spec_path_l0=$git_path/pipe/l0_data_loader
spec_path_source_type=$git_path/pipe/$source_type
spec_path_product=$git_path/pipe/$product

# Create sensor-specific import_trigger with a small amount of data
pachctl create repo import_trigger_$source_type
pachctl start commit import_trigger_$source_type@master
sites=(BARC FLNT) # Edit as desired
days=(01 02 03) # Edit as desired. Jan 2020 is hardcoded throughout this code
for site in $(echo ${sites[*]}); do 
for day in $(echo ${days[*]}); do 
pachctl put file import_trigger_$source_type@master:/2020/01/$day/$site -f $data_path/import_trigger_FULL/2020/01/$day/$site
done
done
pachctl finish commit import_trigger_$source_type@master

# Load L0 data
# *** First, create/edit/update all pipeline specs ***
pachctl create pipeline -f $spec_path_l0/'data_source_'$source_type'_site.json'
pachctl create pipeline -f $spec_path_l0/'data_source_'$source_type'_linkmerge.yaml'

# Transfer calibrations from pachd1 to pachd1sandbox
pachctl config set active-context pachd1
pachctl1x get file -r calibration@master:/$source_type -o $data_path/calibration/$source_type
pachctl config set active-context pachd1sandbox
pachctl create repo calibration_$source_type
pachctl start commit calibration_$source_type@master
pachctl put file -r calibration_$source_type@master:/$source_type -f $data_path/calibration/$source_type
pachctl finish commit calibration_$source_type@master

# Create daily cron pipeline
# *** First, create/edit/update all pipeline specs ***
pachctl create pipeline -f $spec_path_source_type/'cron_daily_'$source_type'.yaml'
pachctl run cron cron_daily_$source_type
# Wait 10 seconds...
pachctl stop pipeline cron_daily_$source_type

# Create location & threshold pipelines
# *** First, create/edit/update all pipeline specs ***
pachctl create pipeline -f $spec_path_source_type/'location_asset_'$source_type'.yaml'
pachctl create pipeline -f $spec_path_source_type/'location_loader_'$source_type'.yaml'
pachctl create pipeline -f $spec_path_source_type/'threshold_loader_'$source_type'.yaml'

# Create source-type-specific empty_files
pachctl create repo empty_files_$source_type
pachctl start commit empty_files_$source_type@master
pachctl put file -r empty_files_$source_type@master:/$source_type -f $data_path/empty_files/$source_type
pachctl finish commit empty_files_$source_type@master

# Create source-type-specific avro_schemas
pachctl create repo avro_schemas_$source_type
pachctl start commit avro_schemas_$source_type@master
pachctl put file -r avro_schemas_$source_type@master:/ -f $data_path/avro_schemas/
pachctl finish commit avro_schemas_$source_type@master

# Create source-type-specific fdas uncertainty (ONLY IF NEEDED FOR YOUR SOURCE TYPE)
pachctl create repo uncertainty_fdas_$source_type
pachctl start commit uncertainty_fdas_$source_type@master
pachctl put file -r uncertainty_fdas_$source_type@master:/ -f $data_path/uncertainty_fdas/
pachctl finish commit uncertainty_fdas_$source_type@master

# Stand up source_type pipeline
# *** First, create/edit/update all pipeline specs and adjust the list below to include them all in order ***
pachctl create pipeline -f $spec_path_source_type/'data_source_'$source_type'_list_years.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_calibration_assignment.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_data_calibration_group.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_calibration_conversion.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_location_asset_assignment.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_calibrated_location_group.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_location_active_dates_assignment.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_structure_repo_by_location.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_merge_data_by_location.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_date_gap_filler_limiter.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_date_gap_filler.yaml'
pachctl create pipeline -f $spec_path_source_type/$source_type'_date_gap_filler_linker.yaml'

# Set up product pipeline
# *** First, create/edit/update all pipeline specs and adjust the list below to include them all in order ***
pachctl create pipeline -f $spec_path_product/$product'_context_filter.yaml'
pachctl create pipeline -f $spec_path_product/$product'_regularized.yaml'
pachctl create pipeline -f $spec_path_product/$product'_threshold_filter.yaml'
pachctl create pipeline -f $spec_path_product/$product'_threshold_select.yaml'
pachctl create pipeline -f $spec_path_product/$product'_timeseries_padder.yaml'
pachctl create pipeline -f $spec_path_product/$product'_padded_timeseries_analyzer.yaml'
pachctl create pipeline -f $spec_path_product/$product'_qaqc_plausibility.yaml'
pachctl create pipeline -f $spec_path_product/$product'_qaqc_flags_group.yaml'
pachctl create pipeline -f $spec_path_product/$product'_pre_statistics_group.yaml'
pachctl create pipeline -f $spec_path_product/$product'_statistics.yaml'
pachctl create pipeline -f $spec_path_product/$product'_quality_metrics.yaml'
pachctl create pipeline -f $spec_path_product/$product'_level1_group.yaml'

# Bump to full scale for a few days
# *** First - edits the date_gap_filler pipeline spec to remove restriction on particular CFGLOCs. ***
# *** Also make any desired edits to the date_gap_filler_limiter pipeline spec to adjust date range to the dates of the import trigger 
pachctl start transaction
pachctl update pipeline --reprocess -f $spec_path_source_type/$source_type'_date_gap_filler_limiter.yaml'
pachctl update pipeline -f $spec_path_source_type/$source_type'_date_gap_filler.yaml'
pachctl start commit import_trigger_$source_type@master
pachctl finish transaction
for day in $(echo ${days[*]}); do 
pachctl put file -r import_trigger_$source_type@master:/2020/01/$day -f $data_path/import_trigger_FULL/2020/01/$day
done
pachctl finish commit import_trigger_$source_type@master

# Add 1 day at full scale to evaluate resource requests in normal operations
# *** First - edit the date_gap_filler_limiter pipeline spec to add 1 day to the end date ***
pachctl start transaction
pachctl update pipeline --reprocess -f $spec_path_source_type/$source_type'_date_gap_filler_limiter.yaml'
pachctl start commit import_trigger_$source_type@master
pachctl finish transaction
days=(04) # Edit as desired
for day in $(echo ${days[*]}); do 
pachctl put file -r import_trigger_$source_type@master:/2020/01/$day -f $data_path/import_trigger_FULL/2020/01/$day
done
pachctl finish commit import_trigger_$source_type@master




