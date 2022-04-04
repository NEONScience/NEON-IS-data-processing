#!/bin/bash
# Run interactively

# Define paths
data_path='/scratch/pfs' # Where base repos like avro_schemas, empty_files, etc. are stored
git_path='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir'
source_type='windobserverii'
product='pressureAir'

# Define paths based on base paths and product information above 
spec_path_l0=$git_path/pipe/l0_data_loader
spec_path_source_type=$git_path/pipe/$source_type
spec_path_product=$git_path/pipe/$product

# Create sensor-specific import_trigger with a small amount of data
pachctl create repo import_trigger_$source_type
pachctl start commit import_trigger_$source_type@master
sites=(CPER HARV BARC ARIK) # Edit as desired
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

# Set up source type pipeline
# Read in the pipelines (in order) for this source type and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_SOURCETYPE.txt in the 
# directory of pipeline specs for the source type
pipelines=`cat $spec_path_source_type/pipe_list_$source_type.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl create pipeline -f $spec_path_source_type/$pipe
pachctl create pipeline -f $spec_path_source_type/$pipe
done

# Now run the daily cron pipeline to initialize it.
# Then stop it almost immediately after (so it doesn't run every day until we want it to)
pachctl run cron cron_daily_$source_type
# Wait 10 seconds...
pachctl stop pipeline cron_daily_$source_type


# Set up product pipeline
# Read in the pipelines (in order) for this product and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_PRODUCT.txt in the 
# directory of pipeline specs for the data product
pipelines=`cat $spec_path_product/pipe_list_$product.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl create pipeline -f $spec_path_product/$pipe
pachctl create pipeline -f $spec_path_product/$pipe
done

# Before proceeding...
# Check that everything has run properly for the few locations and days loaded. Verify the output.


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




