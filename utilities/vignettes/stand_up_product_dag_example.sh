#!/bin/bash
# *** Run interactively ***
# This is an end-to-end example of standing up a full product DAG, from converting L0 data from trino
# all the way through creating L1 output.
# Note that this is a simple scenario in which a single source type feeds into a product. If multiple
# source types feed into the product, repeat the steps that load in L0 data, metadata, and pipeline specifications
# for each relevant source type.


# Define paths
data_path='/scratch/pfs' # Where base repos like avro_schemas, empty_files, etc. are stored
git_path_pipelines='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir/pipe'
git_path_avro='/home/NEON/csturtevant/R/NEON-IS-avro-schemas'
git_path_avro_l0='/home/NEON/csturtevant/R/avro-schemas'
pipe_list_prefix='pipe_list_'
source_type='prt'
product='tempSoil'

# Define paths based on base paths and product information above 
spec_path_source_type=$git_path_pipelines/$source_type
spec_path_product=$git_path_pipelines/$product

# Create site list (controls sites to run and indicates kafka start dates). The site-list.json should be located
# along with the pipeline specs for the source type.
# Make sure cron_daily_and_date_control pipeline uses the correct file name
pc create repo $source_type'_site_list'
pc start commit $source_type'_site_list'@master
pc put file $source_type'_site_list'@master:/site-list.json -f spec_path_source_type/site-list.json
pc finish commit $source_type'_site_list'@master

# Create source-type-specific empty_files
pc create repo $source_type'_empty_files'
pc start commit $source_type'_empty_files'@master
pc put file -r $source_type'_empty_files'@master:/$source_type -f $git_path_avro/empty_files/$source_type
pc finish commit $source_type'_empty_files'@master

# Create source-type-specific avro_schemas
pc create repo $source_type'_avro_schemas'
pc start commit $source_type'_avro_schemas'@master
pc put file -r $source_type'_avro_schemas'@master:/$source_type -f $git_path_avro/avro_schemas/$source_type
pc put file $source_type'_avro_schemas'@master:/$source_type/$source_type.avsc -f $git_path_avro_l0/schemas/$source_type/$source_type.avsc
pc finish commit $source_type'_avro_schemas'@master

# Create source-type-specific fdas uncertainty (ONLY IF NEEDED FOR YOUR SOURCE TYPE)
pc create repo $source_type'_uncertainty_fdas'
pc start commit $source_type'_uncertainty_fdas'@master
pc put file -r $source_type'_uncertainty_fdas'@master:/ -f $data_path/uncertainty_fdas/
pc finish commit $source_type'_uncertainty_fdas'@master

# Create product-specific avro_schemas
pc create repo $product'_avro_schemas'
pc start commit $product'_avro_schemas'@master
pc put file -r $product'_avro_schemas'@master:/$product -f $git_path_avro/avro_schemas/$product
pc finish commit $product'_avro_schemas'@master

# Set up source type pipeline
# Read in the pipelines (in order) for this source type and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_SOURCETYPE.txt in the 
# directory of pipeline specs for the source type
unset pipelines
pipelines=`cat $spec_path_source_type/$pipe_list_prefix$source_type.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pc create pipeline -f $spec_path_source_type/$pipe
pc create pipeline -f $spec_path_source_type/$pipe
done

# Set up product pipeline
# Read in the pipelines (in order) for this product and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_PRODUCT.txt in the 
# directory of pipeline specs for the data product
unset pipelines
pipelines=`cat $spec_path_product/$pipe_list_prefix$product.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pc create pipeline -f $spec_path_product/$pipe
pc create pipeline -f $spec_path_product/$pipe
done

# Now run the daily cron pipeline to initialize it. q
pc run cron $source_type'_cron_daily_and_date_control'

# Before proceeding...
# Check that everything has run properly for the few locations and days loaded. Verify the output.


# Bump to full scale for a few days
# *** First - edit the date_gap_filler pipeline spec to remove restriction on particular CFGLOCs. ***
# *** Also make any desired edits to the cron_daily_and_date_control pipeline and cron_monthly_and_pub_control
# pipeline specs to adjust date ranges 
pc start transaction
pc update pipeline --reprocess -f $spec_path_source_type/$source_type'_cron_daily_and_date_control.yaml'
pc update pipeline -f $spec_path_source_type/$source_type'_fill_date_gaps_and_regularize.yaml'
pc update pipeline --reprocess -f $spec_path_product/$product'_cron_monthly_and_pub_control.yaml'
pc finish transaction

# Add 1 day at full scale to evaluate resource requests in normal operations
# *** First - edit the cron_daily_and_date_control pipeline spec to add 1 day to the end date ***
pc update pipeline --reprocess -f $spec_path_source_type/$source_type'_cron_daily_and_date_control.yaml'

