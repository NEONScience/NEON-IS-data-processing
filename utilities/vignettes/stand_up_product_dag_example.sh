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
pc put file $source_type'_site_list'@master:/site-list.json -f $spec_path_source_type/site-list.json
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





# #NC troll pipelines
# data_path='/scratch/pfs'
# git_path_pipelines='/home/NEON/ncatolico/R/NEON-IS-data-processing/pipe'
# git_path_avro='/home/NEON/ncatolico/R/NEON-IS-avro-schemas'
# pipe_list_prefix='pipe_list_'
# source_type1='aquatroll200'
# source_type2='leveltroll500'
# product1='groundwaterPhysical'
# product2='surfacewaterPhysical'
# 
# # Define paths based on base paths and product information above 
# spec_path_source_type1=$git_path_pipelines/$source_type1
# spec_path_source_type2=$git_path_pipelines/$source_type2
# spec_path_product1=$git_path_pipelines/$product1
# spec_path_product2=$git_path_pipelines/$product2
# 
# # Create site list (controls sites to run and indicates kafka start dates). The site-list.json should be located
# pc create repo $source_type1'_site_list'
# pc start commit $source_type1'_site_list'@master
# pc put file $source_type1'_site_list'@master:/site-list.json -f $spec_path_source_type1/site-list.json
# pc finish commit $source_type1'_site_list'@master
# pc create repo $source_type2'_site_list'
# pc start commit $source_type2'_site_list'@master
# pc put file $source_type2'_site_list'@master:/site-list.json -f $spec_path_source_type2/site-list.json
# pc finish commit $source_type2'_site_list'@master
# 
# # Create source-type-specific empty_files
# pc create repo $source_type1'_empty_files'
# pc start commit $source_type1'_empty_files'@master
# pc put file -r $source_type1'_empty_files'@master:/$source_type1 -f $git_path_avro/empty_files/$source_type1
# pc finish commit $source_type1'_empty_files'@master
# pc create repo $source_type2'_empty_files'
# pc start commit $source_type2'_empty_files'@master
# pc put file -r $source_type2'_empty_files'@master:/$source_type2 -f $git_path_avro/empty_files/$source_type2
# pc finish commit $source_type2'_empty_files'@master
# 
# # Create source-type-specific avro_schemas
# pc create repo $source_type1'_avro_schemas'
# pc start commit $source_type1'_avro_schemas'@master
# pc put file -r $source_type1'_avro_schemas'@master:/$source_type1 -f $git_path_avro/avro_schemas/$source_type1
# pc finish commit $source_type1'_avro_schemas'@master
# pc create repo $source_type2'_avro_schemas'
# pc start commit $source_type2'_avro_schemas'@master
# pc put file -r $source_type2'_avro_schemas'@master:/$source_type2 -f $git_path_avro/avro_schemas/$source_type2
# pc finish commit $source_type2'_avro_schemas'@master
# pc create repo 'troll_shared_avro_schemas'
# pc start commit 'troll_shared_avro_schemas'@master
# pc put file -r 'troll_shared_avro_schemas'@master:/troll_shared -f $git_path_avro/avro_schemas/troll_shared
# pc finish commit 'troll_shared_avro_schemas'@master
# 
# # Create product1-specific avro_schemas
# pc create repo $product1'_avro_schemas'
# pc start commit $product1'_avro_schemas'@master
# pc put file -r $product1'_avro_schemas'@master:/$product1 -f $git_path_avro/avro_schemas/$product1
# pc finish commit $product1'_avro_schemas'@master
# pc create repo $product2'_avro_schemas'
# pc start commit $product2'_avro_schemas'@master
# pc put file -r $product2'_avro_schemas'@master:/$product2 -f $git_path_avro/avro_schemas/$product2
# pc finish commit $product2'_avro_schemas'@master
# 
# # Set up source type pipeline
# unset pipelines
# pipelines=`cat $spec_path_source_type1/$pipe_list_prefix$source_type1.txt`
# for pipe in $(echo ${pipelines[*]}); do
# echo pc create pipeline -f $spec_path_source_type1/$pipe
# pc create pipeline -f $spec_path_source_type1/$pipe
# done
# unset pipelines
# pipelines=`cat $spec_path_source_type2/$pipe_list_prefix$source_type2.txt`
# for pipe in $(echo ${pipelines[*]}); do
# echo pc create pipeline -f $spec_path_source_type2/$pipe
# pc create pipeline -f $spec_path_source_type2/$pipe
# done
# 
# # Set up product1 pipeline
# unset pipelines
# pipelines=`cat $spec_path_product1/$pipe_list_prefix$product1.txt`
# for pipe in $(echo ${pipelines[*]}); do
# echo pc create pipeline -f $spec_path_product1/$pipe
# pc create pipeline -f $spec_path_product1/$pipe
# done
# unset pipelines
# pipelines=`cat $spec_path_product2/$pipe_list_prefix$product2.txt`
# for pipe in $(echo ${pipelines[*]}); do
# echo pc create pipeline -f $spec_path_product2/$pipe
# pc create pipeline -f $spec_path_product2/$pipe
# done
# 
# # Now run the daily cron pipeline to initialize it. q
# pc run cron $source_type1'_cron_daily_and_date_control'
# pc run cron $source_type2'_cron_daily_and_date_control'
# 
# # Bump to full scale for a few days
# pc start transaction ##aquatroll and groundwater and leveltroll and surface water
# pc update pipeline --reprocess -f $spec_path_source_type1/$source_type1'_cron_daily_and_date_control.yaml'
# pc update pipeline --reprocess -f $spec_path_source_type2/$source_type2'_cron_daily_and_date_control.yaml'
# pc update pipeline --reprocess -f $spec_path_product1/$product1'_cron_monthly_and_pub_control.yaml'
# pc update pipeline --reprocess -f $spec_path_product2/$product2'_cron_monthly_and_pub_control.yaml'
# pc finish transaction




