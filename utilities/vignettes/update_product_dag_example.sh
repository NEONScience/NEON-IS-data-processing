#!/bin/bash
# This can be run programmatically, but best to run interactively.
# This is an end-to-end example of updating all the pipelines in a product DAG, with or without reprocessing.
# Note that this is a simple scenario in which a single source type feeds into a product. If multiple
# source types feed into the product, run/adjust/rerun the steps as necessary.

# Select a reprocessing option by uncommenting whichever you want.
reprocess_flag='' # No-op update to all pipelines (changes applied to next run so long as datum specifications are consistent)
#reprocess_flag='--reprocess' # Reprocess all pipelines

# Define paths
data_path='/scratch/pfs' # Where base repos like avro_schemas, empty_files, etc. are stored
git_path_pipelines='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir'
source_type='hmp155'
product='relHumidity'

# Define paths based on base paths and product information above 
spec_path_source_type=$git_path_pipelines/pipe/$source_type
spec_path_product=$git_path_pipelines/pipe/$product

pachctl start transaction

# Set up source type pipeline
# Read in the pipelines (in order) for this source type and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_SOURCETYPE.txt in the 
# directory of pipeline specs for the source type
unset pipelines
pipelines=`cat $spec_path_source_type/pipe_list_$source_type.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl update pipeline $reprocess_flag -f $spec_path_source_type/$pipe
pachctl update pipeline $reprocess_flag -f $spec_path_source_type/$pipe
done


# Set up product pipeline
# Read in the pipelines (in order) for this product and stand them up
# The (ordered) list of pipeline files should be located in the file pipe_list_PRODUCT.txt in the 
# directory of pipeline specs for the data product
unset pipelines
pipelines=`cat $spec_path_product/pipe_list_$product.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl update pipeline $reprocess_flag -f $spec_path_product/$pipe
pachctl update pipeline $reprocess_flag -f $spec_path_product/$pipe
done


pachctl finish transaction

