#!/bin/bash
# This can be run programmatically.
# This is an end-to-end example of deleting all the pipelines in a product DAG. Note that the base repos 
# such as calibration are not removed. It relies on the pipe_list_<source type>.txt and pipe_list_<product>.txt
# files in the respective directories to know which pipelines to remove. 
# IMPORTANT: The pipe_list files MUST have a final newline at the end of the file. 
# If you don't want to remove particular edge pipelines, comment them out (using preceding # symbol) in the 
# pipe_list file. 
# Note that this is a simple scenario in which a single source type feeds into a single product. If multiple
# source types or products are involved, repeat the steps as necessary.


# Define paths
git_path_pipelines='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir/pipe'
source_type='aquatroll200'
product='surfacewaterPhysical'
pipe_list_prefix='pipe_list_'
ext='.yaml' # file extension for pipeline specs specified in the pipe_list files. Must be consistent.

# Define paths based on base paths and product information above 
spec_path_source_type=$git_path_pipelines/$source_type
spec_path_product=$git_path_pipelines/$product

# Remove product DAG
# Read in the pipelines (in reverse order) for this product and delete them
# The (ordered) list of pipeline files should be located in the file pipe_list_<PRODUCT>.txt in the 
# directory of pipeline specs for the data product
unset pipelines
pipelines=`tac $spec_path_product/$pipe_list_prefix$product.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl delete pipeline ${pipe/$ext}
pachctl delete pipeline ${pipe/$ext}
done

# Remove source type DAG
# Read in the pipelines (in reverse order) for this product and delete them
# The (ordered) list of pipeline files should be located in the file pipe_list_<SOURCE TYPE>.txt in the 
# directory of pipeline specs for the data product
unset pipelines
pipelines=`tac $spec_path_source_type/$pipe_list_prefix$source_type.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pachctl delete pipeline ${pipe/$ext}
pachctl delete pipeline ${pipe/$ext}
done



