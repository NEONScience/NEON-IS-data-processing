#!/bin/bash
# *** Run interactively ***
# This is an end-to-end example of updating a full product DAG within a transaction. 
# This will result in a single job.
# Note that this is a simple scenario in which a single source type feeds into a product. If multiple
# source types feed into the product, repeat the steps that load in L0 data, metadata, and pipeline specifications
# for each relevant source type.
# Also note that if this script errors without finishing, you may need to finish the transaction manually (pc finish transaction)


# Define paths
git_path_pipelines='/home/NEON/csturtevant/R/NEON-IS-data-processing-homeDir/pipe'
pipe_list_prefix='pipe_list_' # The prefix to the file name in each pipe folder that lists the pipelines in the DAG
source_type='li191r'
product='parQuantumLine'
reprocess_flag="" # Use "--reprocess" to reprocess the pipelines, or use "" to update without reprocessing

# Define paths based on base paths and product information above 
spec_path_source_type=$git_path_pipelines/$source_type
spec_path_product=$git_path_pipelines/$product

# Do all this in one transaction
pc start transaction

# Set up source type pipeline
# Read in the pipelines (in order) for this source type and update them up
# The (ordered) list of pipeline files should be located in the file pipe_list_SOURCETYPE.txt in the 
# directory of pipeline specs for the source type
unset pipelines
pipelines=`cat $spec_path_source_type/$pipe_list_prefix$source_type.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pc update pipeline $reprocess_flag -f $spec_path_source_type/$pipe
pc update pipeline $reprocess_flag -f $spec_path_source_type/$pipe
done

# Set up product pipeline
# Read in the pipelines (in order) for this product and update them up
# The (ordered) list of pipeline files should be located in the file pipe_list_PRODUCT.txt in the 
# directory of pipeline specs for the data product
unset pipelines
pipelines=`cat $spec_path_product/$pipe_list_prefix$product.txt`
for pipe in $(echo ${pipelines[*]}); do
echo pc update pipeline $reprocess_flag -f $spec_path_product/$pipe
pc update pipeline $reprocess_flag -f $spec_path_product/$pipe
done

pc finish transaction

