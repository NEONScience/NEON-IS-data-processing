# Run either interactively, or from the command line. Must be run from repository root (e.g "~/R/NEON-IS-data-processing-homeDir")
# Example run from command line:
# Rscript ./flow.img.bld.all.R "flow" "TRUE"


# Run all build_tag_push_update scripts found in either the flow, modules, or modules_combined directories. 
# Update image references in the pipe directory with the new tag. 


# Pull in command line arguments if available (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Run interatively without command line arguments
if(length(arg) == 0){
  DirBld <- 'modules'
  UpdtPipe <- TRUE
} else {
  DirBld <- arg[1]
  UpdtPipe <- as.logical(arg[2])
}

# Find all build_tag_push_update.sh scripts
file <- base::list.files(paste0('./',DirBld),pattern='build_tag_push_update.sh',full.names = TRUE, recursive=TRUE)

# Clear the docker build cache
system("docker builder prune -f --all")

# rebuild each image and update the yaml files
for(idxFile in file){
  pathPrnt <- fs::path_dir(fs::path(idxFile))
  pathDockBld <- fs::path(pathPrnt,'build_tag_push_update.sh')
  if(fs::file_exists(pathDockBld)){
    # Build the image and update all the pipeline specs. Note: Uses the latest commit SHA to tag the image. 
    # Make sure all edits are included in the latest commit (i.e. might need to run this after package updates are committed) 
    system(pathDockBld)
  }
}

