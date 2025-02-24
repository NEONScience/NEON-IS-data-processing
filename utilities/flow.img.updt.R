# Run either interactively, or from the command line
# Example run from command line:
# Rscript ./flow.img.updt.R "~/R/NEON-IS-data-processing-homeDir/" "Dockerfile" "quay.io/battelleecology/neon-is-base-r" "v1.1.5" "TRUE"


# Update image references to a new tag throughout the repo
# If ImgBld = TRUE and a Dockerfile is found that references the image in the /flow, /modules, or /modules_combined
# directories, the build_tag_push_update.sh script found in the same directory will be run, which should include building 
# the image for that module, tagging it with the SHA of the HEAD commit, pushing the new image to quay.io, and updating any 
# yaml files in the repo that reference the image with the new image tag. Note that that docker images built from the 
# /pack directory, and any dependents also in the /pack directory, must be built by hand.


# Pull in command line arguments if available (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Run interatively without command line arguments
if(length(arg) == 0){
  pathBgn <- '~/R/NEON-IS-data-processing-homeDir/pipe/'
  typeFile <- 'json' # also, e.g. 'Dockerfile' or ".yaml"
  imgBase <- "us-central1-docker.pkg.dev/neon-shared-service/neonscience/neon-is-context-filter" # repo and base name without the tag (e.g. quay.io/battelleecology/neon-is-base-r) 
  tagNew <- "v3.0.0"
  ImgBld <- FALSE # Build,tag, push downstream module images and update pipeline specs?
} else {
  pathBgn <- arg[1]
  typeFile <- arg[2]
  imgBase <- arg[3]
  tagNew <- arg[4]
  ImgBld <- as.logical(arg[5]) # Build,tag, push downstream module images and update pipeline specs?
  
}

# Default image build to false if not specified
if (is.na(ImgBld)){
  ImgBld <- FALSE
}

setwd(pathBgn)
imgGrep <- gsub(pattern='.',replacement='\\.',x=imgBase,fixed=TRUE)
imgGrep <- gsub(pattern='/',replacement='\\/',x=imgGrep,fixed=TRUE)
# imgGrep <- paste0(imgGrep,'\\:\\S+')
imgGrep <- paste0(imgGrep,'\\:(*[^\\"])+')
imgRepl <- paste0(imgBase,':',tagNew)

# Find all of the indicated file type
file <- base::list.files('.',pattern=typeFile,full.names = TRUE, recursive=TRUE)

# Go through each file, searching for the image 
for(idxFile in file){

  text <- base::readLines(idxFile)
  textEdit <- base::grepl(pattern=imgGrep,x=text)
  
  if(sum(textEdit) > 0){
    message(idxFile)
  }
  
  for(idxEdit in which(textEdit)){
    textNew <- base::sub(pattern=imgGrep,replacement = imgRepl,x=text[idxEdit])
    print(paste(text[idxEdit],' -> ',textNew))
    text[idxEdit] <- textNew 
  }
  
  if(sum(textEdit) > 0){
    base::write(x=text,file=idxFile)
    print(paste0('Wrote updated file: ',idxFile))

    # If this is a flow, modules, or modules_combined script, rebuild the image and update the yaml files
    if(ImgBld == TRUE && 
       (grepl(pattern="./flow",x=idxFile,fixed=TRUE) || 
       grepl(pattern="./modules",x=idxFile,fixed=TRUE) ||
       grepl(pattern="./modules_combined",x=idxFile,fixed=TRUE))){
        pathPrnt <- fs::path_dir(fs::path(idxFile))
        pathDockBld <- fs::path(pathPrnt,'build_tag_push_update.sh')
        if(fs::file_exists(pathDockBld)){
          # Build the image and update all the pipeline specs. Note: Uses the latest commit SHA to tag the image. 
          # Make sure all edits are included in the latest commit (i.e. might need to run this after package updates are committed) 
          system(pathDockBld)
        }
    }
    
  }
}

