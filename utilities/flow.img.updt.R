# Run either interactively, or from the command line

# Update image references to a new tag throughout the repo
# If a Dockerfile is found that references the image, and that dockerfile is in the /flow, /modules, or /modules_combined
# directories, the build_tag_push_update.sh script found in the same directory will be run, which should include building 
# the image for that module, tagging it with the SHA of the HEAD commit, pushing the new image to quay.io, and updating any 
# yaml files in the repo that reference the image with the new image tag. Note that that docker images built from the 
# /pack directory, and any dependents also in the /pack directory, must be built by hand.


# Pull in command line arguments if available (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)
ImgBld <- TRUE # Build,tag, push downstream module images and update pipeline specs?

# Run interatively without command line arguments
if(length(arg) == 0){
  pathBgn <- '~/R/NEON-IS-data-processing/'
  typeFile = 'Dockerfile' # also, e.g. ".yaml"
  imgBase <- "quay.io/battelleecology/neon-is-troll-uncertainty-r" # repo and base name without the tag (e.g. quay.io/battelleecology/neon-is-base-r) 
  tagNew <- "v1.0.3"
} else {
  pathBgn = arg[1]
  typeFile = arg[2]
  imgBase = arg[3]
  tagNew = arg[4]
}


setwd(pathBgn)
imgGrep <- gsub(pattern='.',replacement='\\.',x=imgBase,fixed=TRUE)
imgGrep <- gsub(pattern='/',replacement='\\/',x=imgGrep,fixed=TRUE)
imgGrep <- paste0(imgGrep,'\\:\\S+')
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

