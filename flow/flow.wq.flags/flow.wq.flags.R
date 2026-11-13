##############################################################################################
#' @title Workflow for low conductivity and buoy profiling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Flags all sensor streams for water quality when pressure is below one.
#' Updates step flags for profiling.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/yyyy/mm/dd/GROUP/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'        
#' 2. "SiteFileBuoy=value", where value is the full path to the site list buoy JSON file.
#' 
#' 3. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 4. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 5. "PathAvro=value" (optional), where value is the full path to the avro schemas for the output flags file. 
#' 
#' 6. "DirSubCopy=value" (optional), where value is a pipe-separated list of subdirectories to be copied over 
#' to the output directory with a symbolic link.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return zeroConductivityQF and buoyProfilingQF.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=~/Git/pfs/waterQuality_flags_specific_group/2025/09/09/water-quality_WALK101100",
#          "SiteFileBuoy=~/Git/pfs/exo2_site_list/site-list-buoy.json",
#          "PathAvro=~/Git/pfs/waterQuality_avro_schemas/waterQuality_flags_specific",
#          "DirOut=~/Git/pfs/out/wq.flags",
#          "DirErr=~/Git/pfs/out/errored_datums")
# rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2026-11-11)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.wq.flags.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
} 
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","DirErr","SiteFileBuoy"),NameParaOptn = c("DirSubCopy","PathAvro"),log = log)


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output flags: ', Para$PathAvro))

# Read in the schemas so we only have to do it once and not every
# time in the avro writer.
if(base::is.null(Para$PathAvro) || Para$PathAvro == 'NA'){
  SchmPath <- NULL
} else {
  SchmPath <- Para$PathAvro
}

# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(
    Para$DirSubCopy,
    c('data')
  ))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))


#what are the expected subdirectories of each input path
nameDirSub <- 'exo2conductivity'
log$debug(base::paste0(
  'Expected subdirectories of each input path include: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn, 
                              nameDirSub = nameDirSub,
                              log = log)
#Determine site from DirIn
Site <- base::sub("^water-quality_([A-Z]+).*", "\\1", base::basename(DirIn))
log$debug(base::paste0('Site determined from input directory: ', Site))

# Read in the site list buoy JSON file
SiteFileBuoyJson <- jsonlite::fromJSON(base::paste0(Para$SiteFileBuoy, collapse = ''))

# Check if site is in the buoy site list
if (Site %in% SiteFileBuoyJson$site) {
  BuoySite <- TRUE
} else {
  BuoySite <- FALSE
}



# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.wq.flags(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmPath=SchmPath,
        DirSubCopy=DirSubCopy,
        BuoySite=BuoySite,
        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        
        # Re-route the failed datum
        NEONprocIS.base::def.err.datm(
          err=err,
          call.stack=call.stack,
          DirDatm=idxDirIn,
          DirErrBase=Para$DirErr,
          RmvDatmOut=TRUE,
          DirOutBase=Para$DirOut,
          log=log
        )
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
  
  return()
}
