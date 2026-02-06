##############################################################################################
#' @title Workflow for insufficient data calculations

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Uses number of measuremnts in averaging window to determine whether insufficient  
#' data quality flag should be applied. 
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the statistics data (including number of points) and the QM data.
#' 
#' 2. "numPoints=value", List of the name(s) of the field(s) in the input data frame containing 
#' the number of points. Currently set in the yaml. 
#' 
#' 3. "minPoints=value", List of the corresponding minimum number of points required to not trigger 
#' the insufficient data quality flag.Currently set in the yaml. 
#'
#' 4. "insuffQFnames=value", List of the names of the corresponding insufficient data QF's in the output 
#' data frame that should be triggered if less than minPoints. Currently set in the yaml. 
#'
#' 5. "minPoints=value", List of the names of the corresponding final data QF's in the output data frame 
#' that should be triggered if the insufficient data QF is triggered. Currently set in the yaml. 
#'
#' 6. "DirOut=value", The base file path for the output data.
#' 
#' 7. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 8. "SchmStats=value" (optional), The avro schema for the input and output stats file.
#' 
#' 9. "SchmQMs=value" (optional), The avro schema for the updated QMs (insufficientDataQF added).   
#' 
#' 10. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, that are to be copied with a symbolic link to the output path. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Updated stats and QMs data files in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' flow.qf.insuff.data <- function(DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733",                        
#'                               numPoints=c("nitrateNumPts"),
#'                               minPoints=c(5),
#'                               insuffQFnames=c("nitrateInsufficientDataQF"),
#'                               finalQFnames=c("nitrateFinalQF"),
#'                               DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" ,
#'                               SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse=''), 
#'                               SchmQMs<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_quality_metrics.avsc'),collapse=''),
#'                               log=log)
#' Stepping through the code in R studio                               
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
 arg <- c("DirIn=~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733",
           "numPoints=c('nitrateNumPts')","minPoints=c(5)","insuffQFnames=c('nitrateInsufficientDataQF')","finalQFnames=c('nitrateFinalQF')",
          "DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums","DirSubCopy=location",
          "SchmQMs=~/pfs/nitrate_avro_schemas/nitrate/nitrate_insufficient_data.avsc")
# rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2025-10-31)
#' Initial creation.
#' Nora Catolico (2025-11-04)
#' add in copied directories
#' Nora Catolico (2025-12-11)
#' fix schema outputs
#' Bobby Hensley (2026-02-05)
#' Updated to test multiple variables.
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.qf.insuff.data.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","numPoints","minPoints","insuffQFnames","finalQFnames","DirOut","DirErr"),
                                      NameParaOptn = c("SchmStats","SchmQMs","DirSubCopy"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Number of points: ', Para$numPoints))
log$debug(base::paste0('Minimum points: ', Para$minPoints))
log$debug(base::paste0('Insufficient QF names: ', Para$insuffQFnames))
log$debug(base::paste0('Final QF names: ', Para$finalQFnames))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output stats: ', Para$SchmStats))
log$debug(base::paste0('Schema for output QMs: ', Para$SchmQMs))
log$debug(base::paste0('Director to copy: ', Para$DirSubCopy))

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$SchmStats) || Para$SchmStats == 'NA'){
  SchmStats <- NULL
} else {
  SchmStats <- base::paste0(base::readLines(Para$SchmStats),collapse='')
}
if(base::is.null(Para$SchmQMs) || Para$SchmQMs == 'NA'){
  SchmQMs <- NULL
} else {
  SchmQMs <- base::paste0(base::readLines(Para$SchmQMs),collapse='')
}


# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = c('stats','quality_metrics'),
                              log = log)

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'stats'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.qf.insuff.data(
        DirIn=idxFileIn,
        numPoints=Para$numPoints,
        minPoints=Para$minPoints,
        insuffQFnames=Para$insuffQFnames,
        finalQFnames=Para$finalQFnames,
        DirOutBase=Para$DirOut,
        SchmStats=SchmStats,
        SchmQMs=SchmQMs,
        DirSubCopy=DirSubCopy,
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




