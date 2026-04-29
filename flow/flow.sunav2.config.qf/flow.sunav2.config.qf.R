##############################################################################################
#' @title Workflow for SUNA configuration quality flag

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow.  Adds a quality flag to SUNA data when more than the
#' expected number of points are present, indicating that the sensor was configured in
#' continuous rather than periodic mode). The main purpose is to flag field cleaning and 
#' calibration measurements taken in DI.  This flag will also trigger the final QF.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the input stats and qm files. 
#' 
#' 2. "DirOutBase=value", The base file path for the output stats and qm files.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#'  
#' 4. "SchmStats=value" (optional), The avro schema for the stats file.
#' 
#' 5. "SchmQMs=value" (optional), The avro schema for the QM file.   
#' 
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, that are to be copied with a symbolic link to the output path. 
#' 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Sensor-specific quality flag files in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' flow.sunav2.config.qf <- function(DirIn="~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_HOPB112100/sunav2/CFGLOC113620",                        
#'                               DirOutBase="~/pfs/out", 
#'                               SchmStats=base::paste0(base::readLines('~/pfs/nitrate_avro_schemas/sunav2_stats.avsc'),collapse=''),
#'                               SchmQMs=base::paste0(base::readLines('~/pfs/nitrate_avro_schemas/sunav2_config.avsc'),collapse=''),
#'                               log=log)
#' Stepping through the code in R studio                               
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=~/pfs/nitrate_null_gap_ucrt_test/2025/06/24/nitrate-surfacewater_HOPB112100/sunav2/CFGLOC113620",
#          "DirOutBase=~/pfs/out",
#          "DirSubCopy=location",
#          "DirErr=~/pfs/out/errored_datums")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2026-04-13)
#' Initial creation.
#' Nora Catolico (2026-04-13)
#' vectorize for loops, add in dirsubcopy

##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)
library(dplyr)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.sunav2.config.qf.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","DirOutBase","DirErr"),
                                      NameParaOptn = c("SchmStats","SchmQMs","DirSubCopy"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOutBase))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for stats: ', Para$SchmStats))
log$debug(base::paste0('Schema for QMs: ', Para$SchmQMs))
log$debug(base::paste0('Directory to copy: ', Para$DirSubCopy))

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

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'stats'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))


# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = c('stats','quality_metrics'),
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.sunav2.config.qf(
        DirIn=idxFileIn,
        DirOutBase=Para$DirOutBase,
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
          DirDatm=idxFileIn,
          DirErrBase=Para$DirErr,
          RmvDatmOut=TRUE,
          DirOutBase=Para$DirOutBase,
          log=log
        )
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
  
  return()
}




