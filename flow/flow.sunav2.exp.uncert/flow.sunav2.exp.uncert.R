##############################################################################################
#' @title Workflow for SUNA expanded uncertainty calculation

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Calculates the expanded uncertainty for each SUNA burst.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the statistics data and calibration coefficients
#' 
#' 2. "DirOut=value", The base file path for the output data.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 4. "SchmStats=value" (optional), The avro schema for the input and output stats file.
#' 
#' 5. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, that are to be copied with a symbolic link to the output path. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Updated stats files with expanded uncertainty in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' flow.sunav2.exp.uncert <- function(DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733",                        
#'                               DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" ,
#'                               SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse=''), 
#'                               log=log)
#' Stepping through the code in R studio                               
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=~/pfs/testing/nitrate-surfacewater_SUGG103100",
#           "DirOut=~/pfs/nitrate_null_gap_ucrt_updated",
#           "DirErr=~/pfs/nitrate_null_gap_ucrt_updated/errored_datums",
#          "DirSubCopy=group|location|quality_metrics",
#          "SchmStats=~/pfs/nitrate_avro_schemas/nitrate/nitrate_ucrt.avsc")
# rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2025-10-31)
#' Initial creation.
#' Nora Catolico (2025-11-04)
#' add in copied directories

##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.sunav2.exp.uncert.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","DirOut","DirErr"),
                                      NameParaOptn = c("SchmStats","DirSubCopy"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output stats: ', Para$SchmStats))
log$debug(base::paste0('Director to copy: ', Para$DirSubCopy))

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$SchmStats) || Para$SchmStats == 'NA'){
  SchmStats <- NULL
} else {
  SchmStats <- base::paste0(base::readLines(Para$SchmStats),collapse='')
}


# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = c('stats','uncertainty_coef'),
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
      wrap.sunav2.exp.uncert(
        DirIn=idxFileIn,
        DirOutBase=Para$DirOut,
        SchmStats=SchmStats,
        DirSubCopy=DirSubCopy,
        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        log$error(err$message)
        InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxFileIn, 
                                                             log = log)
        DirSub <- strsplit(InfoDirIn$dirRepo,".", fixed = TRUE)[[1]][1]
        NEONprocIS.base::def.dir.crea(DirBgn = Para$DirErr, DirSub = DirSub, 
                                      log = log)
        csvname <- DirSub %>%
          strsplit( "/" ) %>%
          sapply( tail, 1 )
        nameFileErr <- base::paste0(Para$DirErr, DirSub, "/",csvname)
        log$info(base::paste0("Re-routing failed datum path to ", nameFileErr))
        con <- base::file(nameFileErr, "w")
        base::close(con)
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
  
  return()
}




