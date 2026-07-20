##############################################################################################
#' @title Workflow for buoy wind-specific direction statistics and uncertainty calculations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Uses thresholds to apply wind direction statistics and uncertainty calculations to buoy wind data.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the input data, QA/QC plausibility flags and quality flag thresholds. 
#' #/pfs/BASE_REPO/date/location/cfgloc, where files will then be in /data, /flags and /threshold sub-folders.
#' 
#' 2. "DirOut=value", The base file path for the output data.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#'  
#' 4. "WndwAgr=value", The window aggregation period for the buoy wind data (e.g., "002" for 2-minute averages, "030" for 30-minute averages).
#' 
#' 5. "FileSchmStats=value" (optional), The avro schema for the input and output data file.
#' 
#' 6. "FileSchmQf=value" (optional), The avro schema for the combined flag file.   
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
#' flow.wind.buoy.direction.stats.ucrt <- function(DirIn="~/pfs/windBuoy_analyze_pad_and_qaqc_plau/2025/12/17/wind-buoy_BARC103100",                        
#'                               DirOut="~/pfs/wind_buoy_direction_stats_ucrt",
#'                               log=log)
#' Stepping through the code in R studio                               
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
arg <- c("DirIn=/home/ncatolico/Git/pfs/windBuoy_analyze_pad_and_qaqc_plau/2025/12/17/wind-buoy_BARC103100",
         "DirOut=/home/ncatolico/Git/pfs/wind_buoy_direction_stats_ucrt",
         "DirErr=/home/ncatolico/Git/pfs/out/errored_datums", "WndwAgr=002|030")
# rm(list=setdiff(ls(),c('arg','log')))
#' 
#' @seealso None currently

# changelog and author contributions / copyrights
#' Nora Catolico (2026-07-13)
#' Initial creation

##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)
library(dplyr)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.wind.buoy.direction.stats.ucrt.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","DirOut","DirErr","WndwAgr"),
                                      NameParaOptn = c("FileSchmStats","FileSchmQf"),
                                      log = log)


# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmStats))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))
log$debug(base::paste0('Window aggregation: ', Para$WndwAgr))

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$FileSchmStats) || Para$FileSchmStats == 'NA'){
  SchmStatsOut <- NULL
} else {
  SchmStatsOut <- base::paste0(base::readLines(Para$FileSchmStats),collapse='')
}
if(base::is.null(Para$FileSchmQf) || Para$FileSchmQf == 'NA'){
  SchmFlagsOut <- NULL
} else {
  SchmFlagsOut <- base::paste0(base::readLines(Para$FileSchmQf),collapse='')
}

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = c('data'),
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.wind.buoy.direction.stats.ucrt(
        DirIn=idxFileIn,
        DirOutBase=Para$DirOut,
        WndwAgr=Para$WndwAgr,
        SchmStatsOut=SchmStatsOut,
        SchmFlagsOut=SchmFlagsOut,
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




