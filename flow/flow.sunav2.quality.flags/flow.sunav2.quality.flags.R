##############################################################################################
#' @title Workflow for SUNA Sensor-specific Quality Flags

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Uses thresholds to apply sensor-specific quality flags to SUNA data.  
#' Measurements where the lamp has not had enough time to stabilze  (nitrateLampStabilizeQF=1) are removed. 
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the input data, QA/QC plausibility flags and quality flag thresholds. 
#' #/pfs/BASE_REPO/date/location/sunav2/cfgloc, where files will then be in /data, /flags and /threshold sub-folders.
#' 
#' 2. "DirInAdditional=value", The file path to the log file flags and calibration flags.                           
#' 
#' 2. "DirOut=value", The base file path for the output data.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#'  
#' 4. "SchmData=value" (optional), The avro schema for the input and output data file.
#' 
#' 5. "SchmFlagsOut=value" (optional), The avro schema for the combined flag file.   
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
#' flow.sunav2.quality.flags <- function(DirIn="~/pfs/nitrate_thresh_select_ts_pad/2025/06/25/nitrate_HOPB112100",                        
#'                               DirOut="~/pfs/sunav2_sensor_specific_flags/sunav2/2024/09/10/CFGLOC110733", 
#'                               FileSchmQf=base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_sensor_specific_flags.avsc'),collapse='')
#'                               log=log)
#' Stepping through the code in R studio                               
# Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/nitrate_analyze_pad_and_qaqc_plau/2025/06/24/nitrate_HOPB112100')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
arg <- c("DirIn=~/pfs/nitrate_analyze_pad_and_qaqc_plau/2025/06/24/nitrate_HOPB112100/sunav2/CFGLOC113620",
          "DirInAdditional=~/pfs/nitrate_group_path/2025/06/24/nitrate_HOPB112100/sunav2/CFGLOC113620/flags",
         "DirOut=~/pfs/out",
         "DirErr=~/pfs/out/errored_datums")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2025-08-30)
#' Initial creation.
#' 
#' Bobby Hensley (2025-09-18)
#' Updated so that measurements prior to lamp stabilization (never intended to be
#' used in downstream pipeline) are removed.
# 
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.sunav2.quality.flags.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirInAdditional","DirOut","DirErr"),
                                      NameParaOptn = c("SchmData","SchmFlagsOut"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Additional input data directory: ', Para$DirInAdditional))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$SchmData))
log$debug(base::paste0('Schema for output flags: ', Para$SchmFlagsOut))

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$SchmData) || Para$SchmData == 'NA'){
  SchmData <- NULL
} else {
  SchmData <- base::paste0(base::readLines(Para$SchmData),collapse='')
}
if(base::is.null(Para$SchmFlagsOut) || Para$SchmFlagsOut == 'NA'){
  SchmFlagsOut <- NULL
} else {
  SchmFlagsOut <- base::paste0(base::readLines(Para$SchmFlagsOut),collapse='')
}

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = 'data',
                              log = log)
DirInAdditional <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirInAdditional,
                              nameDirSub = 'flags',
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.sunav2.quality.flags(
        DirIn=idxFileIn,
        DirOut=Para$DirOut,
        SchmFlagsOut=FileSchmQf,
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




