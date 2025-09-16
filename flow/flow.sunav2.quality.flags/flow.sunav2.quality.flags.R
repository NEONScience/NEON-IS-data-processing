##############################################################################################
#' @title Workflow for SUNA Sensor-specific Quality Flags

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Calculates quality flags for SUNA internal humidity, lamp temperature 
#' and light to dark spectral ratio, and saves into daily parquets.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data, structured as follows: 
#' #/pfs/BASE_REPO/date/source-id/data.              
#' 
#' 2. "DirOut=value", where the value is the output path.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#'  
#' 4. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output data 
#' file. 
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
# arg <- c("DirIn=$DIR_IN",
#          "DirOut=~/pfs/out",
#          "DirErr=~/pfs/out/errored_datums")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2025-08-26) Original creation 
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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","DirErr"),
                                      NameParaOptn = c("FileSchmQf"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmQf))


# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$FileSchmQf) || Para$FileSchmQf == 'NA'){
  FileSchmQf <- NULL
} else {
  FileSchmQf <- base::paste0(base::readLines(Para$FileSchmQf),collapse='')
}

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = 'data',
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




