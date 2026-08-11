##############################################################################################
#' @title Workflow for combining logged and streamed EXO2 data

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Combines logged and streamed EXO2 data.
#' 
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path structured as follows: /BASE_REPO/yyyy/mm/dd/assetuid.
#' Within this directory should be sub-folders for logged and streamed data.       
#'        
#' 3. "DirOut=value", The base output path that replaces the BASE_REPO part of DirInBase.
#' Sub-folders for data and log flag will be created in this directory.
#' 
#' 4. "DirErr=value", The output path to for errored datums.
#' 
#' 4. "DirSchm=value" (optional), The base path for the avro schema for the output data 
#' file. Within this directory should be sub-folders for data and log flags. 
#' 
#' 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Combined logged and streamed EXO2 data and log flags in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
setwd("//wsl.localhost/Ubuntu/home/hensley/Git/NEON-IS-data-processing/flow/flow.exo2.logfiles.fill")
arg <- c("DirIn=//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_testing/exo2conductance/2025/09/08/26669",
         "DirOut=//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_log_filled",
         "DirErr=//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_errored_datums",
         "DirSchm=//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas")
log <- NEONprocIS.base::def.log.init(Lvl = "debug")

#' @seealso None currently

# changelog and author contributions / copyrights
#   Bobby Hensley (2026-08-10) original creation
#   Bobby Hensley \email{hensley@battelleecology.org}

##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.exo2.logfiles.fill.R")

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
                                      NameParaOptn = c("DirSchm"),log = log)

# Echo arguments
log$debug(base::paste0('Base input directory: ', Para$DirIn))
log$debug(base::paste0('Base output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Base schema directory: ', Para$DirSchm))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = "streamed",
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.exo2.logfiles.fill(
        DirInBase=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmBase=Para$DirSchm,
        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls()
        
        # Re-routes failed datum
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
    error=function(err) {}
  )
  
  return()
}


