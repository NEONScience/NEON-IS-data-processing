##############################################################################################
#' @title Workflow for Continuous Discharge Processing

#' @author
#' Zachary Nickerson \email{ncatolico@battelleecology.org}

#' @description Workflow. Validates, cleans, and formats troll log files into daily parquets.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id.The source-id folder may have multiple csv log files. 
#' The source-id is the unique identifier of the sensor.
#' 
#'           
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#'  
#' 4. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Cleaned troll log files in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
setwd("/home/nickerson/Git/NEON-IS-data-processing/flow/flow.discharge.predict")
# Sys.setenv(DIR_IN='~/pfs/l4discharge_group_and_parse/2025/09/29/l4discharge_HOPB132100')
Sys.setenv(DIR_IN='~/pfs/l4discharge_group_and_parse/2025')
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
arg <- c("DirIn=$DIR_IN",
         "DirBaM=/home/nickerson/Git/NEON-IS-data-processing/flow/flow.discharge.predict/BaM_beta",
         "DirOut=/home/nickerson/pfs/out",
         "DirErr=/home/nickerson/pfs/out/errored_datums",
         "FileSchmData=/home/nickerson/pfs/l4discharge_avro_schemas/l4discharge/l4discharge_dp04.avsc")
# rm(list=setdiff(ls(),c('arg','log')))
#setwd("/home/NEON/nickerson/R/NEON-IS-data-processing/flow/flow.discharge.predict")

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-10-15) 
#     original creation
#   Nora Catolico (2025-12-17) 
#     added error logging, updates to better interact with pachyderm
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)
library(dplyr)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.discharge.predict.R")
source("./def.dir.in.partial.R")
# source("./BaM_beta")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","DirBaM","DirOut","DirErr"),
                                      NameParaOptn = c("FileSchmData"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Model executable directory: ', Para$DirBaM))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))


# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$FileSchmData) || Para$FileSchmData == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(Para$FileSchmData),collapse='')
}


# Find all the input paths (datums). We will process each one.
DirIn <-
  def.dir.in.partial(DirBgn = Para$DirIn,
                     nameDirSubPartial = 'l4discharge',
                     log = log)
log$debug(base::paste0('Directories identified:', DirIn))

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  # idxDirIn=DirIn[1]
  log$info(base::paste0('Processing path to file: ', idxDirIn))
  
  # Copy BaM model to a temporary directory within this iteration of DirIn
  fs::dir_copy(path = Para$DirBaM, new_path = base::paste(idxDirIn,"BaM_beta",sep="/"), overwrite = TRUE)
  Para$DirBaM <- base::paste(idxDirIn,"BaM_beta",sep="/")
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.discharge.predict(
        DirIn=idxDirIn,
        DirBaM=Para$DirBaM,
        DirOutBase=Para$DirOut,
        SchmDataOut=SchmDataOut,
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
  
  # Clean up the temporary BaM model directory after processing
  base::unlink(
    base::paste(idxDirIn,"BaM_beta",sep="/"),
    recursive = TRUE,
    force = TRUE
  )
  Para$DirBaM <- "/home/nickerson/Git/NEON-IS-data-processing/flow/flow.discharge.predict/BaM_beta"
}




