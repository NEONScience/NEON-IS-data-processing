##############################################################################################
#' @title Workflow for Troll Log File Comparison and Gap Filling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Compares logged data to streamed data and fills gaps.
#' 
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id.The source-id folder may have multiple csv log files. 
#' The source-id is the unique identifier of the sensor.#'         
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
#' 5. "FileSchmFlags=value" (optional), where values is the full path to the avro schema for the output flags 
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
#' Sys.setenv(DirIn='/home/NEON/ncatolico/pfs/logjam_clean_troll_files/leveltroll500/2022/03/10/21115') #cleaned log data
#' Sys.setenv(DirIn='/home/NEON/ncatolico/pfs/leveltroll500_data_source_trino/leveltroll500/2022/03/10/21115') #streamed L0 data
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DirIn","DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2024-01-30) original creation
# 
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.troll.logfiles.fill.R")

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
                                      NameParaOptn = c("FileSchmData","FileSchmFlags"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmFlags))


# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$FileSchmData) || Para$FileSchmData == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(Para$FileSchmData),collapse='')
}
if(base::is.null(Para$FileSchmFlags) || Para$FileSchmFlags == 'NA'){
  SchmFlagsOut <- NULL
} else {
  SchmFlagsOut <- base::paste0(base::readLines(Para$FileSchmFlags),collapse='')
}

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = 'data',
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.troll.logfiles(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmDataOut=SchmDataOut,
        SchmFlagsOut=SchmFlagsOut,
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




