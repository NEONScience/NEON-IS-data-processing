##############################################################################################
#' @title Workflow for EXO2 Multisonde Log File Formatting and Parsing

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Parses combined sonde log files into individual sensors.
#' 
#' The arguments are: 
#' 
#' "DirIn=value", input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id. The input source-id is the unique identifier of the sonde body.           
#'        
#' "DirOutBase=value", base directory where files will be written into individual sub-folders
#' for each individual sensor.
#' 
#' "DirErr=value", where the value is the output path to place the path structure of errored datums.
#' 
#' "SchmExo2" (optional), the avro schema for the sonde body data streams
#' 
#' "SchmCond" (optional), the avro schema for the conductivity sensor data streams
#' 
#' "SchmDO" (optional), the avro schema fro the dissolved oxygen sensor data streams
#' 
#' "SchmPh" (optional), the avro schema for the pH sensor data streams
#' 
#' "SchmTurb" (optional), the avro schema for the turbidity sensor data streams
#' 
#' "SchmFdom" (optional), the avro schema for the fDOM sensor data streams
#' 
#' "SchmChl" (optional), the avro schema for the total algae sensor data streams 
#' 
#' @return Parquets of logged multisonde data parsed into individual sensors streams.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
 # arg <- c("DirIn=~/pfs/exo2_logjam_load_files/25832",
 #           "DirOutBase=~/pfs/out/exo2_test",
 #           "DirErr=~/pfs/out/errored_datums")
 # log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Bobby Hensley (2025-04-16) 
#     Original creation


##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)
library(lubridate)
library(readr)
library(stringi)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.exo2.logfiles.R")

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
                                      NameParaOptn = c("SchmExo2","SchmCond","SchmDO","SchmPh","SchmTurb","SchmFdom","SchmChl"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOutBase))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for exo2 body: ', Para$SchmExo2))
log$debug(base::paste0('Schema for conductance: ', Para$SchmCond))
log$debug(base::paste0('Schema for dissolved oxygen: ', Para$SchmDO))
log$debug(base::paste0('Schema for pH: ', Para$SchmPh))
log$debug(base::paste0('Schema for turbidity: ', Para$SchmTurb))
log$debug(base::paste0('Schema for fDOM: ', Para$SchmFdom))
log$debug(base::paste0('Schema for total algae: ', Para$SchmChl))

# Read in the schemas.
if(base::is.null(Para$SchmExo2) || Para$SchmExo2 == 'NA'){SchmExo2 <- NULL} else {SchmExo2 <- base::paste0(base::readLines(Para$SchmExo2),collapse='')}
if(base::is.null(Para$SchmCond) || Para$SchmCond == 'NA'){SchmCond <- NULL} else {SchmCond <- base::paste0(base::readLines(Para$SchmCond),collapse='')}
if(base::is.null(Para$SchmDO) || Para$SchmDO == 'NA'){SchmDO <- NULL} else {SchmDO <- base::paste0(base::readLines(Para$SchmDO),collapse='')}
if(base::is.null(Para$SchmPh) || Para$SchmPh == 'NA'){SchmPh <- NULL} else {SchmPh <- base::paste0(base::readLines(Para$SchmPh),collapse='')}
if(base::is.null(Para$SchmTurb) || Para$SchmTurb == 'NA'){SchmTurb <- NULL} else {SchmTurb <- base::paste0(base::readLines(Para$SchmTurb),collapse='')}
if(base::is.null(Para$SchmFdom) || Para$SchmFdom == 'NA'){SchmFdom <- NULL} else {SchmFdom <- base::paste0(base::readLines(Para$SchmFdom),collapse='')}
if(base::is.null(Para$SchmChl) || Para$SchmChl == 'NA'){SchmChl <- NULL} else {SchmChl <- base::paste0(base::readLines(Para$SchmChl),collapse='')}


# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = NULL,
                              log = log)


# Take stock of our data files. 
fileData <- base::list.files(DirIn,full.names=TRUE)
log$debug(base::paste0('Files identified:', fileData))


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = fileData) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.exo2.logfiles(
        FileIn=idxFileIn,
        DirOutBase=Para$DirOutBase,
        SchmExo2=Para$SchmExo2,
        SchmCond=Para$SchmCond,
        SchmDO=Para$SchmDO,
        SchmPh=Para$SchmPh,
        SchmTurb=Para$SchmTurb,
        SchmFdom=Para$SchmFdom,
        SchmChl=Para$SchmChl,
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


