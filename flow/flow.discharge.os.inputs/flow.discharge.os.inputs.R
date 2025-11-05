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
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}. 
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
# Sys.setenv(DIR_IN='~/pfs/testing',
#            DIR_START="2024-02-25",
#            DIR_END="2024-04-01")
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=$DIR_IN",
#          "DirStart=$DIR_START",
#          "DirEnd=$DIR_END",
#          "DirInTables=~/pfs",
#          "DirOut=~/pfs/out",
#          "DirErr=~/pfs/out/errored_datums")
# rm(list=setdiff(ls(),c('arg','log')))
# setwd("/home/NEON/nickerson/R/NEON-IS-data-processing/flow/flow.discharge.os.inputs")

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-10-15) 
#     original creation
##############################################################################################
options(digits.secs = 3)
library(lubridate)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.discharge.os.inputs.R")
#source("./BaM_beta")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,
                                      NameParaReqd = c("DirIn",
                                                       "DirStart","DirEnd",
                                                       "DirInTables","DirOut",
                                                       "DirErr"),
                                      log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Start directory: ', Para$DirIn))
log$debug(base::paste0('End directory: ', Para$DirIn))
log$debug(base::paste0('Tables directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Read in all OS table loader outputs
dirList <- list.files(Para$DirInTables,pattern = "table_loader")
tableNameMap <- list()
for(d in 1:length(dirList)){
  fileName <- list.files(paste(Para$DirInTables,dirList[d],sep = "/"))
  filePath <- list.files(paste(Para$DirInTables,dirList[d],sep = "/"),
                         full.names = T)
  currFile <- read.csv(filePath,encoding = "UTF-8",header = T)
  tableNameMap[[gsub("\\.csv$","",
                     gsub("^.*\\.001\\.","",
                          fileName))]] <- currFile
}

# Create a sequence of days
seqDate <- seq.Date(as.Date(Para$DirStart),as.Date(Para$DirEnd),"day")
seqDate <- format(seqDate,"%Y/%m/%d")

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                                     nameDirSub = NULL,
                                     log = log)
# Subset to those directories that fit the date range
DirIn <- DirIn[sapply(DirIn, 
                      function(x) any(grepl(paste(seqDate, 
                                                  collapse = "|"), x)))]
log$debug(base::paste0('Directories identified:', DirIn))

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  idxDirIn=DirIn[1]
  log$info(base::paste0('Processing path to file: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.discharge.os.inputs(
        ListTables = tableNameMap,
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        log$error(err$message)
        InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn, 
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
}

# End flow.discharge.os.inputs