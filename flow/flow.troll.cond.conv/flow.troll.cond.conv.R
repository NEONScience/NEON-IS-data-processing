##############################################################################################
#' @title Workflow for Missing Temp Flag and Conductivity Conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#' 
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor.
#'
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' For example:
#' Input path = pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681 with nested folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
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
#' THE INPUT DATA.
#' 
#' 5. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Corrected conductivity data and associated flags for missing temperature data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' If no output schema is provided for the flags, the output column/variable names will be 
#' readout_time, qfFlow, qfHeat, in that order. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS 
#' MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/aquatroll200_calibration_group_and_convert')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2020-08-01)
#     original creation
#   Cove Sturtevant (2020-09-22)
#     placed output flags in existing flags directory
#     symbolically linked any files already in the flags directory to the output
#   Nora Catolico (2023-09-19)
#     Applied internal parallelization
#     Moved main functionality to wrapper function
#     Added datum error routing
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)


# Source the wrapper function. Assume it is in the working directory
source("./wrap.troll.cond.conv.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","DirErr"),NameParaOptn = c("FileSchmData","FileSchmQf"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))


# Read in the schemas so we only have to do it once and not every
# time in the avro writer.
if(base::is.null(Para$FileSchmData) || Para$FileSchmData == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(Para$FileSchmData),collapse='')
}
if(base::is.null(Para$FileSchmQf) || Para$FileSchmQf == 'NA'){
  SchmQfOut <- NULL
} else {
  SchmQfOut <- base::paste0(base::readLines(Para$FileSchmQf),collapse='')
}


# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(
    Para$DirSubCopy,
    c('data', 'uncertainty_coef', 'uncertainty_data', 'flags')
  ))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))


#what are the expected subdirectories of each input path
nameDirSub <- c('data')
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.troll.cond.conv(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmDataOut=SchmDataOut,
        SchmQf=SchmQfOut,
        DirSubCopy=DirSubCopy,
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
