
##############################################################################################
#' @title Workflow for correcting fdom for temperature and absorbance

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Apply temperature and absorbance corrections to fdom data for the
#' water quality transition.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /named_location_group/exo2fdom/SENSOR/data
#'         /named_location_group/exo2fdom/SENSOR/calibration
#'         /named_location_group/exoconductivity/SENSOR/data
#'         /named_location_group/exoconductivity/SENSOR/uncertainty_data
#'         /named_location_group/SUNA/SENSOR/data
#'         /named_location_group/SUNA/SENSOR/calibration
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.

#' 4. "DirSubCopy=value" (optional), where value is a comma-separated list of subdirectories to copy from the input to the output directory.
#' 
#' 5. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 6. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Corrected fdom data and associated flags for temperature and absorbance corrections.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/scratch/pfs/waterQuality_exo2fdom_correct_group/2025/09/04/water-quality_POSE102100')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/pfs/out/waterQuality_exo2fdom_correct", "FileSchmData=/pfs/avro_schemas/dp0p/exo2fdom_corrected.avsc", "FileSchmQf=/pfs/avro_schemas/dp0p/flags_correction_exo2fdom.avsc")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     added support for parquet format
#   Nora Catolico (2026-11-09)
#     updated with wrapper function
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.wq.fdom.corr.R")
source("./def.pars.data.suna.R")
source("./def.pars.cal.tab.suna.R")
source("./def.wq.abs.corr.R")

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

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
                          NameParaOptn = c("FileSchmData","FileSchmQf","DirSubCopy"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))


# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$FileSchmData) || Para$FileSchmData == 'NA'){
  SchmData <- NULL
} else {
  SchmData <- base::paste0(base::readLines(Para$FileSchmData),collapse='')
}

if(base::is.null(Para$FileSchmQf) || Para$FileSchmQf == 'NA'){
  SchmQf <- NULL
} else {
  SchmQf <- base::paste0(base::readLines(Para$FileSchmQf),collapse='')
}

# A datum must have fDOM data and optionally exoconductivity and sunav2
nameDirSub <- c('exo2fdom')

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,nameDirSub = nameDirSub,log = log)

if(base::length(DirIn) < 1){
  log$fatal('No datums found to process.')
  stop()
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'exo2fdom'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))


# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxDirIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.wq.fdom.corr(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmData=SchmData,
        SchmQf=SchmQf,
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


