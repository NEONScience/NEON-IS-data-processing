##############################################################################################
#' @title Workflow for barometric pressure flag and barometric pressure conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Calculates converted pressure and flags/removes pressure data if barometric pressure final QF is 1. 
#' 
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data from a single group ID, structured as follows: 
#' #/pfs/BASE_REPO/yyyy/mm/dd/group/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day.
#'
#' 
#' Nested within this path are the folders:
#'         /leveltroll400
#'         /leveltroll400/data
#'         /leveltroll400/flags
#'         /leveltroll400/location
#'         /pressure-air-buoy_*
#'         /pressure-air-buoy_*/data
#'         /pressure-air-buoy_*/group
#'         /pressure-air-buoy_*/location
#'         
#' For example:
#' Input path = pfs/subsurfMoorTempCond_group_path/2022/06/15/subsurf-moor-temp-cond_PRPO103100 with nested folders:
#'         /leveltroll400
#'         /leveltroll400/data
#'         /leveltroll400/flags
#'         /leveltroll400/location
#'         /pressure-air-buoy_PRPO103100
#'         /pressure-air-buoy_PRPO103100/data
#'         /pressure-air-buoy_PRPO103100/group
#'         /pressure-air-buoy_PRPO103100/location
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
#' 6. "FileSchmUcrt=value" (optional), where values is the full path to the avro schema for the output uncertainty file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable).
#' 
#' 7. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the data folder in the input path that are to be copied with a 
#' symbolic link to the output path. Note that it is acceptable to include the
#' "stats" directory if stats files generated from other processing modules (differently named) are to be 
#' passed through. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Corrected pressure data and associated flags for missing bouy barometric pressure data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data', 'flags', and 'uncertainty' are automatically populated in the output directory, where the files 
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
# Sys.setenv(DIR_IN='~/pfs/subsurfMoorTempCond_group_path/2022/06/16/subsurf-moor-temp-cond_CRAM103502')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums") #,"FileSchmData=$FILE_SCHEMA_DATA","FileSchmQf=$FILE_SCHEMA_QF")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2025-09-18)
#     original creation
#   Nora Catolico (2025-12-31)
#     update for when baro perssure is NA

##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)


# Source the wrapper function. Assume it is in the working directory
source("./wrap.troll.baro.conv.R")
source("./def.dir.in.partial.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","DirErr"),NameParaOptn = c("FileSchmData","FileSchmQf","FileSchmUcrt","DirSubCopy"),log = log)

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
if(base::is.null(Para$FileSchmQf) || Para$FileSchmUcrt == 'NA'){
  SchmUcrtOut <- NULL
} else {
  SchmUcrtOut <- base::paste0(base::readLines(Para$FileSchmUcrt),collapse='')
}

# Retrieve optional subdirectories to copy over
# DirSubCopy <-
#   base::unique(base::setdiff(
#     Para$DirSubCopy,
#     c('hobou24','group')
#   ))
# log$debug(base::paste0(
#   'Additional subdirectories to copy: ',
#   base::paste0(DirSubCopy, collapse = ',')
# ))
DirSubCopy<-c('hobou24','group')

#what are the expected subdirectories of each input path
nameDirSub <- base::c('leveltroll400')
log$debug(base::paste0(
  'expected subdirectories: ',
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
        SchmQfOut=SchmQfOut,
        SchmUcrtOut=SchmUcrtOut,
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
