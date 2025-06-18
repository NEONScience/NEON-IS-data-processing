##############################################################################################
#' @title  Process data from metone370380 tipping bucket sensors to 1 minute and 30 minute 
#' aggregations

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Add 0s to periods where no precipitation recorded, process Throughfall area conversion
#' and uncertainty (as applicable, location context based), process heater QMs, aggregate to 1 minute and apply 
#' extremePrecipFlag and finalQF, aggregate to 30 minutes, including summed precipitation, extreme precip flags and 
#' uncertainty calculations 
#' 
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in the  data files into arrow datasets
#'      read in applicable thresholds
#'      apply Throughfall conversion (if applicable)
#'      Compute flags based on heater QMs
#'      aggregate data to 1 and 30 minutes
#'      create extreme precip flags and final quality flag
#'      Write stats output to file
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. 
#' The input repo should be structured by source ID as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within the path for each source ID is (at a minimum) the folder:
#'         /data
#'         /flags
#'         /threshold
#'         /location
#'         /uncertainty_coef
#' The data/flags folders holds any number of daily data/flags files padded around the yyyy/mm/dd in the input path.
#' #' 
#' For example:
#' Input path = precipBucket_group_path/2025/05/31/precip-tipping_BLAN000040/metone370380/CFGLOC100292/
#'
#' There may be other folders at the same level as the data directory. They are ignored and not passed 
#' to the output unless indicated in SubDirCopy.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of \code{DirIn}.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 4. "SchmData=value" (optional), where value is the full path to schema for the QF flags after inputing custom flags
#' startDateTime
#' EndDateTime
#' precipBulk
#' precipBulkExpUncert 
#' heater0QM
#' heater1QM
#' heater2QM
#' heater3QM
#' validCalQF
#' suspectCalcQF
#' extremePrecipQF
#' finalQF
#' Ensure that any schema input here matches the column order of the auto-generated schema, 
#' simply making any desired changes to column names.
#'
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the data folder that are to be copied with a
#' symbolic link to the output path. May NOT include 'data'. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository with the computed precipitation and flags in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. The terminal directories of each 
#' sensor location folder are "stats" and "location"
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not Run - uses all available defaults
#' Rscript flow.precip.bucket.R  
# 'DirIn=/scratch/pfs/precipBucket_group_path/2025/05/31/precip-tipping_BLAN000040/metone370380/CFGLOC100292/''DirOut=/scratch/pfs/out_tb' 'DirErr=/scratch/pfs/out_tb/errored_datums''DirSubCopy=data' 

#' Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='DirIn=/scratch/pfs/precipBucket_group_path/2025/05/31/precip-tipping_BLAN000040/metone370380/CFGLOC100292/')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "DirErr=/scratch/pfs/out/errored_datums")
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-06-15)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)
library(magrittr)

# Source the wrapper function and other dependency functions. Assume it is in the working directory
source("./wrap.precip.bucket.R")

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
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c(
                     "DirIn", 
                     "DirOut", 
                     "DirErr" 
                     
                     ),
    NameParaOptn = c(
                     "DirSubCopy",
                     "FileSchmData"
                     ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve output schema for  stats
FileSchmData <- Para$FileSchmData

#one or more will always be null 
# Read in the schema 
if(base::is.null(FileSchmData) || FileSchmData == 'NA'){
  FileSchmData <- NULL
} else {
  FileSchmData <- base::paste0(base::readLines(FileSchmData),collapse='')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- c('data','flags', 'threshold', 'location', 'uncertainty_coef')
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub =  nameDirSub,
                              log = log)


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.precip.bucket(DirIn=DirIn,
                              DirOutBase=Para$DirOut,
                              SchmData = FileSchmData,
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

} # End loop around datum paths

