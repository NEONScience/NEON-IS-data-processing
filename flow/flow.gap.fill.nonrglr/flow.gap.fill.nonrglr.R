##############################################################################################
#' @title Gap filling module for non-regularized data in NEON IS data processing.

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr

#' @description Workflow. Bin data to generate a regular time sequence of observations.
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read regularization frequency from location file (if not in input parameters)
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the gap filled data
#'      
#' This script is run at the command line with the following arguments. Each argument must be a
#' string in the format "Para=value", where "Para" is the intended parameter name and "value" is
#' the value of the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the
#' value of the parameter will be assigned from the system environment variable matching the value
#' string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be a
#' parent of the terminal directory where the data to be gap filled reside. See argument "DirFill"
#' below to indicate the terminal directory.
#'
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs', the same name
#' as the terminal directory indicated in argument "DirFill", or recognizable as the 'yyyy/mm/dd'
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained
#' in the folder.
#'
#' For example:
#' Input path = /scratch/pfs/sunav2_fill_date_gaps/sunav2/2019/01/01
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "DirFill=value", where value is the name of the terminal directory where the data to be
#' gap filled resides. This will be one or more child levels away from "DirIn". All files in the
#' terminal directory will be gap filled. The value may also be a vector of terminal directories,
#' separated by pipes (|). All terminal directories must be present and at the same directory level.
#' For example, "DirFill=data|flags" indicates to regularize the data files within each the data
#' and flags directories.
#' 
#' 5. "WndwFill=value", where value is the window in minutes in which data are expected. It is formatted as a 3 character sequence,
#'  representing the number of minutes over which any number of measurements are expected. 
#' For example, "WndwFill=015" refers to a 15-minute interval, while "WndwAgr=030" refers to a 
#' 30-minute  interval. 
#'
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the regularization folder in the input path that are to be copied with a
#' symbolic link to the output path.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return gap filled data and flag output in Parquet format in DirOut, where DirOut directory
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in R studio
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg<-c(      "DirIn=~/pfs/sunav2_fill_date_gaps/sunav2/2025/06/22/CFGLOC110819",
#              "DirOut=~/pfs/out ",
#              "DirErr=~/pfs/out/errored_datums ",
#              "DirFill=data|flags",
#              "WndwFill=015",
#              "DirSubCopy=location|uncertainty_coef")

#' @seealso \code{\link[eddy4R.base]{def.rglr}}

# changelog and author contributions / copyrights
#   Nora Catolico (12/4/2025)
#     original creation 
##############################################################################################
library(foreach)
library(doParallel)
library(dplyr)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.gap.fill.nonrglr.R")

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
      "DirErr", 
      "DirFill",
      "WndwFill"
    ),
    NameParaOptn = c(
      "DirSubCopy"
    ),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0(
  'Terminal Directories to regularize: ',
  base::paste0(Para$DirFill, collapse = ',')
))

# Retrieve intervals for gap filling
WndwFill <- base::as.numeric(Para$WndwFill)
log$debug(base::paste0('Interval for gap filling, in minutes: ',base::paste0(WndwFill,collapse=',')))


# Retrieve output schema(s)
log$debug(base::paste0(
  'Output schema(s) for gap filled data: ',
  base::paste0(Para$FileSchmFill, collapse = ',')
))

# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(Para$DirSubCopy, Para$DirFill))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

nameDirSub <- base::as.list(c(Para$DirFill))
log$debug(base::paste0(
  'Expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing datum path: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.gap.fill.nonrglr(DirIn=idxDirIn,
                DirOutBase=Para$DirOut,
                WndwFill=WndwFill,
                DirFill=Para$DirFill,
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
