##############################################################################################
#' @title Compute average precipitation for the same day computed from different central days 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Compute average precipitation computed for the  
#' Belfort AEPG600m sensor that was generated from different central days of the smoothing algorithm
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in the computed precipitation data files 
#'      Compute average precip
#'      Write data and flags output to file
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
#' Nested within the path for each source ID is (at a minimum) the folder:
#'         /stats
#'         /flags
#' The stats/flags folders holds any number of daily stats/flags files padded around the yyyy/mm/dd in the input path.
#'
#' There may be other folders at the same level as the stats directory. They are ignored and not passed 
#' to the output unless indicated in SubDirCopy.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of \code{DirIn}.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 4. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the data folder that are to be copied with a
#' symbolic link to the output path. May NOT include 'data'. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository with the average precipitation for each sensor location in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. Only the stats directory is populated as 
#' the terminal directory. (Flags do not need averaging across computation days.) 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not Run - uses all available defaults
#' Rscript flow.precip.aepg.comb.R "DirIn=/scratch/pfs/precipWeighing_ts_pad_smoother/2022/07/28/precip-weighing_BLUE900000" "DirOut=/scratch/pfs/out" "DirErr=/scratch/pfs/out/errored_datums"  
#'
#' Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='/scratch/pfs/precipWeighing_ts_pad_smoother/2022/07/28')
#' log <- NEONprocIS.base::def.log.init(Lvl = "DEBUG")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "DirErr=/scratch/pfs/out/errored_datums")
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant & Teresa Burlingame (2024-06-13)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.precip.aepg.comb.R")

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
                     "DirSubCopy"
                     ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- c('stats')
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
      wrap.precip.aepg.comb(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
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
