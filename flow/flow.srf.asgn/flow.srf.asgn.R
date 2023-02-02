##############################################################################################
#' @title Science Review Flag assignment module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Workflow. Assign the science review flag (SRF) file(s) for a group ID to each data day which it applies
#' over 1 or more data years. When assigning the SRF file to each data day, the  
#' information is filtered to that relevant only to the data day and resaved. This includes filtering
#' out all things that might change without impacting the flagging, including create/update dates and user comment.
#' In addition, the start and end dates are truncated to the start or end of the data day, as applicable. 
#' Original dates falling within the data day will not be modified. 
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine the years over which to assign SRF files
#'    For each group:
#'      Read the srf file(s) 
#'      Create a folder structure of all relevant data days for the group.
#'      Filter the original SRF file for info relevant to the data day and re-save it to the output
#'      
#' This script is run at the command line with the following arguments. Each argument must be a string in the format
#' "Para=value", where "Para" is the intended parameter name and "value" is the value of the parameter.
#' Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter will be assigned
#' from the system environment variable matching the value string. The arguments are:
#'
#' 1. "DirIn=value", where value is the starting directory path where to search for SRF files. 
#' The full repository must be structured as follows: #/pfs/BASE_REPO/GROUP_ID, 
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' The GROUP_ID folder holds any number of SRF files pertaining to the GROUP_ID. Typically there will only be one file. 
#' There may be no further subdirectories of GROUP_ID.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/surfacewater-physical_PRLA130100/:
#'    surfacewater-physical_PRLA130100_science_review_flags.json
#' 
#' Note that DirIn can be any point between #/pfs and #/pfs/BASE_REPO/GROUP_ID. 
#' For the folder structure in the example above, DirIn=/scratch/pfs will process all GROUP_IDs found 
#' within the recursive path structure. In contrast, DirIn=/scratch/pfs/proc_group/surfacewater-physical_PRLA130100
#' will process only GROUP_ID surfacewater-physical_PRLA130100.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileYear=value", where value is the path to a single file that contains only a list of numeric years. The 
#' minumum and maximum of the years found in the file will determine the maximum date range to populate the 
#' output repository with files. No header. Each and every row should be a numeric year. Example file:
#' 2019
#' 2020
#' 2021
#' 
#' Note: This script implements optional parallelization as well as logging (described in 
#' \code{\link[NEONprocIS.base]{def.log.init}}), both of which use system environment variables if available. 

#' @return A directory structure in the format DirOut/YEAR/MONTH/DAY/GROUP_ID/science_review_flags, where 
#' DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the terminal path 
#' includes the filtered location files applicable to the year, month, day, and GROUP_ID indicated in 
#' the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.srf.asgn.R "DirIn=/pfs/proc_group/surfacewater-physical_PRLA130100" "DirOut=/pfs/out" "DirErr=/pfs/out/errored_datums" "FileYear=/pfs/intended_data_years/data_years.txt"

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-01-26)
#     original creation, refactored from flow.loc.grp.asgn
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.srf.asgn.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Options
base::options(digits.secs = 3)

# Use environment variable to specify how many cores to run on - MAKE INTO A FUNCTION
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
    NameParaReqd = c("DirIn", "DirOut","DirErr","FileYear"),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Parse the file containing the years to populate
log$debug(base::paste0('File containing data years to populate: ', Para$FileYear))
yearFill <- base::as.integer(base::readLines(con=Para$FileYear))
if(base::length(yearFill) == 0 || base::any(base::is.na(yearFill))){
  log$fatal(base::paste0('Cannot determine years to populate from file: ', Para$FileYear,'. Check file contents.'))
  stop()
}
timeBgn <- base::as.POSIXct(x=paste0(min(yearFill),'-01-01'),tz='GMT')
timeEnd <- base::as.POSIXct(x=paste0(max(yearFill)+1,'-01-01'),tz='GMT')

# Find all the input paths (terminal directories). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = NULL,
                              log = log)

# Process each file path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.srf.asgn(DirIn=idxDirIn,
                    DirOutBase=Para$DirOut,
                    TimeBgn=timeBgn,
                    TimeEnd=timeEnd,
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

