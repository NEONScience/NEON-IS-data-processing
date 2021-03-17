##############################################################################################
#' @title Calibration assignment module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Workflow. Assign the calibration file(s) for a sensor ID to each data day which it applies
#' over 1 or more data years.
#' Valid date ranges and certificate numbers in calibration files are used to determine the most applicable
#' calibration for each data day. The most applicable cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#' Note that calibrations with a valid date range beginning after the data day of interest are treated
#' as if they don't exist, which expired calibrations are considered applicable after the valid date
#' range if no other valid calibration exists.
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine the years over which to assign calibration files
#'    For each sensor ID:
#'      Read in all calibration files for the sensor ID 
#'      Determine the most applicable calibration for each data day within the data years
#'      Create a folder structure of all relevant data days for the sensor ID.
#'      Copy the most applicable calibration file(s) into each the folder for each data day
#'      
#' This script is run at the command line with the following arguments. Each argument must be a string in the format
#' "Para=value", where "Para" is the intended parameter name and "value" is the value of the parameter.
#' Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter will be assigned
#' from the system environment variable matching the value string. The arguments are:
#'
#' 1. "DirIn=value", where value is the starting directory path where to search for calibrations. 
#' The full repository must be structured as follows: #/pfs/BASE_REPO/SOURCE_TYPE/SOURCE_ID/TERM, 
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' The TERM folder holds any number of calibration files pertaining to the SOURCE_ID and TERM combination.  
#' There may be any number of TERM folders at the same level. There may be no further subdirectories of TERM.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/prt/27134/ with nested folders:
#'    /resistance
#'    /voltage
#' 
#' Note that DirIn can be any point between #/pfs and #/pfs/BASE_REPO/SOURCE_TYPE/SOURCE_ID/TERM. 
#' In the example above, DirIn=/scratch/pfs will process all SOURCE_ID x TERM combinations found 
#' within the recursive path structure. In contrast, DirIn=/scratch/pfs/proc_group_prt/27134/resistance
#' will process only the TERM resistance for SOURCE_ID 27134.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 3. "FileYear=value", where value is the path to a single file that contains only a list of numeric years. The 
#' minumum and maximum of the years found in the file will determine the maximum date range to populate the 
#' output repository with calibration files. No header. Each and every row should be a numeric year. Example file:
#' 2019
#' 2020
#' 2021
#' 
#' 4. "PadDay=value" (optional), where value contains the integer days to include applicable 
#' calibration files before/after a given data day. A negative value will copy in the calibration file(s) 
#' that are applicable to the given data day AND # number of days before the data day. A positive value 
#' will copy in the calibration file(s) applicable to the given data day AND # number of days after the data day. 
#' Default is 0. For example, if the current data day is 2019-01-15, "PadDay=-2" will copy in any calibration file(s)
#' that are applicable between 2019-01-13 00:00 and 2019-01-15 24:00. "PadDay=2" will copy in calibration file(s) 
#' that are applicable between 2019-01-15 00:00 and 2019-01-17 24:00. To provide both negative and positive pads 
#' (a window around a given day), separate the values with pipes (e.g. "PadDay=-2|2"). 
#'
#' Note: This script implements optional parallelization as well as logging (described in 
#' \code{\link[NEONprocIS.base]{def.log.init}}), both of which use system environment variables if available. 

#' @return A directory structure in the format DirOut/SOURCE_TYPE/YEAR/MONTH/DAY/SOURCE_ID/calibration/TERM, where 
#' DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the terminal path 
#' includes the calibration files applicable to the year, month, day, source_id, and term indicated in 
#' the path and following the selection hierarchy above. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.cal.asgn.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" 

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-03-03)
#     original creation, refactored from flow.cal.filt
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.cal.asgn.R")

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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut","FileYear"),
    NameParaOptn = c("PadDay"),
    ValuParaOptn = base::list(PadDay=0),
    TypePara = base::list(PadDay="integer"),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))

# Parse the file containing the years to populate
log$debug(base::paste0('File containing data years to populate: ', Para$FileYear))
yearFill <- base::as.integer(base::readLines(con=Para$FileYear))
if(base::length(yearFill) == 0 || base::any(base::is.na(yearFill))){
  log$fatal(base::paste0('Cannot determine years to populate from file: ', Para$FileYear,'. Check file contents.'))
  stop()
}
timeBgn <- base::as.POSIXct(x=paste0(min(yearFill),'-01-01'),tz='GMT')
timeEnd <- base::as.POSIXct(x=paste0(max(yearFill)+1,'-01-01'),tz='GMT')

# Parse the days to pad
if(base::length(Para$PadDay) == 1 && !base::is.na(Para$PadDay)){
  # Assign the pads
  if(Para$PadDay > 0){
    timePadEnd <- base::as.difftime(Para$PadDay,units='days')
    timePadBgn <- base::as.difftime(0,units='days')
  } else {
    timePadBgn <- base::as.difftime(Para$PadDay,units='days')
    timePadEnd <- base::as.difftime(0,units='days')
  }
} else if(base::length(Para$PadDay) == 2 && !base::any(base::is.na(Para$PadDay))){
  
  # Make sure one is negative and one is positive
  minPadDay <- base::min(Para$PadDay)
  maxPadDay <- base::max(Para$PadDay)
  if(minPadDay > 0 || maxPadDay < 0){
    log$fatal('If two numbers are provided for input argument PadDay, one must be negative, one must be positive. See documentation.')
    stop()
  }
  
  # Assign the pads
  timePadBgn <- base::as.difftime(minPadDay,units='days')
  timePadEnd <- base::as.difftime(maxPadDay,units='days')
} else {
  log$fatal('Poorly formed input argument PadDay. See documentation.')
  stop()
}
log$debug(base::paste0('Days to pad calibrations: ',base::paste0(Para$PadDay,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = NULL,
                              log = log)

# Process each file path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  wrap.cal.asgn(DirIn=idxDirIn,
                DirOutBase=Para$DirOut,
                TimeBgn=timeBgn,
                TimeEnd=timeEnd,
                PadDay=c(timePadBgn,timePadEnd),
                log=log
                )

  return()
  
} # End loop around datum paths
