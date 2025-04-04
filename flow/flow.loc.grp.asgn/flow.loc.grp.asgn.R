##############################################################################################
#' @title Location or group assignment module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Workflow. Assign the location file(s) for a sensor ID to each data day which it applies
#' over 1 or more data years. When assigning the location file to each data day, the location 
#' information is filtered to that relevant only to the data day and resaved. This includes truncating
#' any dates in the locations file that span the data day to the start or end of the data day, as applicable. 
#' Original dates falling within the data day will not be modified. This code works for 
#' both sensor-based location files as well as location-based location files.
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine the years over which to assign location files
#'    For each sensor ID:
#'      Read the location file(s) for the sensor ID 
#'      Create a folder structure of all relevant data days for the sensor ID.
#'      Filter the original location file for info relevant to the data day and resave it in the location
#'          directory for that sensor ID and day
#'      
#' This script is run at the command line with the following arguments. Each argument must be a string in the format
#' "Para=value", where "Para" is the intended parameter name and "value" is the value of the parameter.
#' Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter will be assigned
#' from the system environment variable matching the value string. The arguments are:
#'
#' 1. "DirIn=value", where value is the starting directory path where to search for location or group files. 
#' The full repository must be structured as follows: #/pfs/BASE_REPO/SOURCE_TYPE/ID, 
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' The SOURCE_ID folder holds any number of location files pertaining to the SOURCE_ID. 
#' There may be no further subdirectories of SOURCE_ID.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/prt/27134/:
#'    prt_27134_locations.json
#' 
#' Note that DirIn can be any point between #/pfs and #/pfs/BASE_REPO/SOURCE_TYPE/SOURCE_ID. 
#' For the folder stucture in the example above, DirIn=/scratch/pfs will process all SOURCE_IDs found 
#' within the recursive path structure. In contrast, DirIn=/scratch/pfs/proc_group_prt/27134
#' will process only SOURCE_ID 27134.
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
#' 5. "TypeFile=value", where value is the type of file. 
#' Options are 'asset', 'namedLocation', and 'group'. Only one may be specified. 'asset' corresponds to a 
#' location file for a particular asset, which includes information about where and for how long
#' the asset was installed, including its geolocation history. 'namedLocation' corresponds to a 
#' location file specific to a named location, including the properties of that named location and
#' the dates over which it was active (should have been producing data). 'group' corresponds to a group
#' file specific to a group member, including what groups the member is in and properties of the group.
#' 
#' 5. "Prop=value" (optional), where value contains any number of properties in the location or group 
#' file to retain, separated by pipes (|). The meaning of this input changes according to TypeFile. 
#' See the filtering functions for each type for details. Defaults to 'all', which results in no filtering.
#'  Currently only relevant for 'TypeFile=asset', with filtering function NEONprocIS.base::def.loc.filt.

#' Note: This script implements optional parallelization as well as logging (described in 
#' \code{\link[NEONprocIS.base]{def.log.init}}), both of which use system environment variables if available. 

#' @return A directory structure in the format DirOut/SOURCE_TYPE/YEAR/MONTH/DAY/SOURCE_ID/location, where 
#' DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the terminal path 
#' includes the filtered location files applicable to the year, month, day, and source_id indicated in 
#' the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.loc.grp.asgn.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "TypeFile=asset"

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}
#' @seealso \code{\link[NEONprocIS.base]{def.loc.filt}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-03-15)
#     original creation, refactored from flow.loc.filt
#   Cove Sturtevant (2021-08-31)
#     Add datum error routing
#   Cove Sturtevant (2022-11-22)
#     Add option for group files
#   Cove Sturtevant (2023-11-16)
#     Add option for retaining only particular properties
#   Cove Sturtevant (2024-11-27)
#     Allow good records to pass through, while removing bad records and routing to errored datums
#   Cove Sturtevant (2025-03-20)
#     Accommodate location_properties:<property> syntax for Prop argument (see def.loc.filt)
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.loc.grp.asgn.R")

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
    NameParaReqd = c("DirIn", "DirOut","DirErr","FileYear","TypeFile"),
    NameParaOpt = c('Prop'),
    ValuParaOpt = list(Prop = 'all'),
    log = log
  )

# Re-construct the Prop argument if location_properties was included (potentially in the form location_properties:<property>)
idxProp <- base::which(base::substr(arg,start=1,stop=5) == 'Prop=')
if (base::length(idxProp) == 1 & 'location_properties' %in% Para$Prop){
    Prop <- base::sub('Prop=','',arg[idxProp])
    Para$Prop <- base::strsplit(Prop,'[|]')[[1]]
}
  

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Properties to retain: ', base::paste0(Para$Prop,collapse=',')))

# Parse the file containing the years to populate
log$debug(base::paste0('File containing data years to populate: ', Para$FileYear))
yearFill <- base::as.integer(base::readLines(con=Para$FileYear))
if(base::length(yearFill) == 0 || base::any(base::is.na(yearFill))){
  log$fatal(base::paste0('Cannot determine years to populate from file: ', Para$FileYear,'. Check file contents.'))
  stop()
}
timeBgn <- base::as.POSIXct(x=paste0(min(yearFill),'-01-01'),tz='GMT')
timeEnd <- base::as.POSIXct(x=paste0(max(yearFill)+1,'-01-01'),tz='GMT')

# Check that TypeFile is one of 'asset', 'namedLocation', or 'group'
log$debug(base::paste0('Type of location files: ', Para$TypeFile))
if(base::length(Para$TypeFile) != 1 || !(Para$TypeFile %in% c('asset','namedLocation','group'))){
  log$fatal("TypeFile must be either 'asset', 'namedLocation', or 'group'. See documentation.")
  stop()
}

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
      wrap.loc.grp.asgn(DirIn=idxDirIn,
                    DirOutBase=Para$DirOut,
                    TimeBgn=timeBgn,
                    TimeEnd=timeEnd,
                    TypeFile=Para$TypeFile,
                    Prop=Para$Prop,
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
          RmvDatmOut=FALSE, # Set to FALSE so that good records make it through
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

