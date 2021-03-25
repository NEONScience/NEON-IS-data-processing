##############################################################################################
#' @title Location filtering module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Location filtering module for NEON IS data processing. Applies a filter for 
#' the location information relevant to the data day as indicated in the file path, and resaves the 
#' filtered location file. This code works for both sensor-based location files as well as 
#' location-based location files.
#' 
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy over (by symbolic link) unmodified components
#'      Open each location file and delete location information that does not apply to the data day
#'         indicated in the file path. For location information that does apply, truncate any start/end
#'         dates in the location file to start/end at the data day/data day+1. 
#'      Write out the filtered location file
#'
#' This script is run at the command line with the following arguments. Each argument must be a string 
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter 
#' will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the input path, structured as follows:  #/pfs/BASE_REPO/#/yyyy/mm/dd/#, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 'pfs', 
#' 'location', or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 2-digit month, 
#' and 2-digit day of the data contained in the folder. The data day is identified from the input path.
#'   
#' Nested within this path is (at a minimum) the folder:
#'         /location  
#' The location folder holds a single json file holding the location data to be filtered. 
#' 
#' For example:
#' Input path = /scratch/pfs/proc_group/prt/2019/01/01/27134 with nested folders:
#'    /location 
#'    /data
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at 
#' the same level as the calibration folder in the input path that are to be copied with a symbolic link to the 
#' output path.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A filtered location json file in the location folder of DirOut, where DirOut replaces the input 
#' directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure 
#' of the input path. Any other additional subfolders specified in DirSubCopy are symbolically linked at the same 
#' level of the 'location' directory. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.loc.filt.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "SubDirCopy=data|flags|uncertainty_coef"
#' 
#' # Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/prt_calibration/prt/2019/01/01/27134')
#' Rscript flow.loc.filt.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "SubDirCopy=data|flags|uncertainty_coef"

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-01)
#     original creation
#   Cove Sturtevant (2019-09-12)
#     added reading of input path and file schemas from environment variables
#     simplified fatal errors to not stifle the R error message
#   Cove Sturtevant (2019-09-26)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2020-03-04) 
#     adjust datum identification to allow copied-through directories to be present or not
#   Cove Sturtevant (2020-08-19)
#     extend application to filter location-based location files as well
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
##############################################################################################
library(foreach)
library(doParallel)

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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),NameParaOptn="DirSubCopy",log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,c('location')))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('location'))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Data date
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutLoc <- base::paste0(idxDirOut,'/location')
  base::dir.create(idxDirOutLoc,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  

  # Get a list of location files
  idxDirInLoc <- base::paste0(idxDirIn,'/location')
  fileLoc <- base::dir(idxDirInLoc)
  
  # If there is no location file, skip
  numFileLoc <- base::length(fileLoc)
  if(numFileLoc == 0){
    log$warn(base::paste0('No location data in ',idxDirInLoc,'. Skipping...'))
    next()
  }
  
  # Filter the locations in the location file(s)
  if(numFileLoc > 1){
    log$warn(base::paste0('There is more than one location file in path: ',idxDirInLoc,'... Filtering them all!'))
  }
  for(idxFileLoc in fileLoc){
    
    # Filter the sensor location file for named locations and geolocations applicable to this day
    # Note: This will intentionally fail for namedLocation location files, for which the next 
    # statement applies.
    idxNameFileIn <- base::paste0(idxDirInLoc,'/',idxFileLoc)
    idxNameFileOut <- base::paste0(idxDirOutLoc,'/',idxFileLoc)
    loc <- try(NEONprocIS.base::def.loc.filt(NameFileIn=idxNameFileIn,
                           NameFileOut=idxNameFileOut,
                           TimeBgn=timeBgn,
                           TimeEnd=timeBgn+base::as.difftime(1,units='days')),
               silent=TRUE)
    if(class(loc) == 'try-error'){
      # Truncate the active dates for named location location files to the data date.
      loc <- try(NEONprocIS.base::def.loc.trnc.actv(NameFileIn=idxNameFileIn,
                                                    NameFileOut=idxNameFileOut,
                                                    TimeBgn=timeBgn,TimeEnd=timeBgn+base::as.difftime(1,units='days')),
                 silent=TRUE)
      
      # If we failed both functions, then there is a problem 
      if(class(loc) == 'try-error'){
        log$error(base::paste0('Attempted to filter location information for location file ',
                               idxNameFileIn,', treating it as a sensor-based location file as well as a location-based location. ',
                               'Both attempts failed. Check file.'))
        stop()
      }
    }

  } # End loop around location files
  
  return()
} # End loop around datum paths
