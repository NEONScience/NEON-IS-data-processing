##############################################################################################
#' @title Truncate and combine/merge sensor-based by location  

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Truncate and combine/merge sensor-based by location. 
#' After data have been grouped by location, truncate the data for each sensor based on when it was 
#' installed in the specified location and merge it with data from other sensors at the same location.
#' 
#' This script is run at the command line with 3 or 4 arguments. Each argument must be a string in the 
#' format "Para=value", where "Para" is the intended parameter name and "value" is the value of the 
#' parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter 
#' will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the input path, structured as follows:  #/pfs/BASE_REPO/#/yyyy/mm/dd/#, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 'pfs', 
#' the same name as subdirectories expected at the terminal directory (see below)), or recognizable as the 
#' 'yyyy/mm/dd' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data 
#' contained in the folder. 
#' 
#' Nested within this path is a directory named for the location identifier of the data included 
#' within it. (e.g. #/pfs/BASE_REPO/#/yyyy/mm/dd/#/CGFLOC12345/). The location identifier will be matched 
#' against the location information supplied in the location files (see below). Further nested within the 
#' location identifier folder is (at a minimum) the folder:
#'         location/
#' 
#' The location folder holds json files with the location data corresponding to the data files in the data
#' directory. The source-ids for which data are expected are gathered from the location files. 
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "DirSubCombData=value" (optional), where the value is the name of subfolders holding timeseries files 
#' (e.g. data, flags) to be truncated and/or merged, separated by pipes (|). These additional subdirectories must 
#' be at the same level as the location directory. Within each subfolder are timeseries files from one or more 
#' source-ids. The source-id is the identifier for the sensor that collected the data in the file, and must be included
#' somewhere in the file name (and must match one of the source-ids gathered from the location files). The data folders
#' listed here may contain files holding different types of data (e.g. one file holds calibration flags, 
#' another holds sensor diagnostic flags). However, the file names for each type must be identical with the exception of
#' the source ID. An attempt will be made to group the files that have the same naming convention and merge/truncate 
#' files matching the convention. For example, prt_12345_sensorFlags.parquet will be merged with 
#' prt_678_sensorFlags.parquet, and prt_12345_calFlags.parquet will be merged with prt_678_calFlags.parquet, since 
#' the file names in each of these two groups are identical with the exception of the source ID.
#' 
#' 5. "DirSubCombUcrt=value" (optional), where the value is the name of subfolders holding uncertainty coefficient 
#' json files to be merged, separated by pipes (|). These additional subdirectories must be at the same level as 
#' the location directory. Within each subfolder are uncertainty json files, one for each source-id. The 
#' source-id is the identifier for the sensor pertaining to the uncertainty info in the file, and must be somewhere
#' in the file name. 
#' 
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A repository structure in DirOut, where DirOut replaces the input directory 
#' structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure 
#' of the input path. A single merged file will replace the originals in each of the subdirectories
#' specified in the input arguments. The merged files will be written with the same schema as returned from 
#' reading the input file(s). The characters representing the source-id in the merged filenames will be replaced 
#' by the location name. All other subdirectories indicated in DirSubCopy will be carried through unmodified.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.loc.comb.data.R 'DirIn=/pfs/tempSoil_structure_repo_by_location/prt/2018/01/01' 'DirOut=/pfs/out' 'DirSubCombData=data|flags' 'DirSubCombUcrt=uncertainty_coef' 'DirSubCopy=location'
#' 
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/tempSoil_structure_repo_by_location/prt/2018/01/01')
#' Rscript flow.loc.comb.data.R 'DirIn=$DIR_IN' 'DirOut=/pfs/out' 'DirSubCombData=data|flags' 'DirSubCombUcrt=uncertainty_coef' 'DirSubCopy=location'

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-10)
#     original creation
#   Cove Sturtevant (2019-08-20)
#     adjust reading of source-id to account for modifiers in the file name
#   Cove Sturtevant (2019-09-12)
#     added reading of input path and file schemas from environment variables
#     simplified fatal errors to not stifle the R error message
#     use input file schemas read in with data to write output files
#   Cove Sturtevant (2019-09-27)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2019-10-23)
#     added merging of uncertainty files
#   Cove Sturtevant (2020-03-04) 
#     adjust datum identification to allow copied-through directories to be present or not
#   Cove Sturtevant (2020-04-15)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2020-07-20)
#     adjusted determination of source_ids to be read from location files rather than assuming
#     an ordered component of the data file names
#   Cove Sturtevant (2020-09-22)
#     added support for multiple data files with different schemas within the same folder. Grouping
#     based on file names is now attempted, truncating and/or merging done within each group
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
#   Cove Sturtevant (2021-05-10)
#     moved main functionality into wrapper function
#   Cove Sturtevant (2021-08-31)
#     Add datum error routing
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.loc.data.trnc.comb.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

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
Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c("DirIn","DirOut","DirErr"),
                                      NameParaOptn=c("DirSubCombData","DirSubCombUcrt","DirSubCopy"),
                                      log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output paths
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve optional data directories to merge timeseries files in
DirSubCombData <- Para$DirSubCombData
log$debug(base::paste0('Subdirectories to combine timeseries files within: ',base::paste0(DirSubCombData,collapse=',')))

# Retrieve optional uncertainties directories to merge uncertainty files in
DirSubCombUcrt <- Para$DirSubCombUcrt
log$debug(base::paste0('Subdirectories to combine uncertainty files within: ',base::paste0(DirSubCombUcrt,collapse=',')))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,DirSubCombData))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list('location')
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.loc.data.trnc.comb(DirIn=idxDirIn,
                              DirOutBase=DirOut,
                              DirSubCombData=DirSubCombData,
                              DirSubCombUcrt=DirSubCombUcrt,
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

