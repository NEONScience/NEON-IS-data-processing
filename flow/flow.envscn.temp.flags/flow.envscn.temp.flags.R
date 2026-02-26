##############################################################################################
#' @title Assess soil temperature closest to sensor depths to determine if data should be flagged.

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' 
#' @description Workflow. Compute the heater and status flags by assessing the bit rate. Only 
#' flagging alarm codes of interest. Add columns to qfPlau table prior to push to QM calculation module. 
#'
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in the L0 data files into arrow datasets
#'      Compute flags based on sensor status streams
#'      aggregate data to 5 and 30 minutes
#'      Write stats and flags output to file
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
#' The data/flags folders holds any number of daily data/flags files padded around the yyyy/mm/dd in the input path.
#' #' 
#' For example:
#' Input path = precipWeighingv2_analyze_pad_and_qaqc_plau/2025/03/31/precip-weighing-v2_HQTW900000/pluvio/CFGLOC114405
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
#' 4. "SchmQF=value" (optional), where value is the full path to schema for the QF flags after inputing custom flags
#'
#' 5. "DirTemp=valuë", where the value is the input path for soil temperature and location data to perform test. 
#' 
#' Ensure that any schema input here matches the column order of the auto-generated schema, 
#' simply making any desired changes to column names.
#'
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
#' sensor location folder are "data" and "flags". 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN="DirIn=/scratch/pfs/concH2oSoilSalinity_analyze_pad_and_qaqc_plau/2025/10/17/conc-h2o-soil-salinity_GRSM001501/")
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg = c( "DirIn=$DIR_IN",
#'          "DirTemp=/scratch/pfs/concH2oSoilSalinity_group_path/2025/10/17/conc-h2o-soil-salinity_GRSM005501/",
#'          "DirOut=/scratch/pfs/tb_out",
#'          "DirErr=/scratch/pfs/tb_out/errored_datums")
#' Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-16)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)
# # Source the wrapper function and other dependency functions. Assume it is in the working directory
source("./wrap.envscn.temp.flags.R")
source("./def.apply.temp.flags.R")
source("./def.find.temp.sensor.R")
source("./def.load.temp.sensors.R")
source("./def.sort.qf.cols.R")
source("./def.calc.temp.flags.R")

# # Pull in command line arguments (parameters)
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
                     "DirTemp",
                     "SchmQf"
                     ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
#log$debug(base::paste0('Temperature directory: ', Para$DirTemp))

# Retrieve output schema for  flags
FileSchmQf <- Para$SchmQf

# Read in the schema 
if(base::is.null(FileSchmQf) || FileSchmQf == 'NA'){
  FileSchmQf <- NULL
} else {
  FileSchmQf <- base::paste0(base::readLines(FileSchmQf),collapse='')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- c('data','flags')
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Retrieve DirTemp if provided
FileDirTemp <- Para$DirTemp

# Read in the schema 
if(base::is.null(FileDirTemp) || FileDirTemp == 'NA'){
  FileDirTemp <- NULL
} else {
  log$debug(base::paste0('Temperature Directory provided: ', FileDirTemp))
  
}

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub =  nameDirSub,
                              log = log)


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  #if no temperature directory was provided assume it is two levels up from IdxDirIn. 
  if (is.null(FileDirTemp)){
    # Get the directory name two levels up
    idxDirTemp <- dirname(dirname(idxDirIn))
  } else {
    tempFiles <- list.files(FileDirTemp, full.names = T)
    idxDirTemp <- tempFiles[grepl(tempFiles, pattern =basename(dirname(dirname(idxDirIn))))]
  }
    # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.envscn.temp.flags(DirIn=idxDirIn,
                              DirOutBase=Para$DirOut,
                              DirTemp=idxDirTemp,
                              SchmQf=FileSchmQf, 
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
