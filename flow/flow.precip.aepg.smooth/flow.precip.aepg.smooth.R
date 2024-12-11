##############################################################################################
#' @title Compute average depth of individual strain gauges, smooth, and compute precipitation for 
#' Belfort AEPG600m sensor

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Compute the average depth and related QC flags for the
#' Belfort AEPG600m sensor, then apply a smoothing algorithm over multiple days to 
#' reduce or eliminate period noise due to environmental variation and draw out actual
#' precipitation events. Compute precipitation sums, uncertainty, and quality flags
#' for hourly and daily intervals. 
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in the L0 data files into arrow datasets
#'      Compute average depth streams and performed related QC
#'      Average consolidated depth to X minutes (user defined in thresholds) 
#'      Smooth average depth stream to compute hourly and daily precip for 3 central days
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
#' Input path = /scratch/pfs/aepg600m_calibration_group_and_convert/aepg600m_heated/2023/01/01/17777/data/ with nested file:
#'    aepg600m_heated_17777_2023-01-01.parquet
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
#' 4. "FileSchmStatHour=value" (optional), where value is the full path to schema for the hourly aggregated data 
#' output by this workflow. If not input, a schema will be automatically crafted. 
#' The output is ordered as follows:
#' startDateTime
#' endDateTime
#' precipBulk
#' precipBulkExpUncert
#' insuffDataQF
#' extremePrecipQF
#' heaterErrorQF
#' dielNoiseQF
#' strainGaugeStabilityQF
#' evapDetectedQF
#' inletHeater1QM
#' inletHeater2QM
#' inletHeater3QM
#' inletHeaterNAQM
#' finalQF
#' Ensure that any schema input here matches the column order of the auto-generated schema, 
#' simply making any desired changes to column names.
#'
#' 5. "FileSchmStatDay=value" (optional), where value is the full path to schema for the daily aggregated data 
#' output by this workflow. The columns are the same as the hourly output, except that startDate and endDate 
#' are the timestamp columns (instead of startDateTime and endDateTime). If not input, a schema will be 
#' automatically crafted. 
#'
#' 6. "FileSchmQfGage=value" (optional), where value is the full path to output schema for the quality flags
#' representing the results of quality tests run on the individual strain gauge measurements. These quality flags
#' are aggregated across the three strain gauges such that a single flag is output for each test (instead of 
#' one quality flag for each strain gauge). If not input, a schema will be automatically crafted. 
#' The output is ordered as follows:
#' readout_time
#' all quality flags output by the plausibility module (retaining the same order)
#' all quality flags output by the calibration module (retaining the same order)
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
#' @return A repository with the aggregated sensor depth data and flags in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not Run - uses all available defaults
#' Rscript flow.precip.aepg.smooth.R "DirIn=/scratch/pfs/precipWeighing_thresh_select_ts_pad_smoother/2024/05/30" "DirOut=/scratch/pfs/out" "DirErr=/scratch/pfs/out/errored_datums"  
#'
#' Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='/scratch/pfs/precipWeighing_thresh_select_ts_pad_smoother/2024/05/30')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "DirErr=/scratch/pfs/out/errored_datums")
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant & Teresa Burlingame (2024-06-13)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function and other dependency functions. Assume it is in the working directory
source("./wrap.precip.aepg.smooth.R")
source("./def.ucrt.agr.precip.bench.R")
source("./def.precip.depth.smooth.R")

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
                     "FileSchmStatHour",
                     "FileSchmStatDay",
                     "FileSchmQfGage"
                     ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve output schema for hourly stats
FileSchmStatHour <- Para$FileSchmStatHour
log$debug(base::paste0('Output schema for hourly bulk precipitation stats: ',base::paste0(FileSchmStatHour,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmStatHour) || FileSchmStatHour == 'NA'){
  SchmStatHour <- NULL
} else {
  SchmStatHour <- base::paste0(base::readLines(FileSchmStatHour),collapse='')
}

# Retrieve output schema for daily stats
FileSchmStatDay <- Para$FileSchmStatDay
log$debug(base::paste0('Output schema for bulk precipitation stats: ',base::paste0(FileSchmStatDay,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmStatDay) || FileSchmStatDay == 'NA'){
  SchmStatDay <- NULL
} else {
  SchmStatDay <- base::paste0(base::readLines(FileSchmStatDay),collapse='')
}

# Retrieve output schema for strain gauge quality flags
FileSchmQfGage <- Para$FileSchmQfGage
log$debug(base::paste0('Output schema for aggregated strain gauge quality flags: ',base::paste0(FileSchmQfGage,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmQfGage) || FileSchmQfGage == 'NA'){
  SchmQfGage <- NULL
} else {
  SchmQfGage <- base::paste0(base::readLines(FileSchmQfGage),collapse='')
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
      wrap.precip.aepg.smooth(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmStatHour=SchmStatHour,
        SchmStatDay=SchmStatDay,
        SchmQfGage=SchmQfGage,
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
