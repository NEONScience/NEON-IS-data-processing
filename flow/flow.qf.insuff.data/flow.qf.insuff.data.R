##############################################################################################
#' @title Workflow for insufficient data calculations

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}

#' @description Workflow. Uses number of measuremnts in averaging window to determine whether insufficient  
#' data quality flag should be applied. 
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The base file path to the statistics data (including number of points) and the QM data.
#' 
#' 2-N. "insuffInfoX=value", where X is a number beginning at 1 and value is a group of parameter fields and 
#' user-defined values for determining data insufficiency. Each group is a single argument, where the field is listed first followed 
#' by any applicable value strings, separated by pipes. All groups must include the following fields:
#'         term - Name of the product e.g."nitrate" 
#'         wndw - window of time for which insufficient data will be calculated
#'         minPoints - minimum number of points required to not trigger the insufficient data quality flag
#' There may be multiple assignments of insuffInfoX, specified by incrementing the number X by 1 with each additional argument. 
#' For example, a 3-argument set of parameter-value groups could be: 
#' "insuffInfo1=term:nitrate|wndw:030|minPoints:10"
#' "insuffInfo3=term:nitrate|wndw:005|minPoints:5"
#' "insuffInfo3=term:temp|wndw:030|minPoints:10"
#' 
#' N+1. "DirOut=value", The base file path for the output data.
#' 
#' N+2. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' N+3. "SchmStats=value" (optional), The avro schema for the input and output stats file.
#' 
#' N+4. "SchmQMs=value" (optional), The avro schema for the updated QMs (insufficientDataQF added).   
#' 
#' N+5. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, that are to be copied with a symbolic link to the output path. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Updated stats and QMs data files in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' flow.qf.insuff.data <- function(DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733",                        
#'                               insuffInfo1<-"term:nitrate|wndw:015|minPoints:5",
#'                               DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" ,
#'                               SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse=''), 
#'                               SchmQMs<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_quality_metrics.avsc'),collapse=''),
#'                               log=log)
#' Stepping through the code in R studio                               
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate-surfacewater_CRAM103100/sunav2/CFGLOC110733",
#           "insuffInfo1=term:nitrate|wndw:015|minPoints:5",
#           "insuffInfo2=term:nitrate|wndw:030|minPoints:10",
#           "DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums","DirSubCopy=location",
#           "SchmQMs=~/pfs/nitrate_avro_schemas/nitrate/nitrate_insufficient_data.avsc")
# rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#' Bobby Hensley (2025-10-31)
#' Initial creation.
#' Nora Catolico (2025-11-04)
#' add in copied directories
#' Nora Catolico (2025-12-11)
#' fix schema outputs
#' Nora Catolico (2026-02-06)
#' Updated code structure for data frame input of multiple variables.
#' 
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.qf.insuff.data.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn","insuffInfo1","DirOut","DirErr"),
                                      NameParaOptn = c(base::paste0("insuffInfo",2:100),"SchmStats","SchmQMs","DirSubCopy"),log = log)

# Echo arguments
log$debug(base::paste0('Input data directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output stats: ', Para$SchmStats))
log$debug(base::paste0('Schema for output QMs: ', Para$SchmQMs))
log$debug(base::paste0('Director to copy: ', Para$DirSubCopy))

# These are input as subsequent arguments with term and context strings separated by pipes. 
namesParaInsuffInfo <- base::names(Para)[names(Para) %in% base::paste0("insuffInfo",1:100)]
numInsuffInfo <- base::length(namesParaInsuffInfo)
insuffParam <- data.frame(
  InfoSet = character(),
  field = character(),
  value = character(),
  stringsAsFactors = FALSE
)
for(idx in base::seq_len(numInsuffInfo)){
  nameParaInsuffInfo <- namesParaInsuffInfo[idx]
  splt <- Para[[namesParaInsuffInfo[idx]]]
  numSplt <- base::length(splt)
  if(!"term" %in% splt | !"wndw" %in% splt | !"minPoints" %in% splt){
    log$error(base::paste0('ERROR: insuffInfo',idx,' does not contain the required parameters (term, wndw, minpoints)'))
    stop()
  }
  if (numSplt > 1) {
    rpt <- base::data.frame(InfoSet = nameParaInsuffInfo, field = splt[base::seq.int(from = 1,to = numSplt, by = 2)], value = splt[base::seq.int(from = 2,to = numSplt, by = 2)], stringsAsFactors = FALSE)
  }
  insuffParam <- rbind(insuffParam,rpt)
}
if(nrow(insuffParam)>=3){
  log$debug(base::paste0('Insufficient data parameters successfully read in for ',nrow(insuffParam)/3,' info sets.'))
}else{
  log$error(base::paste0('Error reading in info sets.'))
  stop()
}

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if(base::is.null(Para$SchmStats) || Para$SchmStats == 'NA'){
  SchmStats <- NULL
} else {
  SchmStats <- base::paste0(base::readLines(Para$SchmStats),collapse='')
}
if(base::is.null(Para$SchmQMs) || Para$SchmQMs == 'NA'){
  SchmQMs <- NULL
} else {
  SchmQMs <- base::paste0(base::readLines(Para$SchmQMs),collapse='')
}


# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = c('stats','quality_metrics'),
                              log = log)

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'stats'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxFileIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to file: ', idxFileIn))
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.qf.insuff.data(
        DirIn=idxFileIn,
        insuffParam=insuffParam,
        DirOutBase=Para$DirOut,
        SchmStats=SchmStats,
        SchmQMs=SchmQMs,
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
}




