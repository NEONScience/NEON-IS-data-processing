##############################################################################################
#' @title Workflow for Continuous Discharge Processing

#' @author
#' Zachary Nickerson \email{nickerson@battelleecology.org}

#' @description Workflow. Validates, cleans, and formats troll log files into daily parquets.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id.The source-id folder may have multiple csv log files. 
#' The source-id is the unique identifier of the sensor.   
#' 
#' 2. "DirInOS=value", The input path to the OS data tables.
#'        
#' 3. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 4. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' 
#' @return Cleaned troll log files in daily parquets.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio
# Sys.setenv(DIR_IN_OS='~/pfs/l4discharge_os_table_group',DIR_IN='~/pfs/l4discharge_csd_swe_group/2024/03/20/l4discharge_HOPB132100')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=$DIR_IN",
#          "DirInOS=$DIR_IN_OS",
#          "DirOut=~/pfs/out",
#          "DirErr=~/pfs/out/errored_datums")
# rm(list=setdiff(ls(),c('arg','log')))
# setwd('/home/NEON/nickerson/R/NEON-IS-data-processing/flow/flow.discharge.parse.os.inputs')

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-10-15) 
#     original creation
#   Nora Catolico(2025-11-18)
#     reorganized input directories and added error logging
##############################################################################################
options(digits.secs = 3)
library(lubridate)
library(foreach)
library(doParallel)
library(stringr)


# Source the wrapper function. Assume it is in the working directory
source("./wrap.discharge.parse.os.inputs.R")
source("./def.dir.in.partial.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,
                                      NameParaReqd = c("DirIn",
                                                       "DirInOS",
                                                       "DirOut",
                                                       "DirErr"),
                                      log = log)
list2env(Para,envir = .GlobalEnv)


# Echo arguments
log$debug(base::paste0('SWE/CSD Input directory: ', Para$DirIn))
log$debug(base::paste0('OS Table Input directory: ', Para$DirInOS))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

#what are the expected subdirectories of each input path
DirIn <-
  def.dir.in.partial(DirBgn = DirIn,
                     nameDirSubPartial = 'l4discharge',
                     log = log)

# Take stock of our OS tables. 
OStables <- base::list.files(DirInOS,full.names=FALSE)

# --------- Load the OS data ----------
# Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
csd_constantBiasShift_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.csv')))
if (base::any(base::class(csd_constantBiasShift_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.csv is unreadable.'))
  base::stop()
}

csd_dataGapToFillMethodMapping_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv')))
if (base::any(base::class(csd_dataGapToFillMethodMapping_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv is unreadable.'))
  base::stop()
}

csd_gapFillingRegression_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.csv')))
if (base::any(base::class(csd_gapFillingRegression_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.csv is unreadable.'))
  base::stop()
}

csd_gaugeWaterColumnRegression_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv')))
if (base::any(base::class(csd_gaugeWaterColumnRegression_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv is unreadable.'))
  base::stop()
}

sdrc_controlInfo_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv')))
if (base::any(base::class(sdrc_controlInfo_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv is unreadable.'))
  base::stop()
}

sdrc_curveIdentification_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv')))
if (base::any(base::class(sdrc_curveIdentification_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv is unreadable.'))
  base::stop()
}

sdrc_priorParameters_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv')))
if (base::any(base::class(sdrc_priorParameters_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv is unreadable.'))
  base::stop()
}

sdrc_gaugeDischargeMeas_pub  <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv')))
if (base::any(base::class(sdrc_gaugeDischargeMeas_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_gaugeDischargeMeas_pub.csv is unreadable.'))
  base::stop()
}

sdrc_sampledParameters_pub <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv')))
if (base::any(base::class(sdrc_sampledParameters_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_sampledParameters_pub.csv is unreadable.'))
  base::stop()
}

sdrc_gaugePressureRelationship_pub <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv')))
if (base::any(base::class(sdrc_gaugePressureRelationship_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_gaugePressureRelationship_pub.csv is unreadable.'))
  base::stop()
}

sdrc_stageDischargeCurveInfo_pub <-
  base::try(read.csv(paste0(DirInOS, '/NEON.DOM.SITE.DP4.00133.001.sdrc_stageDischargeCurveInfo_pub.csv')))
if (base::any(base::class(sdrc_stageDischargeCurveInfo_pub) == 'try-error')) {
  # Generate error and stop execution
  log$error(base::paste0('File ', DirInOS, '/NEON.DOM.SITE.DP1.00133.001.sdrc_stageDischargeCurveInfo_pub.csv is unreadable.'))
  base::stop()
}


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.discharge.parse.os.inputs(
        DirIn=idxDirIn,
        csd_constantBiasShift_pub=csd_constantBiasShift_pub,
        csd_dataGapToFillMethodMapping_pub=csd_dataGapToFillMethodMapping_pub,
        csd_gapFillingRegression_pub=csd_gapFillingRegression_pub,
        csd_gaugeWaterColumnRegression_pub=csd_gaugeWaterColumnRegression_pub,
        sdrc_controlInfo_pub=sdrc_controlInfo_pub,
        sdrc_curveIdentification_pub=sdrc_curveIdentification_pub,
        sdrc_priorParameters_pub=sdrc_priorParameters_pub,
        sdrc_gaugeDischargeMeas_pub=sdrc_gaugeDischargeMeas_pub,
        sdrc_sampledParameters_pub=sdrc_sampledParameters_pub,
        sdrc_gaugePressureRelationship_pub=sdrc_gaugePressureRelationship_pub,
        sdrc_stageDischargeCurveInfo_pub=sdrc_stageDischargeCurveInfo_pub,
        DirOutBase=Para$DirOut,
        log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        log$error(err$message)
        InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn, 
                                                             log = log)
        DirSub <- strsplit(InfoDirIn$dirRepo,".", fixed = TRUE)[[1]][1]
        NEONprocIS.base::def.dir.crea(DirBgn = Para$DirErr, DirSub = DirSub, 
                                      log = log)
        csvname <- DirSub %>%
          strsplit( "/" ) %>%
          sapply( tail, 1 )
        nameFileErr <- base::paste0(Para$DirErr, DirSub, "/",csvname)
        log$info(base::paste0("Re-routing failed datum path to ", nameFileErr))
        con <- base::file(nameFileErr, "w")
        base::close(con)
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
}

# End flow.discharge.os.inputs