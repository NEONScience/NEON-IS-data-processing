##############################################################################################
#' @title Workflow for Level Troll 500 and Aqua Troll 200 Science Computations
#' flow.troll.uncertainty.R
#' 
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Workflow. Calculate elevation and derive uncertainty  for surface and groundwater troll data products.
#' 
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 4. "Context=value", where the value must be designated as either "surfacewater" or "groundwater".
#' 
#' 5. "WndwAgr=value", (optional) where value is the aggregation interval for which to compute unceratainty. It is 
#' formatted as a 3 character sequence, typically representing the number of minutes over which to compute unceratainty 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals. No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' 6. "WndwInst=TRUE", (optional) set to TRUE to include instantaneous uncertainty data output. The defualt value is FALSE. 
#' No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' 7. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' 9. "FileSchmUcrtAgr=value" (optional), where values is the full path to the avro schema for the output aggregate 
#' uncertainty data file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' 9. "FileSchmUcrtInst=value" (optional), where values is the full path to the avro schema for the output instantaneous 
#' uncertainty data file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' 10. "FileSchmStats=value" (optional), where values is the full path to the avro schema for the output statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#' 
#' 11. "FileSchmSciStats=value" (optional), where values is the full path to the avro schema for the output science statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#' @return water table elevation calculated from calibrated pressure, density of water, gravity, and sensor elevation.
#' Data and uncertainty values will be output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#'  
#' If no output schema is provided for the data, the output column/variable names will be determined by the 
#' sensor type (leveltroll500 or aquatroll200). Output column/variable names for the leveltroll500 will be
#' readout_time, pressure, pressure_data_quality, temperature, temperature_data quality, elevation, in that order. 
#' Output column/variable names for the aquatroll200 will be readout_time, pressure, pressure_data_quality, 
#' temperature, temperature_data quality, conductivity, conductivity_data_quality, elevation, in that order.
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' @keywords Currently none
#' 
#' @examples
#' Stepping through the code in Rstudio 
# Sys.setenv(DIR_IN='~/pfs/surfacewaterPhysical_analyze_pad_and_qaqc_plau') #troll data
# Sys.setenv(DIR_IN='~/pfs/surfacewaterPhysical_group_path') #uncertainty data
# Sys.setenv(FILE_SCHEMA_STATS_AQUATROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_aquatroll200_dp01_stats.avsc')
# Sys.setenv(SCHEMA_DATA_TROLL_AQUATROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_data.avsc')
# Sys.setenv(SCHEMA_UCRT_AGR_TROLL_AQUATROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt.avsc')
# Sys.setenv(SCHEMA_UCRT_INST_TROLL_AQUATROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt_inst.avsc')
# Sys.setenv(FILE_SCHEMA_STATS_LEVELTROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_leveltroll500_dp01_stats.avsc')
# Sys.setenv(SCHEMA_DATA_TROLL_LEVELTROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_data.avsc')
# Sys.setenv(SCHEMA_UCRT_AGR_TROLL_LEVELTROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_ucrt.avsc')
# Sys.setenv(SCHEMA_UCRT_INST_TROLL_LEVELTROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_ucrt_inst.avsc')
# Sys.setenv(SCHEMA_SCI_TROLL='~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_troll_specific_sci_stats.avsc')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","DirErr=~/pfs/out/errored_datums","Context=surfacewater","WndwInst=TRUE","WndwAgr=005|030",
#          "FileSchmData=$SCHEMA_DATA_TROLL_AQUATROLL","FileSchmUcrtAgr=$SCHEMA_UCRT_AGR_TROLL_AQUATROLL","FileSchmUcrtInst=$SCHEMA_UCRT_INST_TROLL_AQUATROLL",
#          "FileSchmStats=$FILE_SCHEMA_STATS_AQUATROLL","FileSchmSciStats=$SCHEMA_SCI_TROLL")
#' rm(list=setdiff(ls(),c('arg','log')))
#' 
#' @seealso None currently
#' changelog and author contributions / copyrights
#'   Nora Catolico (2021-02-02)
#'     original creation
#'   Nora Catolico (2023-03-03)
#'     updated for no troll data use case
#'   Nora Catolico (2023-08-30)
#'     updated for inst SW outputs for L4 discharge
#'   Nora Catolico (2023-09-26)
#'     updated for multiple sensors in one day 
#'   Nora Catolico (2024-01-29)
#'     updated to include water column height uncertainty for L4 discharge 
#'     distinguish between average and instantaneous uncertainty outputs
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.troll.uncertainty.R")

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","DirErr","Context"),NameParaOptn = c("FileSchmData","FileSchmUcrtAgr","FileSchmUcrtInst","FileSchmStats","FileSchmSciStats","WndwInst","WndwAgr"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Context: ', Para$Context))
log$debug(base::paste0('Instantaneous window: ', Para$WndwInst))
log$debug(base::paste0('Aggregation interval(s): ', Para$WndwAgr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output aggregate uncertainty: ', Para$FileSchmUcrtAgr))
log$debug(base::paste0('Schema for output instantaneous uncertainty: ', Para$FileSchmUcrtInst))
log$debug(base::paste0('Schema for output stats: ', Para$FileSchmStats))
log$debug(base::paste0('Schema for output science stats: ', Para$FileSchmSciStats))

# Read in the schemas so we only have to do it once and not every
# time in the avro writer.
if(base::is.null(Para$FileSchmData) || Para$FileSchmData == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(Para$FileSchmData),collapse='')
}
if(base::is.null(Para$FileSchmUcrtAgr) || Para$FileSchmUcrtAgr == 'NA'){
  SchmUcrtOutAgr <- NULL
} else {
  SchmUcrtOutAgr <- base::paste0(base::readLines(Para$FileSchmUcrtAgr),collapse='')
}
if(base::is.null(Para$FileSchmUcrtInst) || Para$FileSchmUcrtInst == 'NA'){
  SchmUcrtOutInst <- NULL
} else {
  SchmUcrtOutInst <- base::paste0(base::readLines(Para$FileSchmUcrtInst),collapse='')
}
if(base::is.null(Para$FileSchmStats) || Para$FileSchmStats == 'NA'){
  SchmStatsOut <- NULL
} else {
  SchmStatsOut <- base::paste0(base::readLines(Para$FileSchmStats),collapse='')
}
if(base::is.null(Para$FileSchmSciStats) || Para$FileSchmSciStats == 'NA'){
  SchmSciStatsOut <- NULL
} else {
  SchmSciStatsOut <- base::paste0(base::readLines(Para$FileSchmSciStats),collapse='')
}


# Retrieve context
if(Para$Context=="groundwater"){
  Context<-"GW"
}else if(Para$Context=="surfacewater"){
  Context<-"SW"
}else{
  log$fatal('Context must equal groundwater or surfacewater.')
  stop()
}

# Retrieve instantaneous information
if(base::is.null(Para$WndwInst) || Para$WndwInst == 'NA'|| Para$WndwInst == "FALSE"){
  WndwInst <- FALSE
}else{
  WndwInst <- TRUE
}

# Retrieve aggregation interval(s)
if(base::is.null(Para$WndwAgr) || Para$WndwAgr == 'NA'){
  WndwAgr <- NULL
}else{
  WndwAgr <- base::as.difftime(base::as.numeric(Para$WndwAgr),units="mins")
}

#what are the expected subdirectories of each input path
nameDirSub <- c('data','flags','location')
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)

# Create the binning for each aggregation interval
if(length(WndwAgr)>0){
  timeBgnDiff <- list()
  timeEndDiff <- list()
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
    timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  } # End loop around aggregation intervals
}


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.troll.uncertainty(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        Context=Context,
        WndwAgr=WndwAgr,
        WndwInst=WndwInst,
        SchmDataOut=SchmDataOut,
        SchmUcrtOutAgr=SchmUcrtOutAgr,
        SchmUcrtOutInst=SchmUcrtOutInst,
        SchmStatsOut=SchmStatsOut,
        SchmSciStatsOut=SchmSciStatsOut,
        timeBgnDiff=timeBgnDiff,
        timeEndDiff=timeEndDiff,
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
