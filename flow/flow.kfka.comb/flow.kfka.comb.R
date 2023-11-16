##############################################################################################
#' @title Combine kafka extraction output and conform to L0 schema

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Merge multiple L0 data files generated for a single day. This is very 
#' common for kafka-extracted output because kafka data is pulled by received day rather than
#' data day, thus typically creating at least 2 files for a data day due to short time lag of
#' data transmission spanning midnight UTC. Also remove any duplicate data -- it is common
#' for Kafka to restream data with new kafka offsets when kafka does a hard restart.
#' In addition to merging output for each data day, removes kafka-generated columns so that
#' the data conform to the expected L0 schema.
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read the L0 schema
#'      Read in the L0 data files into an arrow dataset
#'      Pull columns indicated in the L0 schema and sort by time 
#'      Write out the data to a single file
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
#' Nested within the path for each source ID is the folder:
#'         /data
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_SOURCEID_YYYY-MM-DD_KAFKAOFFSETBEGIN_KAFKAOFFSETEND.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/li191r_data_source_kafka/li191r/2023/03/01/11346/data/ with nested files:
#'    li191r_11346_2023-03-05_13275082_13534222.parquet
#'    li191r_11346_2023-03-05_13534225_13534273.parquet
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
#' 4. "FileSchmL0=value", where value is the full path to the avro schema for the L0 data for the source type.
#' This schema will be used to constrain the columns that are output and ensure proper column ordering.
#' 
#' 5. "RmvDupl=value" (optional), where value is TRUE or FALSE indicating whether to remove duplicated 
#' rows in the data (as defined by the L0 schema columns). Defaults to TRUE.
#' 
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the data folder that are to be copied with a
#' symbolic link to the output path. May NOT include 'data'. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository with the merged Kafka files in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not Run - uses all available defaults
#' Rscript flow.kfka.comb.R "DirIn=/scratch/pfs/li191r_data_source_kafka" "DirOut=/scratch/pfs/out" "DirErr=/scratch/pfs/out/errored_datums" "FileSchmL0=~/R/avro-schemas/schemas/li191r/li191r.avsc" 
#'
#' Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='/scratch/pfs/li191r_data_source_kafka')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "DirErr=/scratch/pfs/out/errored_datums", "FileSchmL0=~/R/avro-schemas/schemas/li191r/li191r.avsc")
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-03-08)
#     original creation
#   Cove Sturtevant (2023-11-13)
#     add option to remove duplicated rows
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.kfka.comb.R")

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
                     "DirErr",
                     "FileSchmL0" 
                     ),
    NameParaOptn = c(
                     "DirSubCopy",
                     "RmvDupl"
                     ),
    ValuParaOptn = list(
                    RmvDupl=TRUE
                    ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Remove duplicate rows in the data?: ', Para$RmvDupl))


# Retrieve L0 data schema
FileSchmL0 <- Para$FileSchmL0
log$debug(base::paste0('L0 data schema: ',FileSchmL0))

# Read in the schema 
if(base::is.null(FileSchmL0) || FileSchmL0 == 'NA'){
  log$fatal("L0 schema must be input")
  stop()
} else {
  SchmL0 <- base::paste0(base::readLines(FileSchmL0),collapse='')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- c('data')
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
      wrap.kfka.comb(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        SchmL0=SchmL0,
        RmvDupl=Para$RmvDupl,
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
