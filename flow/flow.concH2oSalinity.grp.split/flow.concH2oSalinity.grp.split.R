##############################################################################################
#' @title  Select and rename depth-specific columns from EnviroSCAN soil salinity group split data
#' (flow.concH2oSalinity.grp.split)

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. For each datum (CFGLOC directory produced by group_path_split), read the
#' group JSON to determine HOR and VER. Extract the depth index from VER (e.g., VER "503" -> depth 3),
#' then select only the columns corresponding to that depth from the stats and quality_metrics parquet
#' files. Rename those columns to remove the depth indicator (e.g., VSICDepth03Mean -> VSICMean).
#' Output files are written preserving the input directory structure.
#' 
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (CFGLOC directories containing stats and quality_metrics)
#'    For each datum:
#'      Read group JSON from sibling group/ directory to determine depth index
#'      Create output directories
#'      Read stats parquets, select depth-specific columns, rename, write output
#'      Read quality_metrics parquets, select depth-specific columns, rename, write output
#'      Copy additional subdirectories as requested
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. The input repo should be
#' structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/group-name/source-id
#' where # indicates any number of parent and child directories of any name, so long as they are not
#' 'pfs' or recognizable as the 'yyyy/mm/dd' structure. The group-name encodes site, HOR, and VER in
#' the format: conc-h2o-soil-salinity-split_SITE_HORVVER. The source-id (CFGLOC######) is the unique
#' identifier of the sensor location.
#'
#' Nested within the path for each source-id is (at a minimum) the folders:
#'         /stats
#'         /quality_metrics
#' At the group-name level, a sibling /group directory must also be present containing a JSON file
#' named <source-id>.json with HOR and VER properties for the location.
#'
#' For example:
#' Input path = /pfs/concH2oSoilSalinity_group_path_split/2025/10/17/
#'              conc-h2o-soil-salinity-split_GRSM004503/CFGLOC105332
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums
#' that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' 4. "FileSchmStats=value" (optional), where value is the full path to the avro schema for the
#' depth-renamed stats output. If not provided, schema is inferred from the data.
#'
#' 5. "FileSchmQm=value" (optional), where value is the full path to the avro schema for the
#' depth-renamed quality_metrics output. If not provided, schema is inferred from the data.
#'
#' 6. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the stats folder that are to be copied with a symbolic link to the
#' output path. May NOT include 'stats' or 'quality_metrics'.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository with depth-renamed stats and quality_metrics in DirOut, where DirOut replaces
#' BASE_REPO but otherwise retains the child directory structure of the input path.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not Run
#' Rscript flow.concH2oSalinity.grp.split.R \
#'   'DirIn=/pfs/concH2oSoilSalinity_group_path_split' \
#'   'DirOut=/pfs/out' \
#'   'DirErr=/pfs/out/errored_datums' \
#'   'FileSchmStats=/pfs/avro_schemas/concH2oSoilSalinity/concH2oSoilSalinity_dp01_stats.avsc' \
#'   'FileSchmQm=/pfs/avro_schemas/concH2oSoilSalinity/concH2oSoilSalinity_dp01_quality_metrics.avsc'
#'
#' # Stepping through in RStudio
#' Sys.setenv(DIR_IN='/home/tburlingame/pfs/concH2oSoilSalinity_group_path_split/2025/10/17/conc-h2o-soil-salinity-split_GRSM004503/CFGLOC105332')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/home/tburlingame/pfs/out_concH2oSalinity", "DirErr=/home/tburlingame/pfs/out_concH2oSalinity/errored_datums")
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-04-22)
#     original creation
#   Teresa Burlingame (2026-04-22)
#     renamed grp.comb -> grp.split, added FileSchmStats and FileSchmQm parameters
##############################################################################################
library(foreach)
library(doParallel)
library(magrittr)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.concH2oSalinity.grp.split.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)) {
  numCoreUse <- 1
}
if (numCoreUse > numCoreAvail) {
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ', numCoreAvail, ' available cores will be used for internal parallelization.'))

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
      "FileSchmStats",
      "FileSchmQm",
      "DirSubCopy"
    ),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for stats output: ', Para$FileSchmStats))
log$debug(base::paste0('Schema for quality_metrics output: ', Para$FileSchmQm))

# Read schemas if provided
if (base::is.null(Para$FileSchmStats) || Para$FileSchmStats == 'NA') {
  SchmStats <- NULL
} else {
  SchmStats <- base::paste0(base::readLines(Para$FileSchmStats), collapse = '')
}

if (base::is.null(Para$FileSchmQm) || Para$FileSchmQm == 'NA') {
  SchmQm <- NULL
} else {
  SchmQm <- base::paste0(base::readLines(Para$FileSchmQm), collapse = '')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input datum path
nameDirSub <- c('stats', 'quality_metrics')
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all input datum paths (CFGLOC directories containing stats and quality_metrics)
DirIn <-
  NEONprocIS.base::def.dir.in(
    DirBgn = Para$DirIn,
    nameDirSub = nameDirSub,
    log = log
  )

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))

  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.concH2oSalinity.grp.split(
        DirIn = idxDirIn,
        DirOutBase = Para$DirOut,
        SchmStats = SchmStats,
        SchmQm = SchmQm,
        DirSubCopy = DirSubCopy,
        log = log
      ),
      error = function(err) {
        call.stack <- base::sys.calls()

        # Re-route the failed datum
        NEONprocIS.base::def.err.datm(
          err = err,
          call.stack = call.stack,
          DirDatm = idxDirIn,
          DirErrBase = Para$DirErr,
          RmvDatmOut = TRUE,
          DirOutBase = Para$DirOut,
          log = log
        )
      }
    ),
    error = function(err) {
      # This outer tryCatch is intentionally empty since errors were already routed above
    }
  )
}
