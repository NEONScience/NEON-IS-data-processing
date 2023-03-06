##############################################################################################
#' @title Create publication tables and apply relevant science review flags for IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Create publication tables and apply relevant science review flags for
#' IS data prod products.  
#'
#' General code workflow:
#'    Parse input parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in the data files, publication workbook(s) and science review flags for each input datum
#'      Create each publication table 
#'      Apply relevant science review flags and associated forcing actions to each publication table
#'      Write out the data
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs', 'group', the same name
#' as any of the terminal directories indicated in argument \code{DirData}, or recognizable as the 'yyyy/mm/dd'
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained
#' in the folder.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01
#' 
#' The \code{DirIn} argument serves as the starting directory to search for datums to process. In this 
#' module each datum is a group ID, and will be identified by the terminal directories expected for each 
#' group. The repository is expected to be in 'consolidated-group' structure, in which the data for each 
#' group is nested directory under the GROUP_ID directory:
#' .../GROUP_ID/DATA_DIR
#' where DATA_DIR includes, at a minimum, the 'group' folder which contains metadata about the group and 
#' all the directory names indicated in input argument \code{DirData}. Any science review flags (SRF) are expected
#' to be in the folder 'science_review_flags'. The SRF folder is not required.
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of \code{DirIn}.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of \code{DirIn}.
#' 
#' 4. "DirData=value", where value is the name(s) of the terminal directories, separated by pipes,
#' where the data to form into publication tables resides. These directories should be direct children
#' of the GROUP_ID directory. The files in these directories will be searched for matching fields in the
#' publication tables. For example, "DirData=stats|quality_metrics" indicates that the stats and 
#' quality_metrics folders are expected child directories of GROUP_ID and contain data relevant to the 
#' publication tables. All specified directories must be present in order to identify the group as a datum.
#' 
#' 5. "PathPubWb=value", where value is the relative or absolute path(s) to publication workbooks, separated by
#' pipes. These paths may be individual files or a parent directory of the publication workbooks, in which case
#' all publication workbooks recursively found in the directory will be used. The publication workbooks are csv 
#' files that define the publication tables to produce. Note that there should be no ambiguity in term names among 
#' files in the DirData directories and in the pub workbooks at the same timing index. The same term name should 
#' refer to the same data in both.  
#' 
#' 6. "TablPub=value" (optional), where value is the names of table(s) in the pub workbook(s) to produce 
#' (denoted in the 'table' column of the pub workbook). Separate multiple tables with pipes, 
#' e.g. "TablPub=SCGW_30_minute|SCGW_5_minute". By default (if this argument is not input) all tables in the 
#' publication workbooks with a discernible timing index are produced. If only the data files contain no matches
#' with the fields in the pub workbook, or only match on the start/end times, the table will not be produced and a 
#' warning will be issue.
#' 
#' 7. "NameVarTimeBgn=value" (optional), where value is the name of the time variable expected in every timeseries 
#' file indicating the start time of the aggregation interval. Default is 'startDateTime'.
#' 
#' 7. "NameVarTimeEnd=value" (optional), where value is the name of the time variable expected in every timeseries file
#' indicating the end time of the aggregation interval. If the data are instantaneous output, set NameVarTimeEnd to the 
#' same variable as NameVarTimeBgn. If any part of the aggregation interval falls within the time range of a SRF, 
#' the SRF will be applied. Note that the aggregation end time and SRF end time are exclusive, meaning they are not considered 
#' part of the interval. Default is 'endDateTime'. 
#'
#' 10. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the DirData folders in the that are to be copied with a
#' symbolic link to the output path. May overlap with the output 'data' directory. Default is 'group'.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository with the resultant publication tables in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. Pub tables will be placed in the 'data' directory
#' nested under each GROUP_ID. The ordering of the columns in each pub table will follow the order found in the pub workbook(s).
#' Note that any order specified in the Rank column of the pub workbooks is ignored.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not Run - uses all available defaults
#' Rscript flow.pub.tabl.R "DirIn=/scratch/pfs/groundwaterPhysical_level1_consolidate" "DirOut=/scratch/pfs/out" "DirErr=/scratch/pfs/out/errored_datums" "DirData=stats|quality_metrics|uncertainty_data" "PathPubWb=/scratch/pfs/pub_workbooks" 
#'
#'  Not Run - Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='/scratch/pfs/groundwaterPhysical_level1_consolidate')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "DirErr=/scratch/pfs/out/errored_datums", "DirData=stats|quality_metrics", "PathPubWb=/scratch/pfs/pub_workbooks" )
#' # Then copy and paste rest of workflow into the command window

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-23)
#     original creation
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.pub.tabl.srf.R")

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
    NameParaReqd = c("DirIn", 
                     "DirOut", 
                     "DirErr",
                     "DirData", 
                     "PathPubWb" 
                     ),
    NameParaOptn = c("TablPub",
                     "NameVarTimeBgn",
                     "NameVarTimeEnd",
                     "DirSubCopy"
                     ),
    ValuParaOptn = base::list(
      TablPub = NULL,
      NameVarTimeBgn = 'startDateTime',
      NameVarTimeEnd = 'endDateTime',
      DirSubCopy = 'group'
    ),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(
  base::paste0(
    'Source data for the publication tables will be searched for in the following terminal directories: ',
    base::paste0(Para$DirData, collapse = ',')
  )
)
FilePubWb <- base::list.files(Para$PathPubWb,full.names = TRUE,recursive=TRUE)
log$debug(
  base::paste0(
    'Publication workbooks: ',
    base::paste0(FilePubWb, collapse = ',')
  )
)
log$debug(base::paste0('Publication tables selected (empty will attempt to create them all): ', Para$TablPub))
log$debug(base::paste0('Time variable in the data indicating start time of aggregation interval: ', Para$NameVarTimeBgn))
log$debug(base::paste0('Time variable in the data indicating end time of aggregation interval: ', Para$NameVarTimeEnd))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- c('group',Para$DirData)
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
      wrap.pub.tabl(
        DirIn=idxDirIn,
        DirOutBase=Para$DirOut,
        DirData=Para$DirData,
        FilePubWb=FilePubWb,
        TablPub=Para$TablPub,
        NameVarTimeBgn=Para$NameVarTimeBgn,
        NameVarTimeEnd=Para$NameVarTimeEnd,
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
