##############################################################################################
#' @title Quality metrics module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Quality metrics module for NEON IS data processing. Aggregates quality flags
#' at specified time intervals and computes pass, fail, and na quality metrics in addition to alpha 
#' and beta summary metrics and the final quality flag.
#' 
#' This script is run at the command line with the following arguments. Each argument must be a string  
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input datum (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are (at a minimum) the folder:
#'         /flags 
#'         
#' The flags folder holds any number of daily files holding quality flags. All files will be combined. Ensure
#' there are no overlapping column names between the files other than "readout_time", otherwise only one of the 
#' columns will be retained. Note that the "readout_time" variable must exist in all files. Any non-matching 
#' timestamps among files will result in NA values for columns that do not have this timestamp.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileSchmQm=value" (optional), where value is the full path to the avro schema for the output quality 
#' metrics file. If this input is not provided, the output schema for the stats will be auto-generated from  
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS 
#' IN THE SORTED FILE LIST. See output information below for details. 
#' 
#' 5. "WndwAgr=value", where value is the aggregation interval for which to compute statistics. It is formatted 
#' as a 3 character sequence, typically representing the number of minutes over which to compute statistics. 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must be an equal divisor of 
#' one day.
#' 
#' 6. "Thsh=value" (optional), where value is the threshold fraction for the sum of the alpha and beta quality 
#' metrics multiplied by the respective weights given in argument WghtAlphaBeta (below) at and above 
#' which triggers the final quality flag (value of 1). Default value = 0.2.
#' 
#' 7. "WghtAlphBeta=value" (optional), where value is a 2-element vector of weights, separated by pipes (|) for 
#' the alpha and beta quality metrics, respectively. The alpha and beta quality metrics (in fraction form) are 
#' multiplied by these respective weights and summed. If the resultant value is greater than the threshold value 
#' set in the Thsh argument, the final quality flag is raised. Default is "WghtAlphaBeta=2|1".
#' 
#' 8-N. "GrpQfX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of  
#' the quality flags that should be used in the computation of alpha & beta quality metrics and the final quality 
#' flag. Begin each argument with the group name (e.g. temp) to be used as a prefix to the output alpha and beta
#' QMs and final quality flag, followed by a colon (:), and then the exact names of the quality flags, delimited
#' by pipes (|). For example, if tempRangeQF and tempPersistenceQF are feed into the final quality flag, and you 
#' want "temp" to be a prefix for resultant alpha/beta QMs and finalQF, the argument is 
#' "GrpQf1=temp:tempRangeQF|tempPersistenceQF". If no prefix to the outputs is desired, include the colon as the
#' first character of value (e.g. "GrpQf1=:tempRangeQF|tempPersistenceQF"). If this argument is not included, all 
#' quality flags in the file(s) will be used to compute a single set of alpha and beta quality metrics and the 
#' final quality flag. 
#' Note that quality metrics for all quality flags found in the input files will be output, the GrpQfX arguments 
#' simply dictate what QMs feed into a set of alpha&beta quality metrics and the final quality flag. There may be 
#' multiple assignments of GrpQfX, specified by incrementing the number X by 1 with each additional argument. 
#' There is a limit of X=100 for GrpQfX arguments. Note that the group names must be unique among GrpQfX arguments.
#'  
#' N+1. "VarIgnr=value" (optional), where value contains the names of the variables that should be ignored if 
#' found in the input files, separated by pipes (|) (e.g. "VarIgnr=timeWndwBgn|timeWndwEnd"). Do not include 
#' readout_time here. No quality metrics will be computed for these variables and they will not be included 
#' in the output. Defaults to empty. 
#' 
#' N+2. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the flags folder in the input path that are to be copied with a 
#' symbolic link to the output path. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Quality metrics for each aggregation interval output in Parquet format in DirOut, where the terminal 
#' directory of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input 
#' path. Directory 'quality_metrics' will automatically populated in the output directory, where the files 
#' for each aggregation interval will be placed. Any other folders specified in argument DirSubCopy will be 
#' copied over unmodified with a symbolic link.
#' 
#' If no output schema is provided for the quality metrics, the variable names for quality metrics will be a combination 
#' of the quality flag and PassQM, FailQM, or NAQM. The variable names for the alpha & beta quality metrics and final 
#' quality flag will be a combination of the group name (including none) and alphaQM, betaQM, or FinalQF. The order of 
#' the outputs will be all quality metrics in the same order they were found in the sorted (increasing order) input
#' files, with nested Pass, Fail, and NA QMs for each flag. These will be followed by the alpha and beta QMs and final 
#' quality flag groups in the same order as the GrpQfX inputs. Additionally, the first two columns of the output file 
#' will contain the start and end times for the aggregation interval, labeled "startDateTime" and "endDateTime", 
#' respectively. The quality metrics are calculated for readout times in the interval [startDateTime endDateTime), with 
#' an open brack on the right (i.e. inclusive of the startDateTime but exclusive of the endDateTime). An example column
#' ordering: Say there are two input files named outflagsA.parquet and outflagsB.parquet, where outflagsA.parquet contains flag 
#' tempValidCalQF and outflagsB.parquet contains flags tempRangeQF, and the grouping input argument is 
#' "GrpQf1=temp:tempRangeQf|tempValidCalQF". The ordering of the output columns will be startDateTime, endDateTime, 
#' tempValidCalQFPassQM, tempValidCalQFFailQM, tempValidCalQFNAQM, tempRangeQFPassQM, tempRangeQFFailQM, tempRangeQFNAQM,  
#' tempAlphaQM, tempBetaQM, and tempFinalQF, in that order. The names of the output columns may be replaced by providing an 
#' output schema in argument FileSchmQm. However, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA MATCHES THIS COLUMN ORDERING. 
#' Otherwise, column names will not pertain to the metrics in the column.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in 
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr 

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.qm.R "DirIn=/pfs/proc_group/prt/2019/01/01/CFGLOC112083" "DirOut=/pfs/out" "FileSchmQm=/outputQmSchema.avsc" "WndwAgr=001|030" "GrpQf1=temp:rangeQF|stepQF"

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-10-16)
#     original creation 
#   Cove Sturtevant (2020-04-23)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2021-02-04)
#     added option to ignore particular variables in the input files
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
#   Cove Sturtevant (2021-10-25)
#     Move main functionality to wrapper function
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.qaqc.qm.R")

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
                                      NameParaReqd=c(
                                        "DirIn",
                                        "DirOut",
                                        "DirErr", 
                                        "WndwAgr"
                                        ),
                                      NameParaOptn=c(
                                        "FileSchmQm",
                                        base::paste0("GrpQf",1:100),
                                        "DirSubCopy",
                                        "Thsh",
                                        "WghtAlphBeta",
                                        "VarIgnr"
                                        ), 
                                      ValuParaOptn=base::list(
                                        Thsh=0.2,
                                        WghtAlphBeta=c(2,1)
                                        ),
                                      TypePara=base::list(
                                        Thsh="numeric",
                                        WghtAlphBeta="numeric"
                                        ),
                                      log=log)

# Echo arguments
log$debug(base::paste0('Input directory: ',Para$DirIn))
log$debug(base::paste0('Output directory: ',Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve output schema for quality metrics
FileSchmQm <- Para$FileSchmQm
log$debug(base::paste0('Output schema for quality metrics: ',base::paste0(FileSchmQm,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmQm) || FileSchmQm == 'NA'){
  SchmQm <- NULL
} else {
  SchmQm <- base::paste0(base::readLines(FileSchmQm),collapse='')
}

# Retrieve aggregation intervals
WndwAgr <- base::as.difftime(base::as.numeric(Para$WndwAgr),units="mins") 
log$debug(base::paste0('Aggregation interval(s), in minutes: ',base::paste0(WndwAgr,collapse=',')))

# Retrieve alpha and beta QM thresholds
log$debug(base::paste0('Threshold fraction for triggering final quality flag: ',Para$Thsh))
log$debug(base::paste0('Respective weights applied to alpha and beta quality metrics in threshold evaluation: ',
                       base::paste0(Para$WghtAlphBeta,collapse=',')))

# Parse the groups of flags feeding into alpha and beta quality metrics and the final quality flag 
nameParaGrpQf <- base::names(Para)[names(Para) %in% base::paste0("GrpQf",1:100)]
if(base::length(nameParaGrpQf) > 0){
  spltGrp <- Para[nameParaGrpQf]
  ParaGrp <- base::lapply(spltGrp,FUN=function(argSplt){
    base::list(name=argSplt[1],
               qf=utils::tail(x=argSplt,n=-1))
  })
  for(idxGrp in base::names(ParaGrp)){
    log$debug(base::paste0('Alpha/beta QMs and finalQF will be computed for group name: ',ParaGrp[[idxGrp]]$name, 
                           ' with flags: ',base::paste0(ParaGrp[[idxGrp]]$qf,collapse=',')))
  }
  
} else {
  ParaGrp <- NULL
  log$debug('Alpha/beta QMs and finalQF will be computed with all flags found in the input file(s).')
  
}

# Retrieve variables to ignore in the flags files
VarIgnr <- setdiff(Para$VarIgnr,'readout_time')
log$debug(base::paste0('Variables to ingnore if found in input files: ',base::paste0(VarIgnr,collapse=',')))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'quality_metrics'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,'flags')))
log$debug(base::paste0('Minimum expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.qaqc.qm(DirIn=idxDirIn,
                   DirOutBase=Para$DirOut,
                   WndwAgr=WndwAgr,
                   Thsh=Para$Thsh,
                   WghtAlphBeta=Para$WghtAlphBeta,
                   ParaGrp=ParaGrp,
                   VarIgnr=VarIgnr,
                   SchmQm=SchmQm,
                   DirSubCopy=DirSubCopy,
                   log=log
      ),
      error = function(err) {
        call.stack <- sys.calls() # is like a traceback within "withCallingHandlers"
        log$error(base::paste0('The following error has occurred (call stack to follow): ',err))
        print(utils::limitedLabels(call.stack))
      }
    ),
    error=function(err) {
      NEONprocIS.base::def.err.datm(
        DirDatm=idxDirIn,
        DirErrBase=Para$DirErr,
        RmvDatmOut=TRUE,
        DirOutBase=Para$DirOut,
        log=log
      )
    }
  )
  
  return()
} # End loop around datum paths 
  