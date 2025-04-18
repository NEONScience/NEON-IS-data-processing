##############################################################################################
#' @title Basic QA/QC (plausibility) module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Basic QA/QC (plausibility) module for NEON IS data processing. Includes tests for 
#' null, gap, range, step, spike, and persistence. See eddy4R.qaqc package functions for details on each test.
#' 
#' This script is run at the command line with 5+ arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /data 
#'         /threshold
#'         
#' The data folder holds any number of daily data files corresponding to the date in the input 
#' path and surrounding days. Names of data files MUST include the data date in the format %Y-%m-%d 
#' (YYYY-mm-dd). It does not matter where in the filename the date is denoted, so long as it is unambiguous.
#' 
#' The threshold folder holds a single file with QA/QC threshold information. 
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4.  "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 5. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details. 
#' 
#' 6-N. "TermTestX=value", where X is a number beginning at 1 and value contains the (exact) names of the tests 
#' to be run for each term/variable. Begin each argument with the term name (e.g. temp), followed by a colon (:),
#' and then the tests to be run, delimited by pipes (|).  Test options are: null, gap, range, step, spike, 
#' persistence. Each of the test names can be appended with "(rmv)" to indicate that failures of the test 
#' remove the affected data point(s) (i.e. turn them to NA). Make sure there are no spaces between the term 
#' name, colon, test names and the optional (rmv) argument, nor between pipes. For example, 
#' "TermTest1=temp:range(rmv)|step|persistence" indicates that the range, step, and persistence tests are to be 
#' run for the term/variable temp. Failures of the range test will turn data values that fail this test to NA, 
#' while the step and persistence tests will retain the data values for which the tests fail (although still 
#' flagging them). There may be multiple assignments of TermTestX, specified by incrementing the 
#' number X by 1 with each additional argument. TermTest1 must be an input, and there is
#' a limit of X=100 for additional TermTestX arguments. 
#' 
#' N+1. "VarAddFileQf=value" (optional), where value contains the names of any variables in the input data file,
#' separated by pipes (|) that should be copied over to the output flags files. Do not include readout_time.
#' In normal circumstances there should be none, as flags files should only contain a timestamp and flags, 
#' but in rare cases additional variables may desired to be included in the flags files (such as source ID, site, 
#' or additional timing information produced from upstream modules). Defaults to empty. Note that these will be 
#' tacked on to the end of the output columns produced by the selections in TermTestX, and any output schema 
#' should account for this.
#' 
#' N+2. "DirSubCopy=value" (optional), where value is the names of additional subfolders (excluding 'data'), 
#' separated by pipes, at the same level as the data & threshold folders in the input path that are to be 
#' copied with symbolic links to the output path. Be sure to specify 'DirSubCopy=threshold" if you want to 
#' retain it in the output directory structure. Note that it is acceptable to include the
#' "flags" directory if flags files generated from other processing modules (differently named) are to be 
#' passed through. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. The data and flags folders will include only the 
#' data/flags for the date indicated in the directory structure. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link.
#' 
#' The flags file will contain a column for the readout time followed by columns for quality flags grouped by 
#' variable/term in the same order as the variables/terms and the tests were provided in the input arguments 
#' (test nested within term), followed by additional variables, if any, specified in argument VarAddFileQf.
#'  
#' If no output schema is provided for the flags, the variable names will be a camelCase combination of the term, 
#' the test, and the characters "QF", in that order. For example, if the input arguments 5-6 are 
#' "TermTest1=temp:null|range(rmv)" and "TermTest1=resistance:spike|gap" and the argument VarAddFileQf is omitted,
#' the output columns will be readout_time, tempNullQF, tempRangeQF, resistanceSpikeQF, resistanceGapQF, in that order. 
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS. 
#' Otherwise, they will be labeled incorrectly.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.plau.R "DirIn=/pfs/proc_group/prt/2019/01/01/CFGLOC112083" "DirOut=/pfs/out" "DirOut=/pfs/out/errored" "FileSchmData=/outputDataSchema.avsc" "FileSchmQf=/outputQfSchema.avsc" "TermTest1=temp:null|gap|range(rmv)|step(rmv)|spike(rmv)|persistence"

#' @seealso 
#' \code{\link[eddy4R.qaqc]{def.plau}} \cr
#' \code{\link[eddy4R.qaqc]{def.dspk.wndw}} \cr

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-08-27)
#     original creation 
#   Cove Sturtevant (2019-10-01)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2019-10-02)
#     made inputs FileSchmData and FileSchmQf optional
#     fix bug returning all padded data
#   Cove Sturtevant (2019-11-27)
#     fix bug when only one test selected
#   Cove Sturtevant (2020-04-22)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2020-07-08)
#     Adjust code to accommodate new (faster!) despike algorithm and efficient plausibility code
#   Cove Sturtevant (2020-10-09)
#     Add check that terms slated for QA/QC are in the data
#   Cove Sturtevant (2021-02-04)
#     Add option to copy one or more variables found in the input file to the output flags file
#   Cove Sturtevant (2021-01-20)
#     Applied internal parallelization
#   Cove Sturtevant (2021-07-02)
#     Fix bug resulting in error when terms have different number of tests
#   Cove Sturtevant (2021-10-11)
#     Move main functionality to wrapper function and add error routing
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.qaqc.plau.R")

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
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c(
      "DirIn", 
      "DirOut",
      "DirErr", 
      "TermTest1"
      ),
    NameParaOptn = c(
      "FileSchmData",
      "FileSchmQf",
      base::paste0("TermTest", 2:100),
      "VarAddFileQf",
      "DirSubCopy"
    ),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve output schema for data
FileSchmDataOut <- Para$FileSchmData
log$debug(base::paste0('Output schema for data: ',base::paste0(FileSchmDataOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}

# Retrieve output schema for flags
FileSchmQfOut <- Para$FileSchmQf
log$debug(base::paste0('Output schema for flags: ',base::paste0(FileSchmQfOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmQfOut) || FileSchmQfOut == 'NA'){
  SchmQf <- NULL
} else {
  SchmQf <- base::paste0(base::readLines(FileSchmQfOut),collapse='')
}

# Parse the terms and associated QA/QC tests to run, and whether test failures result in data removal 
nameParaTermTest <- base::names(Para)[names(Para) %in% base::paste0("TermTest",1:100)]
numTermTest<- base::length(nameParaTermTest)

spltTest <- Para[nameParaTermTest]
ParaTest <- base::lapply(spltTest,FUN=function(argSplt){
  base::list(term=argSplt[1],
             test=base::sub(pattern='(rmv)',replacement='',x=utils::tail(x=argSplt,n=-1),fixed=TRUE),
             rmv=base::grepl(pattern='(rmv)',x=utils::tail(x=argSplt,n=-1),fixed=TRUE))
})
termTest <- base::unlist(base::lapply(ParaTest,FUN=function(idx){idx$term})) # Pull out the terms to test
names(ParaTest) <- termTest

# Retrieve variables to ignore in the flags files
VarAddFileQf <- setdiff(Para$VarAddFileQf,'readout_time')
log$debug(base::paste0('Variables from input files to be added to the output flags files: ',
                       base::paste0(VarAddFileQf,collapse=',')))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'data'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('data','threshold'))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing datum path: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.qaqc.plau(DirIn=idxDirIn,
                     DirOutBase=Para$DirOut,
                     ParaTest=ParaTest,
                     SchmDataOut=SchmDataOut,
                     SchmQf=SchmQf,
                     VarAddFileQf=VarAddFileQf,
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



