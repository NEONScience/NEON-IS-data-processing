##############################################################################################
#' @title Basic statistics and uncertainty module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Basic statistics module for NEON IS data processing. Computes one or more
#' of the following statistics: mean, median, minimum, maximum, sum, variance, standard 
#' deviation, standard error, number of points, skewness, kurtosis, median absolute deviation (mad), 
#' expanded uncertainty
#' 
#' This script is run at the command line with 4+ arguments. Each argument must be a string in 
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
#' Nested within this path are (at a minimum) the folder:
#'         /data 
#'         
#' The data folder holds any number of daily data files for which statistics will be computed. If expUncert
#' is output (see options in TermStatX), information in folders 'uncertainty_coef' and/or 'uncertainty_data' 
#' will be passed into the specified uncertianty function. 
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileSchmStat=value" (optional), where value is the full path to the avro schema for the output stats 
#' file. If this input is not provided, the output schema for the stats will be auto-generated from the output 
#' data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE TERMS 
#' IN THE TermStatX ARGUMENT (stats nested within term/variable). See output information below for details. 
#' 
#' 5. "WndwAgr=value", where value is the aggregation interval for which to compute statistics. It is formatted 
#' as a 3 character sequence, typically representing the number of minutes over which to compute statistics. 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals.
#' 
#' 6-N. "TermStatX=value", where X is a number beginning at 1 and value contains the (exact) names of the stats 
#' to be generated for each term/variable. Begin each argument with the term name (e.g. temp), followed by a 
#' colon (:), and then the stats to compute, delimited by pipes (|).  Statistic options are (exact names): 
#' mean, median, minimum, maximum, sum, variance, stdDev, stdEr, numPts, expUncert, skewness, kurtosis, mad. 
#' For example, to compute the mean, minimum, and maximum for term "temp", the argument is 
#' "TermStat1=temp:mean|minimum|maximum". For expUncert, the name of the function in the NEONprocIS.stat package 
#' to compute the expanded uncertainty is included in parentheses immediately after expUncert. Adding expUncert to 
#' the previous example: "TermStat1=temp:mean|minimum|maximum|expUncert(wrap.ucrt.dp01.cal.cnst.fdas.rstc)". 
#' Here, wrap.ucrt.dp01.cal.cnst.fdas.rstc is the name of the function in the NEONprocIS.stat package that will compute the 
#' expanded uncertainty for temp. Look in the NEONprocIS.stat package for available functions to use, or create your own so 
#' long as it accepts the same inputs and outputs data in the same format. Note that any uncertainty coefficients and/or 
#' L0' uncertainty data in the uncertainty_coef and uncertainty_data folders, respectively, will be passed into the 
#' uncertainty function for use there. 
#' There may be multiple assignments of TermStatX, specified by incrementing the number X by 1 with each additional  
#' argument. TermStat1 must be an input, and there is a limit of X=100 for additional TermStatX arguments. Note that 
#' the order that the terms and statistics are given here is the column order in which they will be output (statistic
#' nested within term, see below). All terms must correspond to numeric data.
#'  
#' N+1. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the data folder in the input path that are to be copied with a 
#' symbolic link to the output path. Note that it is acceptable to include the
#' "stats" directory if stats files generated from other processing modules (differently named) are to be 
#' passed through. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Statistics for each aggregation interval output in parquet format in DirOut, where the terminal 
#' directory of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input 
#' path. Directory 'stats' will automatically populated in the output directory, where the files 
#' for each aggregation interval will be placed. Any other folders specified in argument DirSubCopy will be 
#' copied over unmodified with a symbolic link.
#' 
#' If no output schema is provided for the statistics, the variable names will be a combination of 
#' the term and statistic, in that order. Additionally, the first two columns of the stats file will contain 
#' the start and end times for the aggregation interval, labeled "startDateTime" and "endDateTime", respectively. 
#' The statistics are calculated for readout times in the interval [startDateTime endDateTime), with an open 
#' brack on the right (i.e. inclusive of the startDateTime but exclusive of the endDateTime). The remaining 
#' columns present the chosen statistics for each term. They are ordered in the same order as the TermStatX input 
#' arguments. For example, if the input arguments are "TermStat1=temp:mean|minimum|maximum" and 
#' "TermStat2=precip:sum", the output columns will be startDateTime, endDateTime, tempMean, tempMinimum, 
#' tempMaximum, precipSum, in that order. The names of the output columns may be replaced by providing an output 
#' schema in argument FileSchmStat. However, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE 
#' ORDERING OF THE INPUT ARGUMENTS. Otherwise, column names will not pertain to the statistics in the column.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.stat.basc.R "DirIn=/pfs/proc_group/prt/2019/01/01/CFGLOC112083" "DirOut=/pfs/out" "FileSchmStat=/outputStatSchema.avsc" "WndwAgr=001|030" "TermStat1=temp:mean|minimum|maximum|expUncert(R)|skewness|kurtosis"

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-10-09)
#     original creation 
#   Cove Sturtevant (2019-10-23)
#     added uncertainty calculation
#   Cove Sturtevant (2019-10-30)
#     added fdas uncertainty calculation
#   Cove Sturtevant (2019-11-07)
#     sync up missing data values between data and fdas uncertainty
#   Cove Sturtevant (2020-02-17)
#     adjust reading of fdas uncertainty data to look within generic uncertainty data file
#   Cove Sturtevant (2020-04-23)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2020-09-15)
#     adjust code to explicitly specify if FDAS uncertainty applies, and which (Resistance or Voltage)
#   Robert Lee (2020-10-07)
#     Add skewness and kurtosis compuations
#   Cove Sturtevant (2020-10-28)
#     bug fixes
#     Re-organize structure to loop around windows first, then stats (previously vice versa)
#     Add MAD computation
#     Pull out expanded uncertainty computation into exchangeable function, with options in main code to specify function used
#   Cove Sturtevant (2021-01-20)
#     Applied internal parallelization
#     bug fix for error when no uncertainty stats are selected for output
#   Cove Sturtevant (2022-02-10)
#     allow existing files in stats folder to be passed through
#   Cove Sturtevant (2022-06-15)
#     Move main functionality to wrapper function and add error routing
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.stat.basc.R")

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
                                      NameParaReqd=c("DirIn",
                                                     "DirOut",
                                                     "DirErr", 
                                                     "WndwAgr",
                                                     "TermStat1"
                                                     ),
                                      NameParaOptn=c("FileSchmStat",
                                                     base::paste0("TermStat",2:100),
                                                     "DirSubCopy"
                                                     ),
                                      log=log)

# Echo arguments
log$debug(base::paste0('Input directory: ',Para$DirIn))
log$debug(base::paste0('Output directory: ',Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Retrieve output schema for data
FileSchmStat <- Para$FileSchmStat
log$debug(base::paste0('Output schema for statistics: ',base::paste0(FileSchmStat,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmStat) || FileSchmStat == 'NA'){
  SchmStat <- NULL
} else {
  SchmStat <- base::paste0(base::readLines(FileSchmStat),collapse='')
}

# Retrieve aggregation intervals
WndwAgr <- base::as.difftime(base::as.numeric(Para$WndwAgr),units="mins") 
log$debug(base::paste0('Aggregation interval(s), in minutes: ',base::paste0(WndwAgr,collapse=',')))

# Parse the terms and associated statistics to compute 
nameParaTermStat <- base::names(Para)[names(Para) %in% base::paste0("TermStat",1:100)]
spltStat <- Para[nameParaTermStat]
ParaStat <- base::lapply(spltStat,
                         FUN=function(argSplt){
                           term <- argSplt[1]
                           stat <- utils::tail(x=argSplt,n=-1)
                           funcUcrt <- NULL
                           statUcrt <- base::substr(stat,1,9) %in% "expUncert"
                            if(base::sum(statUcrt) > 1){
                              # Error if multiple
                              log$fatal(base::paste0('Multiple expUncert stats for term ',
                                                     term,
                                                     '. This is not allowed.'))
                              stop()
                            } else if (base::sum(statUcrt) == 1){
                              # Parse out the function to use
                              if (base::grepl(pattern = 'expUncert\\([a-zA-Z_.0-9]+\\)', 
                                              x = stat[statUcrt])) {
                                
                                infoFuncUcrt <- gregexpr(pattern = '\\([a-zA-Z_.0-9]+\\)', 
                                                         text = stat[statUcrt])[[1]]
                                
                                funcUcrt <- base::substr(x=stat[statUcrt],
                                                         start=infoFuncUcrt+1,
                                                         stop=infoFuncUcrt+base::attr(infoFuncUcrt,'match.length')-2)
                                stat[statUcrt] <- 'expUncert'
                          
                              } else {
                                # Function not specified. Error.
                                log$fatal(base::paste0('Uncertainty function for ', 
                                                       term, 
                                                       ' must be specified in parentheses immediately after "expUncert", e.g. "expUncert(funcUcrt)".'))
                                stop()
                              } 
                            }
                            base::list(term=term,
                                       stat=stat,
                                       funcUcrt=funcUcrt)
                            })
termComp <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$term})) # Pull out a list of all the terms we are going to use 
names(ParaStat) <- termComp 

# Error check the chosen statistics
stat <- base::unique(base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$stat})))
StatAvail <- c("mean", "median", "minimum", "maximum", "sum", "variance", "stdDev", "stdEr", "numPts", "skewness", "kurtosis", "mad", "expUncert")
chkStat <- stat %in% StatAvail
if(base::sum(!chkStat) > 0){
  log$fatal(base::paste0('Statistic(s): ',
                         base::paste0(stat[!chkStat],collapse=","), 
                         ' are unrecognized for computation by the basic stats module. Acceptable statistic choices are ',
                         base::paste0(StatAvail,collapse=",")))
  stop()
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('data'))
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
      wrap.stat.basc(DirIn=idxDirIn,
                     DirOutBase=Para$DirOut,
                     WndwAgr=WndwAgr,
                     ParaStat=ParaStat,
                     SchmStat=SchmStat,
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

