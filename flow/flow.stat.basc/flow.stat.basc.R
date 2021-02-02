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
#' 3. "FileSchmStat=value" (optional), where value is the full path to the avro schema for the output stats 
#' file. If this input is not provided, the output schema for the stats will be auto-generated from the output 
#' data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE TERMS 
#' IN THE TermStatX ARGUMENT (stats nested within term/variable). See output information below for details. 
#' 
#' 4. "WndwAgr=value", where value is the aggregation interval for which to compute statistics. It is formatted 
#' as a 3 character sequence, typically representing the number of minutes over which to compute statistics. 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals.
#' 
#' 5-N. "TermStatX=value", where X is a number beginning at 1 and value contains the (exact) names of the stats 
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
#' symbolic link to the output path. 
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
#     Pull out expanded uncertainty computation into exchangable function, with options in main code to specify function used
#   Cove Sturtevant (2021-01-20)
#     Applied internal parallelization
#     bug fix for error when no uncertainty stats are selected for output
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","WndwAgr","TermStat1"),
                                      NameParaOptn=c("FileSchmStat",base::paste0("TermStat",2:100),
                                                     "DirSubCopy"),log=log)

# Retrieve datum path. 
log$debug(base::paste0('Input directory: ',Para$DirIn))

# Retrieve base output path
log$debug(base::paste0('Output directory: ',Para$DirOut))

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
ParaStat <- base::lapply(spltStat,FUN=function(argSplt){
  term <- argSplt[1]
  stat <- utils::tail(x=argSplt,n=-1)
  funcUcrt <- NULL
  statUcrt <- base::substr(stat,1,9) %in% "expUncert"
  if(base::sum(statUcrt) > 1){
    # Error if multiple
    log$fatal(base::paste0('Multiple expUncert stats for term ', term, '. This is not allowed.'))
    stop()
  } else if (base::sum(statUcrt) == 1){
    # Parse out the function to use
    if (base::grepl(pattern = 'expUncert\\([a-zA-Z_.0-9]+\\)', x = stat[statUcrt])) {
      
      infoFuncUcrt <- gregexpr(pattern = '\\([a-zA-Z_.0-9]+\\)', text = stat[statUcrt])[[1]]
      
      funcUcrt <- base::substr(x=stat[statUcrt],start=infoFuncUcrt+1,stop=infoFuncUcrt+base::attr(infoFuncUcrt,'match.length')-2)
      stat[statUcrt] <- 'expUncert'

    } else {
      # Function not specified. Error.
      log$fatal(base::paste0('Uncertainty function for ', term, ' must be specified in parentheses immediately after "expUncert", e.g. "expUncert(funcUcrt)".'))
      stop()
    } 
  }
  base::list(term=term,
             stat=stat,
             funcUcrt=funcUcrt,
             nameTermStat=base::paste0(term,base::paste(base::toupper(base::substr(stat,1,1)),base::substr(stat,2,base::nchar(stat)),sep="")))
})
termComp <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$term})) # Pull out a list of all the terms we are going to use 
names(ParaStat) <- termComp 
FuncUcrt <- base::do.call(base::rbind,base::lapply(ParaStat,FUN=function(idx){if(!base::is.null(idx$funcUcrt)){return(base::data.frame(var=idx$term,FuncUcrt=idx$funcUcrt,stringsAsFactors=FALSE))}else{return(NULL)}})) # Terms functions for uncertainty calcs

# Compile the stats we are going to compute
stat <- base::unique(base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$stat})))

# Compile the full set of TermStat combinations, in the order in which they will be output
nameTermStat <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$nameTermStat}))

# Error check the statistic names
nameStat <- c("mean", "median", "minimum", "maximum", "sum", "variance", "stdDev", "stdEr", "numPts", "skewness", "kurtosis", "mad", "expUncert")
chkStat <- stat %in% nameStat
if(base::sum(!chkStat) > 0){
  log$fatal(base::paste0('Statistic(s): ',base::paste0(stat[!chkStat],collapse=","), ' are unrecognized for computation by this module. Acceptable statistic choices are ',base::paste0(nameStat,collapse=",")))
  stop()
}

# Reformat the parameters to be stat-focused rather than term-focused (i.e. a named list of terms to compute each stat)
statTerm <- lapply(stat,FUN=function(idxStat){
  base::unlist(lapply(ParaStat,FUN=function(idxTerm){
    if(idxStat %in% idxTerm$stat){
      return(idxTerm$term)
    }
  }))
})
names(statTerm) <- stat

# Pull the column names for statTerm
nameStatTerm <- lapply(stat,FUN=function(idxStat){
  base::unlist(lapply(ParaStat,FUN=function(idxTerm){
    if(idxStat %in% idxTerm$stat){
      return(base::paste0(idxTerm$term,base::paste(base::toupper(base::substr(idxStat,1,1)),base::substr(idxStat,2,base::nchar(idxStat)),sep="")))
    }
  }))
})
names(nameStatTerm) <- stat

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'stat'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
dirSub <- c('data')
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,dirSub)))
log$debug(base::paste0('Minimum expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)

# Create the binning for each aggregation interval
timeBgnDiff <- list()
timeEndDiff <- list()
for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
  
  timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
} # End loop around aggregation intervals


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data
  idxDirData <- base::paste0(idxDirIn,'/data')
  fileData <- base::dir(idxDirData)
  if(base::length(fileData) > 1){
    log$warn(base::paste0('There is more than one data file in path: ',idxDirIn,'... Computing statistics for them all!'))
  }
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  idxDirOut <- base::paste0(Para$DirOut,InfoDirIn$dirRepo)
  idxDirOutStat <- base::paste0(idxDirOut,'/stats')
  base::dir.create(idxDirOutStat,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  
  
  # Are we computing uncertainty? If so, load the uncertainty coefficients file (there should be only 1)
  ucrtData <- NULL
  if("expUncert" %in% stat){
    idxDirUcrtCoef <- base::paste0(idxDirIn,'/uncertainty_coef')
    
    fileUcrt <- base::dir(idxDirUcrtCoef)
    if(base::length(fileUcrt) != 1){
      log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ',idxDirUcrtCoef,'... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it..."))
      ucrtCoef <- base::list()
    } else {
      nameFileUcrt <- base::paste0(idxDirUcrtCoef,'/',fileUcrt) # Full path to file
      
      # Open the uncertainty file
      ucrtCoef  <- base::try(rjson::fromJSON(file=nameFileUcrt,simplify=TRUE),silent=FALSE)
      if(base::class(ucrtCoef) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.')) 
        stop()
      }
      # Turn times to POSIX
      ucrtCoef <- base::lapply(ucrtCoef,FUN=function(idxUcrt){
        idxUcrt$start_date <- base::strptime(idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        idxUcrt$end_date <- base::strptime(idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        return(idxUcrt)
      })
      log$debug(base::paste0('Successfully read uncertainty coefficients from file: ',nameFileUcrt))
    }
    
    # Is there a folder for uncertainty data? If so, read it in.
    ucrtData <- NULL
    nameVarDataUcrt <- NULL
    if('uncertainty_data' %in% base::dir(idxDirIn)){
      idxDirUcrtData <- base::paste0(idxDirIn,'/uncertainty_data')
      log$info(base::paste0('Detected uncertainty data folder',idxDirUcrtData,'.'))
      
      fileUcrtData <- base::dir(idxDirUcrtData)
      if(base::length(fileUcrtData) != 1){
        log$warn(base::paste0("There are either zero or more than one uncertainty data files in path: ',idxDirUcrtData,'... Uncertainty data will not be read in. This is fine if the uncertainty function doesn't need it..."))
      } else {
        nameFileUcrtData <- base::paste0(idxDirUcrtData,'/',fileUcrtData) # Full path to file
        
        # Open the uncertainty data file
        ucrtData  <- base::try(NEONprocIS.base::def.read.parq(NameFile=nameFileUcrtData,log=log),silent=FALSE)
        if(base::any(base::class(ucrtData) == 'try-error')){
          log$error(base::paste0('File ', fileUcrtData,' is unreadable.')) 
          stop()
        } else {
          log$debug(base::paste0('Successfully read uncertainty data in file: ',fileUcrtData))
        }
        
      }
    } # End if statement around FDAS uncertainty
  } # End if statement around expUncert
    
  # Run through each data file
  for(idxFileData in fileData){
    
    # Load in data file in parquet format into data frame 'data'.  
    fileIn <- base::paste0(idxDirData,'/',idxFileData)
    data  <- base::try(NEONprocIS.base::def.read.parq(NameFile=fileIn,log=log),silent=FALSE)
    if(base::any(base::class(data) == 'try-error')){
      log$error(base::paste0('File ', fileIn,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in data file: ',fileIn))
    }

    # Validate the data
    valiData <-
      NEONprocIS.base::def.validate.dataframe(dfIn = data,
                                              TestNameCol = base::unique(c(
                                                'readout_time', base::names(ParaStat)
                                              )),
                                              log = log)
    if (!valiData) {
      base::stop()
    }
    timeMeas <- base::as.POSIXlt(data$readout_time) # Pull out time variable
    
    # Choose whether to compute stats, and for which variables, before we head into the loop
    compSkew <- base::any(base::names(statTerm) %in% c('skewness'))
    if(compSkew){
      termSkew <- base::unique(c(statTerm[['skewness']]))
    }
    compKurt <- base::any(base::names(statTerm) %in% c('kurtosis'))
    if(compKurt){
      termKurt <- base::unique(c(statTerm[['kurtosis']]))
    }
    
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      
      log$debug(base::paste0('Computing stats for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      
      # Intialize the output
      rpt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      rpt[,3:(base::length(nameTermStat)+2)] <- base::as.numeric(NA)
      base::names(rpt)[3:(base::length(nameTermStat)+2)] <- nameTermStat
      
      # Allocate data points to aggregation windows
      setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      
      # Allocate uncertainty data points to aggregation windows
      if(!base::is.null(ucrtData)){
        setTimeUcrt <- base::.bincode(base::as.numeric(base::as.POSIXlt(ucrtData$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      } else {
        setTimeUcrt <- base::numeric(0)
      }
      
      # Run through the time bins
      for(idxWndwTime in base::unique(setTime)){
        
        # Rows to pull
        dataWndwTime <- base::subset(data,subset=setTime==idxWndwTime)  
        
        # Compute common dependency stats 
        numPts <- base::colSums(x=!base::is.na(base::subset(dataWndwTime,select=termComp)),na.rm=FALSE)
        vari <- base::apply(X=base::subset(dataWndwTime,select=termComp),MARGIN=2,FUN=stats::var,na.rm=TRUE)
        sd <- base::sqrt(vari[termComp])
        if(compSkew){
          sumDiffMean03 <- base::apply(X=base::subset(dataWndwTime,select=termSkew),MARGIN=2,FUN=function(data){
            base::sum((data-base::mean(data, na.rm=TRUE))^3, na.rm=TRUE)})
        }
        if(compKurt){
          sumDiffMean04 <- base::apply(X=base::subset(dataWndwTime,select=termKurt),MARGIN=2,FUN=function(data){
            base::sum((data-base::mean(data, na.rm=TRUE))^4, na.rm=TRUE)})
        }

        # Run through the stats
        for(idxStat in base::names(statTerm)){
    
          # Compute the stat for this time window, for all the terms that need it. We will get a named vector, where the names correspond to the terms
          statIdx <- switch(idxStat,
                            mean=base::apply(X=base::subset(dataWndwTime,select=statTerm[['mean']]),MARGIN=2,FUN=base::mean,na.rm=TRUE),
                            median=base::apply(X=base::subset(dataWndwTime,select=statTerm[['median']]),MARGIN=2,FUN=stats::median,na.rm=TRUE),
                            minimum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['minimum']]),MARGIN=2,FUN=base::min,na.rm=TRUE)),
                            maximum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['maximum']]),MARGIN=2,FUN=base::max,na.rm=TRUE)),
                            sum=base::apply(X=base::subset(dataWndwTime,select=statTerm[['sum']]),MARGIN=2,FUN=base::sum,na.rm=TRUE),
                            variance=vari[statTerm[['variance']]],
                            stdDev=sd[statTerm[['stdDev']]],
                            stdEr=sd[statTerm[['stdEr']]]/base::sqrt(numPts[statTerm[['stdEr']]]),
                            numPts=base::as.integer(numPts[statTerm[['numPts']]]),
                            skewness=sumDiffMean03[statTerm[['skewness']]]/sd[statTerm[['skewness']]]^3/numPts[statTerm[['skewness']]],
                            kurtosis=sumDiffMean04[statTerm[['kurtosis']]]/sd[statTerm[['kurtosis']]]^4/numPts[statTerm[['kurtosis']]],
                            mad=base::apply(X=base::subset(dataWndwTime,select=statTerm[['mad']]),MARGIN=2,FUN=stats::mad,constant=1,na.rm=TRUE),
                            expUncert=NEONprocIS.stat::wrap.ucrt.dp01(data=dataWndwTime,FuncUcrt=FuncUcrt,ucrtCoef=ucrtCoef,ucrtData=base::subset(ucrtData,subset=setTimeUcrt==idxWndwTime),log=log)
          )

          # Dole out the stats to the proper output
          rpt[idxWndwTime,nameStatTerm[[idxStat]]] <- statIdx
          
        } # End loop through stats
        
      } # End loop through time windows
      
      # Fix the Inf/-Inf results for max/min, respectively, when there is no non-NA data
      rpt[rpt==Inf] <- NA
      rpt[rpt==-Inf] <- NA
      
      
      # Write out the file for this aggregation interval
      NameFileOutStat <- 
        NEONprocIS.base::def.file.name.out(nameFileIn = idxFileData, 
                                           prfx = base::paste0(idxDirOutStat, '/'),
                                           sufx = base::paste0('_basicStats_',Para$WndwAgr[idxWndwAgr])
        )
      rptWrte <- base::try(NEONprocIS.base::def.wrte.parq(data=rpt,NameFile=NameFileOutStat,NameFileSchm=NULL,Schm=SchmStat),silent=TRUE)
      if(base::class(rptWrte) == 'try-error'){
        log$error(base::paste0('Cannot write basic statistics file ', NameFileOutStat,'. ',attr(rptWrte,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Basic statistics written successfully in file: ',NameFileOutStat))
      }

      
    } # End loop around aggregation intervals
    
  } # End loop around files

  return()
} # End loop around datum paths 
  