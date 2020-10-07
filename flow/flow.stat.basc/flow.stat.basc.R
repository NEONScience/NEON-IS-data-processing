##############################################################################################
#' @title Basic statistics and uncertainty module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Basic statistics module for NEON IS data processing. Computes one or more
#' of the following statistics: mean, median, minimum, maximum, sum, variance, standard 
#' deviation, standard error, number of points, expanded uncertainty
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
#' is output (see options in TermStatX), also required is the folder 'uncertainty_coef' at the same level 
#' as the data folder.
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
#' mean, median, minimum, maximum, sum, variance, stdDev, stdEr, numPts, expUncert. For example, to compute the
#' mean, minimum, maximum, and expanded uncertainty for term "temp", the argument is 
#' "TermStat1=temp:mean|minimum|maximum|expUncert". For expUncert, the default is to compute only uncertainty due
#' to natural variation (standard error) and calibration uncertainty (U_CVALA3). For data in which
#' FDAS uncertainty also applies, append '(R)' or '(V)' to the expUncert to indicate that resistance or voltage FDAS 
#' uncertainty should be added, respectively. For example, "TermStat1=temp:mean|minimum|maximum|expUncert(R)" indicates
#' that FDAS uncertainty for resistance measurements should be included in the uncertainty estimate. Note that FDAS
#' uncertainty data and FDAS uncertainty coefficients as produced by the calibration module are required in order 
#' to compute FDAS uncertainty. 
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
##############################################################################################
options(digits.secs = 3)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","WndwAgr","TermStat1"),
                                      NameParaOptn=c("FileSchmStat",base::paste0("TermStat",2:100),
                                                     "DirSubCopy"),log=log)


# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

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
  typeFdas <- NULL
  statUcrt <- base::substr(stat,1,9) %in% "expUncert"
  if(base::sum(statUcrt) > 1){
    # Error if multiple
    log$fatal(base::paste0('Multiple expUncert stats for term ', term, '. This is not allowed.'))
    stop()
  } else if (base::sum(statUcrt) == 1){
    # Parse resistance or voltage
    if (base::grepl(pattern = '(R)', x = stat[statUcrt])) {
      # FDAS resistance uncertainty applies
      typeFdas <- 'R'
      stat[statUcrt] <- gsub(
        pattern = '[(R)]',
        replacement = '',
        x = stat[statUcrt]
      )

    } else if (base::grepl(pattern = '(V)', x = stat[statUcrt])) {
      # FDAS voltage uncertainty applies
      typeFdas <- 'V'
      stat[statUcrt] <- gsub(
        pattern = '[(V)]',
        replacement = '',
        x = stat[statUcrt]
      )
    } 
  }
  base::list(term=term,
             stat=stat,
             typeFdas=typeFdas,
             nameTermStat=base::paste0(term,base::paste(base::toupper(base::substr(stat,1,1)),base::substr(stat,2,base::nchar(stat)),sep="")))
})
names(ParaStat) <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$term})) # Pull out a list of all the terms we are going to use 
nameVarUcrtFdas <- base::unlist(base::lapply(ParaStat,FUN=function(idx){if(!base::is.null(idx$typeFdas)){return(idx$term)}else{return(NULL)}})) # Terms for which FDAS uncertainty applies

# Compile the stats we are going to compute
stat <- base::unique(base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$stat})))

# Compile the full set of TermStat combinations, in the order in which they will be output
nameTermStat <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$nameTermStat}))

# Error check the statistic names
nameStat <- c("mean", "median", "minimum", "maximum", "sum", "variance", "stdDev", "stdEr", "numPts", "expUncert", "skewness", "kurtosis")
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
if("expUncert" %in% stat){
  dirSub <- c('data','uncertainty_coef')
} else {
  dirSub <- c('data')
}
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,dirSub)))
log$debug(base::paste0('Minimum expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Create the binning for each aggregation interval
timeBgnDiff <- list()
timeEndDiff <- list()
for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
  
  timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
} # End loop around aggregation intervals


# Process each datum path
for(idxDirIn in DirIn){
  
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
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutStat <- base::paste0(idxDirOut,'/stats')
  base::dir.create(idxDirOutStat,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  
  
  # Are we computing uncertainty? If so, load the uncertainty coefficients file (there should be only 1)
  if("expUncert" %in% stat){
    idxDirUcrtCoef <- base::paste0(idxDirIn,'/uncertainty_coef')
    
    fileUcrt <- base::dir(idxDirUcrtCoef)
    if(base::length(fileUcrt) != 1){
      log$warn(base::paste0('There are either zero or more than one uncertainty coefficient files in path: ',idxDirUcrtCoef,'... Uncertainty stats will all be NA!'))
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
    }
    
    # Is there a folder for uncertainty data? If so, read it in and search for fdas uncertainty.
    ucrtData <- NULL
    nameVarDataUcrtFdas <- NULL
    if('uncertainty_data' %in% base::dir(idxDirIn)){
      idxDirUcrtData <- base::paste0(idxDirIn,'/uncertainty_data')
      log$info(base::paste0('Detected uncertainty data folder in ',idxDirUcrtData,'.'))
      
      fileUcrtData <- base::dir(idxDirUcrtData)
      if(base::length(fileUcrtData) != 1){
        log$warn(base::paste0('There are either zero or more than one uncertainty data files in path: ',idxDirUcrtData,'... uncertainty for FDAS-applicable variables will be NA!'))
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
        nameColUcrtData <- base::names(ucrtData) # Column names
        
        # Get names of variables that FDAS uncertainty applies
        nameColUcrtFdas <- nameColUcrtData[base::grepl(pattern='_ucrtFdas',x=nameColUcrtData)] # search for ucrtFdas columns
        nameVarDataUcrtFdas <- base::unique(base::unlist(base::lapply(base::strsplit(nameColUcrtFdas,'_'),utils::head,n=1))) # variables

        # Reset uncertainty data to NULL if no FDAS uncertainty columns
        if(base::length(nameVarDataUcrtFdas) == 0){
          ucrtData <- NULL
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
    nameVarIn <- base::names(data)
    
    # Pull out the time variable
    if(!('readout_time' %in% nameVarIn)){
      log$error(base::paste0('Variable "readout_time" is required, but cannot be found in file: ',fileIn)) 
      stop()
    }
    timeMeas <- base::as.POSIXlt(data$readout_time)
    
    # For each of the variables in the data file, go through and remove the FDAS uncertainties for any time points that were filtered (e.g. from the QA/QC step)
    if(base::sum(nameVarIn %in% nameVarDataUcrtFdas) > 0){
      for(idxVar in nameVarIn){
        # Do we have FDAS uncertainty data for this variable? If so, create NAs where they exist in the data
        ucrtData[base::is.na(data[[idxVar]]),base::unlist(base::lapply(base::strsplit(nameColUcrtData,'_'),utils::head,n=1))==idxVar] <- NA
      }      
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
      
      # Allocate FDAS uncertainty data points to aggregation windows
      if(!base::is.null(ucrtData)){
        setTimeUcrtFdas <- base::.bincode(base::as.numeric(base::as.POSIXlt(ucrtData$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      } else {
        setTimeUcrtFdas <- numeric(0)
      }
      
      
      # Run through the stats
      for(idxStat in base::names(statTerm)){
        
        log$debug(base::paste0('Computing ',idxStat, ' for terms: ', base:: paste0(statTerm[[idxStat]], collapse=',')))
        
        # Set up the output for the terms we'll compute this stat for
        statIdx <- base::subset(rpt,select=nameStatTerm[[idxStat]])
        
        # Run through the time bins
        for(idxWndwTime in base::unique(setTime)){

          # Rows to pull
          dataWndwTime <- base::subset(data,subset=setTime==idxWndwTime)  
          
          # Compute dependency stats we need for the chosen stats
          if(idxStat %in% c('numPts','stdEr','variance','expUncert', 'skewness', 'kurtosis')){
            numPts <- base::as.list(base::colSums(x=!base::is.na(base::subset(dataWndwTime,select=base::unique(c(statTerm[['numPts']],statTerm[['stdEr']],statTerm[['expUncert']])))),na.rm=FALSE))
            vari <- base::as.list(base::apply(X=base::subset(dataWndwTime,select=base::unique(c(statTerm[['variance']],statTerm[['stdEr']],statTerm[['expUncert']]))),MARGIN=2,FUN=stats::var,na.rm=TRUE))
            stdEr <- base::as.list(base::sqrt(base::unlist(vari[base::unique(c(statTerm[['stdEr']],statTerm[['expUncert']]))]))/base::sqrt(base::unlist(numPts[base::unique(c(statTerm[['stdEr']],statTerm[['expUncert']]))])))
            skewness <- base::as.list(
              base::apply(X=base::subset(dataWndwTime,select=statTerm[['skewness']]), MARGIN = 2, function(data){
                ((base::sum(data-base::mean(data, na.rm=TRUE))/stats::sd(data,na.rm=TRUE))^3)/(base::length(stats::na.omit(data))-1)
              }
              )
            )
            kurtosis <- base::as.list(
              base::apply(X=base::subset(dataWndwTime,select=statTerm[['kurtosis']]), MARGIN = 2, function(data){
                ((base::sum(data-base::mean(data, na.rm=TRUE))/stats::sd(data,na.rm=TRUE))^4)/(base::length(stats::na.omit(data))-1)
              }
              )
            )
          }
          
          # For uncertainty, we need the correct coefficients for each term
          if(idxStat %in% c('expUncert')){
            
            # Calibration uncertainty
            coefUcrtCal <- base::lapply(statTerm[['expUncert']],FUN=function(idxTerm){
              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALA3)
              mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == idxTerm && idxUcrt$Name == 'U_CVALA3' && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coeffiecient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be NA
                coefUcrtIdx <- base::as.numeric(NA)
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtIdx <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }
              return(coefUcrtIdx)
            })
            
            # FDAS uncertainty
            dataUcrtFdasWndwTime <- base::subset(ucrtData,subset=setTimeUcrtFdas==idxWndwTime) 
            ucrtFdasValu <- base::lapply(statTerm[['expUncert']],FUN=function(idxTerm){
              
              if(!(idxTerm %in% nameVarUcrtFdas)){
                # No FDAS uncertainty applies
                ucrtFdasValuIdxTerm <- 0
                return(ucrtFdasValuIdxTerm)
              }
              
              # Which Fdas uncertainty, resistance or voltage
              typeFdas <- ParaStat[[idxTerm]]$typeFdas
              
              # Fdas uncertainty multiplier
              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3 - only one should be populated)
              mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == idxTerm && 
                  idxUcrt$Name == base::paste0('U_CVAL',typeFdas,'3') && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coefficient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be 0
                coefUcrtFdas <- NA
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtFdas <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }

              # Fdas uncertainty offset
              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3 - only one should be populated)
              mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == idxTerm && 
                  idxUcrt$Name == base::paste0('U_CVAL',typeFdas,'4') && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coefficient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be 0
                coefUcrtFdasOfst <- NA
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtFdasOfst <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }
              
              # Do some error checking
              if (base::is.na(coefUcrtFdas) || base::is.na(coefUcrtFdasOfst)){
                # At least one of the terms is not present (but should be). Send back NA so the uncertainty is NA
                log$warn(base::paste0('At least one of the expected FDAS uncertainty coeffiecients is not present. Setting uncertainty to NA for term: ',idxTerm, ', averaging window ',idxWndwTime))
                ucrtFdasValuIdxTerm <- NA
                return(ucrtFdasValuIdxTerm)
              }
              
              # Do we have FDAS uncertainty data for this term?
              if (idxTerm %in% nameVarDataUcrtFdas){
                # Construct the column names of the output we want
                nameColRawIdx <- base::paste0(idxTerm,'_raw')
                nameColDervCalIdx <- base::paste0(idxTerm,'_dervCal')
                nameColUcrtCombIdx <- base::paste0(idxTerm,'_ucrtComb')
                
                # Find the index of the max combined standard measurement uncertainty 
                idxMax <- utils::head(base::which(dataUcrtFdasWndwTime[[nameColUcrtCombIdx]] == base::max(dataUcrtFdasWndwTime[[nameColUcrtCombIdx]],na.rm=TRUE)),n=1)
                
                # Compute the FDAS uncertainty
                if(!is.na(idxMax) && base::length(idxMax)==1){
                  ucrtFdasValuIdxTerm <-base::abs(dataUcrtFdasWndwTime[[nameColDervCalIdx]][idxMax])*(coefUcrtFdas*dataUcrtFdasWndwTime[[nameColRawIdx]][idxMax] + coefUcrtFdasOfst)
                } else {
                  ucrtFdasValuIdxTerm <- NA
                }
              } else if (!base::is.na(coefUcrtFdas+coefUcrtFdasOfst)){
                # We have FDAS uncertainty coefficients for this term, but there's no FDAS uncertainty data. Turn FDAS uncertainty to NA.
                log$error(base::paste0('FDAS uncertainty data expected for term ',idxTerm,' but not found. FDAS uncertainty set to NA'))
                ucrtFdasValuIdxTerm <- NA
              } 
              
              return(ucrtFdasValuIdxTerm)
              
            })     
            
          }
          browser()
          # Compute the stat for this time window, for all the terms that need it. We will get a named vector, where the names correspond to the terms
          statIdx[idxWndwTime,] <- switch(idxStat,
                            mean=base::apply(X=base::subset(dataWndwTime,select=statTerm[['mean']]),MARGIN=2,FUN=base::mean,na.rm=TRUE),
                            median=base::apply(X=base::subset(dataWndwTime,select=statTerm[['median']]),MARGIN=2,FUN=stats::median,na.rm=TRUE),
                            minimum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['minimum']]),MARGIN=2,FUN=base::min,na.rm=TRUE)),
                            maximum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['maximum']]),MARGIN=2,FUN=base::max,na.rm=TRUE)),
                            sum=base::apply(X=base::subset(dataWndwTime,select=statTerm[['sum']]),MARGIN=2,FUN=base::sum,na.rm=TRUE),
                            variance=base::unlist(vari[statTerm[['variance']]]),
                            stdDev=base::apply(X=base::subset(dataWndwTime,select=statTerm[['stdDev']]),MARGIN=2,FUN=stats::sd,na.rm=TRUE),
                            stdEr=base::unlist(stdEr[statTerm[['stdEr']]]),
                            numPts=base::as.integer(base::unlist(numPts[statTerm[['numPts']]])),
                            expUncert=2*base::sqrt(base::unlist(stdEr[statTerm[['expUncert']]])^2 + base::unlist(coefUcrtCal[statTerm[['expUncert']]])^2 + base::unlist(ucrtFdasValu[statTerm[['expUncert']]])^2),
                            skewness=base::unlist(skewness[statTerm[['skewness']]]),
                            kurtosis=base::unlist(kurtosis[statTerm[['kurtosis']]])
                    )
          
        } # End loop through time windows
        
        # Dole out these stats to the proper output
        rpt[,nameStatTerm[[idxStat]]] <- statIdx
        
      } # End loop through stats
      
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

} # End loop around datum paths 
  