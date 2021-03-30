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
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
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
#' 3. "FileSchmQm=value" (optional), where value is the full path to the avro schema for the output quality 
#' metrics file. If this input is not provided, the output schema for the stats will be auto-generated from  
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS 
#' IN THE SORTED FILE LIST. See output information below for details. 
#' 
#' 4. "WndwAgr=value", where value is the aggregation interval for which to compute statistics. It is formatted 
#' as a 3 character sequence, typically representing the number of minutes over which to compute statistics. 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must be an equal divisor of 
#' one day.
#' 
#' 5. "Thsh=value" (optional), where value is the threshold fraction for the sum of the alpha and beta quality 
#' metrics multiplied by the respective weights given in argument WghtAlphaBeta (below) at and above 
#' which triggers the final quality flag (value of 1). Default value = 0.2.
#' 
#' 6. "WghtAlphBeta=value" (optional), where value is a 2-element vector of weights, separated by pipes (|) for 
#' the alpha and beta quality metrics, respectively. The alpha and beta quality metrics (in fraction form) are 
#' multiplied by these respective weights and summed. If the resultant value is greater than the threshold value 
#' set in the Thsh argument, the final quality flag is raised. Default is "WghtAlphaBeta=2|1".
#' 
#' 7-N. "GrpQfX=value" (optional), where X is a number beginning at 1 and value contains the (exact) names of  
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
##############################################################################################
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
Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c(
                                        "DirIn",
                                        "DirOut",
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

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

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
Thsh <- Para$Thsh
WghtAlphBeta <- Para$WghtAlphBeta
log$debug(base::paste0('Threshold fraction for triggering final quality flag: ',Thsh))
log$debug(base::paste0('Respective weights applied to alpha and beta quality metrics in threshold evaluation: ',
                       base::paste0(WghtAlphBeta,collapse=',')))

# Parse the groups of flags feeding into alpha and beta quality metrics and the final quality flag 
nameParaGrpQf <- base::names(Para)[names(Para) %in% base::paste0("GrpQf",1:100)]
if(base::length(nameParaGrpQf) > 0){
  spltGrp <- Para[nameParaGrpQf]
  ParaGrp <- base::lapply(spltGrp,FUN=function(argSplt){
    base::list(name=argSplt[1],
               qf=utils::tail(x=argSplt,n=-1),
               nameQmGrp=base::paste0(argSplt[1],c("AlphaQM","BetaQM","FinalQF"))) # Note that a match for FinalQF columns are used below. Don't change.
  })
  for(idxGrp in base::names(ParaGrp)){
    log$debug(base::paste0('Alpha/beta QMs and finalQF will be computed for group name: ',ParaGrp[[idxGrp]]$name, 
                           ' with flags: ',base::paste0(ParaGrp[[idxGrp]]$qf,collapse=',')))
  }
  
  # Construct the names of the alpha, beta, and final QF columns
  nameQmGrp <- base::unlist(base::lapply(ParaGrp,FUN=function(idx){idx$nameQmGrp}))
  base::names(nameQmGrp)<- NULL
  
  # Check that the names are unique
  if(base::length(base::unique(nameQmGrp)) < base::length(nameQmGrp)){
    log$fatal('Group names for input parameters GrpQfX are not unique. This is not allowed. Check inputs.')
    stop()
  }
} else {
  ParaGrp <- NULL
  log$debug('Alpha/beta QMs and finalQF will be computed with all flags found in the input file(s).')
  
  # Construct the names of the alpha, beta, and final QF columns
  nameQmGrp <- c("AlphaQM","BetaQM","FinalQF") # Note that a match for FinalQF columns are used below. Don't change.
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
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Create the binning for each aggregation interval
timeDmmyBgn <- base::as.POSIXlt('1970-01-01',tz='GMT')
timeDmmyEnd <- timeDmmyBgn + base::as.difftime(1,units='days') 
timeBgnDiff <- list()
timeEndDiff <- list()
for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  
  # Time series of aggregation windows 
  timeBgnSeq <- base::as.POSIXlt(base::seq.POSIXt(from=timeDmmyBgn,to=timeDmmyEnd,by=base::format(WndwAgr[idxWndwAgr])))
  timeBgnSeq <- utils::head(timeBgnSeq,n=-1) # Lop off the last one at timeEnd 
  timeEndSeq <- timeBgnSeq + WndwAgr[idxWndwAgr] 
  if(utils::tail(timeEndSeq,n=1) != timeDmmyEnd){
    log$fatal(base::paste0('The aggregation interval must be an even divisor of one day. An aggregation interval of ',
                           WndwAgr[idxWndwAgr],' does not fit this requirement. Check inputs.'))
    stop()
  }
  
  timeBgnDiff[[idxWndwAgr]] <- timeBgnSeq - timeDmmyBgn # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff[[idxWndwAgr]] <- timeEndSeq - timeDmmyBgn # Add to timeBgn of each day to represent the end time sequence
} # End loop around aggregation intervals


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for flags
  idxDirQf <- base::paste0(idxDirIn,'/flags')
  fileQf <- base::sort(base::dir(idxDirQf))
  log$info(base::paste0('Flags from ', base::length(fileQf),' files will be combined for computation of quality metrics.'))

  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutQm <- base::paste0(idxDirOut,'/quality_metrics')
  base::dir.create(idxDirOutQm,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  
  
  # Combine flags files
  qf <- NULL
  qf <-
    NEONprocIS.base::def.file.comb.ts(
      file = base::paste0(idxDirQf, '/', fileQf),
      nameVarTime = 'readout_time',
      log = log
    )
  
  # Remove any columns for variables we should ignore
  qf <- qf[,!(base::names(qf) %in% VarIgnr)]
  
  # Take stock of our quality flags
  numQf <- base::ncol(qf)-1 # Minus the readout_time column
  numNa <- base::apply(X=base::is.na(qf),MARGIN=2,FUN=base::sum)
  if(base::sum(numNa) > 0){
    log$warn(base::paste0(numNa[numNa > 0]," NA values found for variable ",base::names(numNa[numNa > 0]),". These will be replaced with -1."))
    qf[base::is.na(qf)]<- -1
  }
  
    
  # Pull the time variable
  timeMeas <- base::as.POSIXlt(qf$readout_time)
  
  # Figure out the names of the output columns
  nameQf <- base::setdiff(base::names(qf),"readout_time")
  nameQm <- c(base::unlist(base::lapply(nameQf,base::paste0,y=c("PassQM","FailQM","NAQM"))),nameQmGrp)
  log$debug(base::paste0('The default output column names are as follow (ensure any output schema that changes these names matches this order):',
                         base::paste0(c('startDateTime','endDateTime',nameQm),collapse=',')))
    
  # Initialize QM column indexing vector
  setQm <- (0:(numQf-1))*3
  
  # Run through each aggregation interval, creating the daily time series of windows
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    
    log$debug(base::paste0('Computing quality metrics for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
    
    # Create start and end time sequences
    timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
    timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
    timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
    
    # Intialize the output
    rpt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
    rpt[,3:(base::length(nameQm)+2)] <- base::as.numeric(NA)
    base::names(rpt)[3:(base::length(nameQm)+2)] <- nameQm
    setQfFinl <- base::which(base::grepl(pattern='FinalQF',x=base::names(rpt),ignore.case = FALSE)) # note the final quality flags. We will turn these to integer.
    for(idxQfFinl in setQfFinl){
      class(rpt[[idxQfFinl]]) <- "integer" # Make the final quality flags integer class
    }
    
    # Allocate qf points to aggregation windows
    setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
    
    # Run through the time bins
    for(idxWndwTime in base::unique(setTime)){

      # Rows to pull
      qfWndwTime <- base::subset(qf,subset=setTime==idxWndwTime)   
      
      # Compute quality metrics (percent)
      numRow <- base::nrow(qfWndwTime)
      rpt[idxWndwTime,setQm+3] <- base::colSums(x=base::subset(qfWndwTime,select=-readout_time)==0,na.rm=TRUE)/numRow*100 # Pass
      rpt[idxWndwTime,setQm+4] <- base::colSums(x=base::subset(qfWndwTime,select=-readout_time)==1,na.rm=TRUE)/numRow*100 # Fail
      rpt[idxWndwTime,setQm+5] <- base::colSums(x=base::subset(qfWndwTime,select=-readout_time)==-1,na.rm=TRUE)/numRow*100 # Na
        
      # Compute alpha & beta QMs and the final quality flag
      if(base::is.null(ParaGrp)){
        # Use all flags in computation
        qmFinl<-eddy4R.qaqc::def.qf.finl(qf=base::subset(qfWndwTime,select=-readout_time),
                                   WghtAlphBeta=WghtAlphBeta,Thsh=Thsh)$qfqm[c("qmAlph","qmBeta","qfFinl")]*c(100,100,1)
        base::class(qmFinl$qfFinl) <- "integer"
        rpt[idxWndwTime,nameQmGrp]<-qmFinl
      } else {
        for(idxGrp in base::names(ParaGrp)){
          qmFinl<-eddy4R.qaqc::def.qf.finl(qf=base::subset(qfWndwTime,select=ParaGrp[[idxGrp]]$qf),WghtAlphBeta=WghtAlphBeta,
                                           Thsh=Thsh)$qfqm[c("qmAlph","qmBeta","qfFinl")]*c(100,100,1)
          base::class(qmFinl$qfFinl) <- "integer"
          rpt[idxWndwTime,ParaGrp[[idxGrp]]$nameQmGrp]<-qmFinl
        }
      }
      
    } # End loop through time windows
    
    # Write out the file for this aggregation interval. Replace the final underscore-delimited component of one of the input 
    # filenames, assuming this final component denotes the type of flags that are in the file
    fileQmOutSplt <- base::strsplit(utils::tail(fileQf,1),'[_]')[[1]] # Separate underscore-delimited components of the file name
    fileQmOutSplt[base::length(fileQmOutSplt)] <- base::paste(base::paste('qualityMetrics',Para$WndwAgr[idxWndwAgr],sep='_'),utils::tail(x=base::strsplit(utils::tail(x=fileQmOutSplt,n=1),'[.]')[[1]],n=-1),sep='.') # Replace last component, but try to keep the extension
    fileQmOut <- base::paste(fileQmOutSplt,collapse='_')
    NameFileOutQm <- base::paste0(idxDirOutQm,'/',fileQmOut)

    rptWrte <- base::try(NEONprocIS.base::def.wrte.parq(data=rpt,NameFile=NameFileOutQm,NameFileSchm=NULL,Schm=SchmQm),silent=TRUE)
    if(base::class(rptWrte) == 'try-error'){
      log$error(base::paste0('Cannot write quality metrics file ', NameFileOutQm,'. ',attr(rptWrte,"condition"))) 
      stop()
    } else {
      log$info(base::paste0('Quality metrics written successfully in file: ',NameFileOutQm))
    }
    
  } # End loop around aggregation intervals

  return()
} # End loop around datum paths 
  