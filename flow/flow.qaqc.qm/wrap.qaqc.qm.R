##############################################################################################
#' @title Quality metrics module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Quality metrics module for NEON IS data processing. Aggregates quality flags
#' at specified time intervals over 1 day of data and computes pass, fail, and na quality metrics in addition 
#' to alpha and beta summary metrics and the final quality flag.
#' General code workflow:
#'      Error-check input parameters
#'      Read in and combine flags files 
#'      Remove flags to ignore
#'      Set any NA values in the flags to -1
#'      Compute alpha and beta QFs for each L0' record and group, according to specifications in the input
#'         arguments. This includes:
#'            Force any flags to particular values based on values of other flags 
#'            Create summary alpha and beta flags for each individual record for each group 
#'      For each aggregation interval:
#'         For each time bin of each aggregation interval:
#'            Compute the quality metrics for each flag, including % Pass, % Fail, % NA (could not evaluate)
#'            Compute summary metrics (alpha and beta QMs) and the final quality flag for each group
#'        Write out the entire set of quality metrics, and the summary metrics and final QF for each group
#'      For each aggregation interval...
#'         Bin the flag data
#'         Compute the quality metrics
#'         Write the output
#'
#' @param DirIn Character value. The path to parent directory where the flags exist. 
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
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param WndwAgr Difftime vector. The aggregation interval(s) for which to compute quality metrics. 
#' Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the expected length of the input data is one day. 
#' The aggregation interval must be an equal divisor of one day. 
#' 
#' @param Thsh Numeric value. The threshold fraction for the sum of the alpha and beta quality 
#' metrics multiplied by the respective weights given in argument WghtAlphaBeta (below) at and above 
#' which triggers the final quality flag. Default value = 0.2.
#' 
#' @param WghtAlphBeta 2-element numeric vector of weights for the alpha and beta quality metrics, respectively. 
#' The alpha and beta quality metrics (in fraction form) are multiplied by these respective weights and summed. 
#' If the resultant value is greater than the threshold value set in the Thsh argument, the final quality flag is 
#' raised. Default is c(2,1).
#' 
#' @param ParaGrp (optional) A named list of lists, each sublist representing the specifications for computing a group of 
#' alpha QM, beta QM, and final QF. List names at the top level are used as a prefix to the output alpha QMs, betaQMs, and final QFs.
#' If no group name/prefix is desired, use a zero-character string (i.e. ""). Each sublist has 3 named elements:\cr
#' \code{qfAlph}: Character vector. The exact names of the quality flags that should be used in the computation of the alpha 
#' QM for the group. \cr
#' \code{qfBeta}: Character vector. The exact names of the quality flags that should be used in the computation of the beta 
#' QM for the group. \cr
#' \code{qfBetaIgnr}: Optional (may be omitted). Character vector. The exact names of the quality flags that, if any of their values equals 1, 
#' the beta QF for each record (which is then aggregated into the beta QM for each aggregation interval) for the indicated group is set to 0. \cr
#' Note that a flag may be specified in multiple groups (i.e. will feed into multiple final quality flags). Also note that quality 
#' metrics for all quality flags found in the input files will be output. The ParaGrp argument 
#' simply dictate what QFs feed into each set of alpha & beta quality metrics and the final quality flag. If this argument is NULL
#' (default), all quality flags in the file(s) will be used to compute a single set of alpha and beta quality metrics and 
#' final quality flag. 
#' 
#' @param ParaForc (optional) A list of lists, each sublist representing the specifications for forcing one or more quality flags  
#' (excluding alpha, beta, and final QFs) to a value if the value of a "driver" flag equals a particular value. 
#' Each sublist has 3 named elements:\cr
#' \code{qfDrve} Character value. The name of the driver flag \cr
#' \code{valuDrve} Numeric value. The "driver value" of the driver flag which activates the force \cr
#' \code{qfForc} Character value. The name(s) of the "forced" flag(s) \cr
#' \code{valuForc} Numeric value. The "forced" value \cr
#' For example, if tempRangeQF and tempPersistenceQF should be set to -1 when sensorNAQF is 1, then the argument is 
#' ParaForc <- list(list(qfDrve="sensorNAQF",valuDrve=1,qfForc=c("tempRangeQF","tempPersistenceQF"),valuForc=-1)).\cr
#' Note that the logic described here occurs before the alpha, beta, and final quality flags are calculated, and
#' forcing actions will occur in the order found in this argument. Also note that before this argument is fulfilled, any 
#' NA values in quality flags will be set to -1.
#'  
#' @param VarIgnr (optional) character vector. The names of the variables that should be ignored if 
#' found in the input files. No quality metrics will be computed for these variables and they will not be included 
#' in the output. Do not include readout_time here.  Defaults to NULL.
#' 
#' @param SchmQm (Optional).  A json-formatted character string containing the schema for the output quality 
#' metrics file. If this input is not provided, the output schema for the will be auto-generated from  
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS
#' IN THE SORTED FILE LIST. See output information below for details.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the flags folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Quality metrics for each aggregation interval output in Parquet format in DirOutBase, where the terminal 
#' directory of DirOutBase replaces BASE_REPO but otherwise retains the child directory structure of the input 
#' path. Directory 'quality_metrics' will automatically populated in the output directory, where the files 
#' for each aggregation interval will be placed. Any other folders specified in argument DirSubCopy will be 
#' copied over unmodified with a symbolic link.
#' If no output schema is provided for the quality metrics, the variable names for quality metrics will be a combination 
#' of the quality flag and PassQM, FailQM, or NAQM. The variable names for the alpha & beta quality metrics and final 
#' quality flag will be a combination of the group name (including none) and alphaQM, betaQM, or FinalQF. The order of 
#' the outputs will be all quality metrics in the same order they were found in the sorted (increasing order) input
#' files, with nested Pass, Fail, and NA QMs for each flag. These will be followed by the alpha and beta QMs and final 
#' quality flag groups in the same order as the ParaGrp inputs. Additionally, the first two columns of the output file 
#' will contain the start and end times for the aggregation interval, labeled "startDateTime" and "endDateTime", 
#' respectively. The quality metrics are calculated for readout times in the interval [startDateTime endDateTime), with 
#' an open brack on the right (i.e. inclusive of the startDateTime but exclusive of the endDateTime). An example column
#' ordering: Say there are two input files named outflagsA.parquet and outflagsB.parquet, where outflagsA.parquet contains flag 
#' tempValidCalQF and outflagsB.parquet contains flags tempRangeQF, and the ParaGrp argument is 
#' ParaGrp=list(GrpQf1=list(name='temp',qf=c('tempRangeQf','tempValidCalQF')). The ordering of the output columns will be 
#' startDateTime, endDateTime, tempValidCalQFPassQM, tempValidCalQFFailQM, tempValidCalQFNAQM, tempRangeQFPassQM, 
#' tempRangeQFFailQM, tempRangeQFNAQM, tempAlphaQM, tempBetaQM, and tempFinalQF, in that order. The names of the output columns 
#' may be replaced by providing an output schema in argument SchmQm. However, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA MATCHES 
#' THIS COLUMN ORDERING. Otherwise, column names will not pertain to the metrics in the column. The best way to ensure that the 
#' output schema matches the expected column ordering is to do a trial run without inputting an output schema. The default output
#' column names will be used, which can then serve as a guide for crafting the output schema.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in 
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr 

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaGrp <- list(relativeHumidity=list(qfAlph=c("relativeHumidityNullQF",
#'                                                "relativeHumidityGapQF",
#'                                                "relativeHumidityRangeQF",
#'                                                "relativeHumidityStepQF",
#'                                                "relativeHumiditySpikeQF",
#'                                                "relativeHumidityPersistenceQF",
#'                                                "errorStateQF"),
#'                                      qfBeta=c("relativeHumidityNullQF",
#'                                                "relativeHumidityGapQF",
#'                                                "relativeHumidityRangeQF",
#'                                                "relativeHumidityStepQF",
#'                                                "relativeHumiditySpikeQF",
#'                                                "relativeHumidityPersistenceQF",
#'                                                "errorStateQF"),
#'                                      qfBetaIgnr=c("relativeHumidityNullQF",
#'                                                   "relativeHumidityGapQF")
#'                                      ),
#'                 temperature=list(qfAlph=c("temperatureNullQF",
#'                                           "temperatureGapQF",
#'                                           "temperatureRangeQF",
#'                                           "temperatureStepQF",
#'                                           "temperatureSpikeQF",
#'                                           "temperaturePersistenceQF",
#'                                           "errorStateQF"),
#'                                  qfBeta=c("temperatureNullQF",
#'                                           "temperatureGapQF",
#'                                           "temperatureRangeQF",
#'                                           "temperatureStepQF",
#'                                           "temperatureSpikeQF",
#'                                           "temperaturePersistenceQF",
#'                                           "errorStateQF"),
#'                                  qfBetaIgnr=c("temperatureNullQF",
#'                                               "temperatureGapQF")
#'                                        ),
#'                  dewPoint=list(qfAlph=c("dewPointNullQF",
#'                                         "dewPointGapQF",
#'                                         "dewPointRangeQF",
#'                                         "dewPointStepQF",
#'                                         "dewPointSpikeQF",
#'                                         "dewPointPersistenceQF",
#'                                         "errorStateQF"),
#'                                qfBeta=c("dewPointNullQF",
#'                                         "dewPointGapQF",
#'                                         "dewPointRangeQF",
#'                                         "dewPointStepQF",
#'                                         "dewPointSpikeQF",
#'                                         "dewPointPersistenceQF",
#'                                         "errorStateQF"),
#'                                qfBetaIgnr=c("dewPointNullQF",
#'                                            "dewPointGapQF")
#'                                )
#'                  )
#' wrap.qaqc.qm(DirIn="~/pfs/relHumidity_qaqc_flags_group/hmp155/2020/01/01/CFGLOC101252",
#'                DirOutBase="~/pfs/out",
#'                WndwAgr=as.difftime(c(1,30),units='mins'),
#'                ParaGrp=ParaGrp                
#' )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-10-25)
#     Convert flow script to wrapper function
#   Cove Sturtevant (2021-12-07)
#     Implement new input arguments for:
#       forcing flag values based on other flag values prior to QM computations
#       specifying separately the flags that feed into each alpha QM and beta QM
#       forcing the betaQF of a record to 0 based on flag values
#   Cove Sturtevant (2022-03-24)
#     Bug fix causing the leading zeros in the aggregation interval to be dropped from the output filenames
##############################################################################################
wrap.qaqc.qm <- function(DirIn,
                         DirOutBase,
                         WndwAgr,
                         Thsh=0.2,
                         WghtAlphBeta=c(2,1),
                         ParaGrp=NULL,
                         ParaForc=NULL,
                         VarIgnr=NULL,
                         SchmQm=NULL,
                         DirSubCopy=NULL,
                         log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Pull group names and error check
  nameGrp <- base::names(ParaGrp)
  nameQmGrp <- base::paste0(base::rep(nameGrp,each=3),c('AlphaQM','BetaQM','FinalQF'))
  # Check that the names are unique
  if(base::length(base::unique(nameQmGrp)) < base::length(nameQmGrp)){
    log$fatal('Group names are not unique. This is not allowed. Check ParaGrp argument.')
    stop()
  }
  if(base::is.null(nameGrp)){
    numGrp <- 1
  } else {
    numGrp <- base::length(nameGrp)
  }
  
  # Create the binning for each aggregation interval
  timeBgnDiff <- list()
  timeEndDiff <- list()
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'),log=log)
    
    timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  } # End loop around aggregation intervals
  
  
  # Get directory listing of input directory. Expect subdirectories for flags
  dirQf <- base::paste0(DirIn,'/flags')
  fileQf <- base::sort(base::dir(dirQf))
  log$info(base::paste0('Flags from ', base::length(fileQf),' files will be combined for computation of quality metrics.'))
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  dirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  dirOutQm <- base::paste0(dirOut,'/quality_metrics')
  base::dir.create(dirOutQm,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn,'/',DirSubCopy),dirOut,log=log)
  }  
  
  # Combine flags files
  qf <- NULL
  qf <- NEONprocIS.base::def.file.comb.ts(
          file = base::paste0(dirQf, '/', fileQf),
          nameVarTime = 'readout_time',
          log = log
        )
  
  # Pull the time variable
  timeMeas <- base::as.POSIXlt(qf$readout_time)
  
  # Remove any columns for variables we should ignore (and readout time)
  qf <- qf[,!(base::names(qf) %in% c(VarIgnr,'readout_time'))]
  
  # Take stock of our quality flags
  numQf <- base::ncol(qf)
  numNa <- base::apply(X=base::is.na(qf),MARGIN=2,FUN=base::sum)
  if(base::sum(numNa) > 0){
    log$warn(base::paste0(numNa[numNa > 0]," NA values found for variable ",base::names(numNa[numNa > 0]),". These will be replaced with -1."))
    qf[base::is.na(qf)] <- -1
  }
    
  # Force any flags that were indicated in the input arguments
  if (!base::is.null(ParaForc)) {
    for (idxForc in 1:length(ParaForc)) {
      qf <-
        NEONprocIS.qaqc::def.qf.forc(
          qf = qf,
          nameQfDrve = ParaForc[[idxForc]]$qfDrve,
          valuDrve = ParaForc[[idxForc]]$valuDrve,
          nameQfForc = ParaForc[[idxForc]]$qfForc,
          valuForc = ParaForc[[idxForc]]$valuForc,
          log = log
        )
    }
  }  
  

  # Figure out the names of the output columns
  nameQf <- base::names(qf)
  nameQm <- c(
    base::paste0(
      base::rep(nameQf,each=3),
      c("PassQM","FailQM","NAQM")
      ),
    nameQmGrp
    )
  log$debug(
    base::paste0(
      'The default output column names are as follow (ensure any output schema that changes these names matches this order):',
      base::paste0(c('startDateTime',
                     'endDateTime',
                     nameQm),
                   collapse=',')
      )
    )

  # Calculate the L0' summary flags (alpha QF and beta QF) for each group
  qfSmmy <- base::vector(mode="list",length=numGrp)
  base::names(qfSmmy) <- nameGrp
  for (idxGrp in 1:numGrp) {
    qfSmmyIdx <- NULL
    nameQfIdx <- c(base::paste0(nameGrp[idxGrp],c('AlphaQF','BetaQF','FinalQF')))
    
    log$debug(
      base::paste0(
        "Computing L0' summary flags for group ",
        nameGrp[idxGrp],
        '...'
      )
    )
    
    # Compute the flags
    qfSmmyIdx <-
      NEONprocIS.qaqc::def.qm.dp0p(qf = qf,
                                   Para = ParaGrp[[idxGrp]],
                                   log = log)
    
    # Put in the output
    qfSmmy[[idxGrp]] <-  qfSmmyIdx[c('AlphaQF', 'BetaQF', 'FinalQF')]
    
  } # End loops around alpha/beta/final QF groups
  
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

    
    # Allocate qf points to aggregation windows
    setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
    
    # Run through the time bins
    for(idxWndwTime in base::unique(setTime)){
      
      # Rows to pull
      setWndwTime <- setTime==idxWndwTime
      qfWndwTime <- qf[setWndwTime,] 
      qfSmmyWndwTime <- base::lapply(qfSmmy,FUN=function(qfSmmyIdx){
        return(qfSmmyIdx[setWndwTime,])
      })
      
      # Compute quality metrics (percent) for all quality flags
      numRow <- base::nrow(qfWndwTime)
      rpt[idxWndwTime,setQm+3] <- base::colSums(x=qfWndwTime==0,na.rm=TRUE)/numRow*100 # Pass
      rpt[idxWndwTime,setQm+4] <- base::colSums(x=qfWndwTime==1,na.rm=TRUE)/numRow*100 # Fail
      rpt[idxWndwTime,setQm+5] <- base::colSums(x=qfWndwTime==-1,na.rm=TRUE)/numRow*100 # Na
      
      # Compute alpha & beta QMs and the final quality flag for indicated groups
      qmFinlIdx <- base::lapply(qfSmmyWndwTime,NEONprocIS.qaqc::def.qm.smmy,Thsh=Thsh,WghtAlphBeta=WghtAlphBeta,log=log)

      rpt[idxWndwTime,nameQmGrp]<-base::unlist(qmFinlIdx)
      
      
    } # End loop through time windows
    
    # Write out the file for this aggregation interval. Replace the final underscore-delimited component of one of the input 
    # filenames, assuming this final component denotes the type of flags that are in the file
    for(idxQfFinl in setQfFinl){
      base::class(rpt[[idxQfFinl]]) <- "integer" # Make the final quality flags integer class
    }
    fileQmOutSplt <- base::strsplit(utils::tail(fileQf,1),'[_]')[[1]] # Separate underscore-delimited components of the file name
    tmi <- base::gsub(
            pattern=' ',
            replacement='0',
            x=base::format(
              base::as.character(WndwAgr[idxWndwAgr]),
              width=3,
              justify='right'))
    fileQmOutSplt[base::length(fileQmOutSplt)] <- base::paste(base::paste('qualityMetrics',tmi,sep='_'),utils::tail(x=base::strsplit(utils::tail(x=fileQmOutSplt,n=1),'[.]')[[1]],n=-1),sep='.') # Replace last component, but try to keep the extension
    fileQmOut <- base::paste(fileQmOutSplt,collapse='_')
    NameFileOutQm <- base::paste0(dirOutQm,'/',fileQmOut)
    
    rptWrte <- base::try(NEONprocIS.base::def.wrte.parq(data=rpt,NameFile=NameFileOutQm,NameFileSchm=NULL,Schm=SchmQm),silent=TRUE)
    if(base::class(rptWrte) == 'try-error'){
      log$error(base::paste0('Cannot write quality metrics file ', NameFileOutQm,'. ',attr(rptWrte,"condition"))) 
      stop()
    } else {
      log$info(base::paste0('Quality metrics written successfully in file: ',NameFileOutQm))
    }
    
  } # End loop around aggregation intervals
  
}
