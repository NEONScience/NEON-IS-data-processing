##############################################################################################
#' @title Basic statistics and uncertainty module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Basic statistics module for NEON IS data processing. Computes one or more
#' of the following statistics: mean, median, minimum, maximum, sum, variance, standard 
#' deviation, standard error, number of points, skewness, kurtosis, median absolute deviation (mad), 
#' expanded uncertainty
#' 
#' General code workflow:
#'      Error-check input parameters
#'      Create output time sequence for each aggregation interval
#'      Read in and uncertainty coefficients and uncertainty data, if applicable 
#'      For each aggregation interval:
#'         For each time bin of each aggregation interval:
#'            Compute the desired statistics for each term
#'         Write the output file for the aggregation inteval
#'
#' @param DirIn Character value. The path to parent directory where the flags exist. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are (at a minimum) the folder:
#'         /data 
#'         
#' The data folder holds any number of daily data files for which statistics will be computed. If expUncert
#' is output (see options in ParaStat), information in folders 'uncertainty_coef' and/or 'uncertainty_data' 
#' will be passed into the specified uncertainty function. Note that there can only be one file in the 
#' uncertainty_data directory, so it must contain all applicable uncertainty data.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param WndwAgr Difftime vector. The aggregation interval(s) for which to compute statistics. 
#' Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the expected length of the input data is one day. 
#' The aggregation interval must be an equal divisor of one day. 
#' 
#' @param ParaStat A list of lists, one sublist for each term for which to compute statistics. Each sublist contains 
#' the following named elements:
#' \code{term}: Character value. The term for which to compute statistics. \cr
#' \code{stat}: Character vector. The exact names of the statistics to compute for the term. Statistic options are 
#' (exact names): 
#' mean, median, minimum, maximum, sum, variance, stdDev, stdEr, numPts, expUncert, skewness, kurtosis, mad
#' \code{funcUcrt}: Character value (optional). The name of the function in the NEONprocIS.stat package 
#' to compute the expanded uncertainty statistic (expUncert). Look in the NEONprocIS.stat package for 
#' available functions to use, or create your own so long as it accepts the same inputs and outputs data 
#' in the same format. Note that any uncertainty coefficients and/or  L0' uncertainty data in the uncertainty_coef 
#' and uncertainty_data folders, respectively, of DirIn will be passed into the uncertainty function for use there. 
#' If expUncert is not in the stat list for the term, this list element may be omitted. 
#' 
#' @param SchmStat (Optional).  A json-formatted character string containing the schema for the output statistics
#' file. If this input is not provided, the output schema for the will be auto-generated from  
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE TERMS 
#' IN THE ParaStat ARGUMENT (stats nested within term). See output information below for details. 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return 
#' Statistics for each aggregation interval output in Parquet format in DirOutBase, where the terminal 
#' directory of DirOutBase replaces BASE_REPO but otherwise retains the child directory structure of the input 
#' path. Directory 'stats' will automatically populated in the output directory, where the files 
#' for each aggregation interval will be placed. Any other folders specified in argument DirSubCopy will be 
#' copied over unmodified with a symbolic link.
#' If no output schema is provided for the statistics, the variable names will be a combination of 
#' the term and statistic, in the order they were provided in ParaStat. Additionally, the first two columns 
#' of the stats file will contain the start and end times for the aggregation interval, labeled 
#' "startDateTime" and "endDateTime", respectively. 
#' The statistics are calculated for readout times in the interval [startDateTime endDateTime), with an open 
#' brack on the right (i.e. inclusive of the startDateTime but exclusive of the endDateTime). The remaining 
#' columns present the chosen statistics for each term. For example, if 
#' ParaStat = list(temp=list(term="temp",stat=c("mean","median")),pres=list(term="pres",stat=c("minimum","maximum"))), 
#' the output columns will be startDateTime, endDateTime, tempMean, tempMedian,presMinimum, presMaximum, in that order. 
#' The names of the output columns may be replaced by providing an output 
#' schema in argument FileSchmStat. However, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE 
#' ORDERING OF THE INPUT ARGUMENTS. Otherwise, column names will not pertain to the statistics in the column.
#' The best way to ensure that the output schema matches the expected column ordering is to do a trial run 
#' without inputting an output schema. The default output column names will be used, which can then serve as a 
#' guide for crafting the output schema.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaStat = list(
#'                 temp=list(
#'                           term="temp",
#'                           stat=c("mean","median","expUncert"),
#'                           funcUcrt="wrap.ucrt.dp01.cal.cnst.fdas.rstc"
#'                           ),
#'                 pres=list(
#'                           term="pres",
#'                           stat=c("minimum","maximum")
#'                           )
#'                 )
#' wrap.stat.basc(DirIn="/pfs/tempSoil_pre_statistics_group/prt/2020/01/02/CFGLOC12345",
#'                DirOutBase="/pfs/out",
#'                WndwAgr=as.difftime(c(1,30),units='mins'),
#'                ParaStat=ParaStat
#'                )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2022-06-16)
#     Convert flow script to wrapper function
##############################################################################################
wrap.stat.basc <- function(DirIn,
                         DirOutBase,
                         WndwAgr,
                         ParaStat,
                         SchmStat=NULL,
                         DirSubCopy=NULL,
                         log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
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
  
  # Pull out all terms and stats we will compute
  termComp <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$term}))

  # Reorganize the parameters to be stat-focused rather than term-focused (i.e. terms nested within each stat)
  # This is because it's more efficient to compute each statistic for all relevant terms at the same time, but is awkward 
  # to have to input and output them this way
  statTerm <- lapply(stat,FUN=function(idxStat){
    base::unlist(lapply(ParaStat,FUN=function(idxTerm){
      if(idxStat %in% idxTerm$stat){
        return(idxTerm$term)
      }
    }))
  })
  names(statTerm) <- stat
  
  # Create the default column names for the stat-focused parameters
  nameColStatTerm <- lapply(stat,FUN=function(idxStat){
    base::unlist(lapply(ParaStat,FUN=function(idxTerm){
      if(idxStat %in% idxTerm$stat){
        return(
          base::paste0(
            idxTerm$term,
            base::paste(
              base::toupper(base::substr(idxStat,1,1)),
              base::substr(idxStat,2,base::nchar(idxStat)),
              sep="")
            )
          )
      }
    }))
  })
  names(nameColStatTerm) <- stat
  
  # Compile the full set of output statistics, in the order in which they will be output (for initializing the output)
  nameColOut <- base::unlist(base::lapply(
    ParaStat,FUN=function(idx){
      base::paste0(idx$term,
                  base::paste(
                    base::toupper(
                      base::substr(idx$stat,1,1)),
                    base::substr(idx$stat,
                                 2,
                                 base::nchar(idx$stat)),
                    sep=""
                    )
      )
      }
    )
  )

  # Put the uncertainty functions into a data frame
  FuncUcrt <- base::do.call(base::rbind,
                            base::lapply(ParaStat,
                                         FUN=function(idx){
                                           if(!base::is.null(idx$funcUcrt)){
                                             return(base::data.frame(var=idx$term,
                                                                     FuncUcrt=idx$funcUcrt,
                                                                     stringsAsFactors=FALSE))
                                           }else{
                                             return(NULL)
                                           }
                                         }
                            )
  ) 
  
  # Create the binning for each aggregation interval
  timeBgnDiff <- list()
  timeEndDiff <- list()
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],
                                                      WndwTime=base::as.difftime(1,units='days'),
                                                      log=log)
    
    timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  } # End loop around aggregation intervals
  
  # Get directory listing of input directory. Expect subdirectories for data
  dirData <- base::paste0(DirIn,'/data')
  fileData <- base::sort(base::dir(dirData))
  log$info(base::paste0('Computing statistics for ', base::length(fileData),' input files (separately).'))
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  dirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  dirOutStat <- base::paste0(dirOut,'/stats')
  base::dir.create(dirOutStat,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    if('stats' %in% DirSubCopy){
      LnkSubObj <- TRUE
    } else {
      LnkSubObj <- FALSE
    }
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,'/',DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=TRUE,
                                       log=log)
  }  
  
  # Are we computing uncertainty? If so, load the uncertainty coefficients file (there should be only 1)
  ucrtData <- NULL
  if("expUncert" %in% stat){
    dirUcrtCoef <- base::paste0(DirIn,'/uncertainty_coef')
    
    fileUcrt <- base::dir(dirUcrtCoef)
    if(base::length(fileUcrt) != 1){
      log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ",dirUcrtCoef,"... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
      ucrtCoef <- base::list()
    } else {
      nameFileUcrt <- base::paste0(dirUcrtCoef,'/',fileUcrt) # Full path to file
      
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
    if('uncertainty_data' %in% base::dir(DirIn)){
      dirUcrtData <- base::paste0(DirIn,'/uncertainty_data')
      log$info(base::paste0('Detected uncertainty data folder',dirUcrtData,'.'))
      
      fileUcrtData <- base::dir(dirUcrtData)
      if(base::length(fileUcrtData) != 1){
        log$warn(base::paste0("There are either zero or more than one uncertainty data files in path: ",dirUcrtCoef,"... Uncertainty data will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
      } else {
        nameFileUcrtData <- base::paste0(dirUcrtData,'/',fileUcrtData) # Full path to file
        
        # Open the uncertainty data file
        ucrtData  <- base::try(NEONprocIS.base::def.read.parq(NameFile=nameFileUcrtData,log=log),silent=FALSE)
        if(base::any(base::class(ucrtData) == 'try-error')){
          log$error(base::paste0('File ', fileUcrtData,' is unreadable.')) 
          stop()
        } else {
          log$debug(base::paste0('Successfully read uncertainty data in file: ',fileUcrtData))
        }
        
      }
    } else {
      log$debug(base::paste0("No L0' uncertainty data detected in : ",DirIn))
    }# End if statement around uncertainty data
  } # End if statement around expUncert
  
  
  # Run through each data file
  for(idxFileData in fileData){
    
    # Load in data file in parquet format into data frame 'data'.  
    fileIn <- base::paste0(dirData,'/',idxFileData)
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
                                                'readout_time', termComp
                                              )),
                                              log = log)
    if (!valiData) {
      base::stop()
    }
    timeMeas <- base::as.POSIXlt(data$readout_time) # Pull out time variable
    
    # Choose whether to compute stats, and for which variables, before we head into the loop
    compSkew <- base::any(stat %in% c('skewness'))
    if(compSkew){
      termSkew <- base::unique(c(statTerm[['skewness']]))
    }
    compKurt <- base::any(stat %in% c('kurtosis'))
    if(compKurt){
      termKurt <- base::unique(c(statTerm[['kurtosis']]))
    }
    
    # Run through each aggregation interval
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      
      log$debug(base::paste0('Computing stats for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      
      # Intialize the output
      rpt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      rpt[,3:(base::length(nameColOut)+2)] <- base::as.numeric(NA)
      base::names(rpt)[3:(base::length(nameColOut)+2)] <- nameColOut
      
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
        vari <- base::apply(X=base::subset(dataWndwTime,select=termComp),
                            MARGIN=2,
                            FUN=stats::var,na.rm=TRUE)
        sd <- base::sqrt(vari[termComp])
        if(compSkew){
          sumDiffMean03 <- base::apply(X=base::subset(dataWndwTime,select=termSkew),
                                       MARGIN=2,
                                       FUN=function(data){
                                        base::sum((data-base::mean(data, na.rm=TRUE))^3, na.rm=TRUE)
                                         }
                                       )
        }
        if(compKurt){
          sumDiffMean04 <- base::apply(X=base::subset(dataWndwTime,select=termKurt),
                                       MARGIN=2,
                                       FUN=function(data){
                                        base::sum((data-base::mean(data, na.rm=TRUE))^4, na.rm=TRUE)
                                         }
                                       )
        }
        
        # Run through the stats
        for(idxStat in stat){
          
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
          rpt[idxWndwTime,nameColStatTerm[[idxStat]]] <- statIdx
          
        } # End loop through stats
        
      } # End loop through time windows
      
      # Fix the Inf/-Inf results for max/min, respectively, when there is no non-NA data
      rpt[rpt==Inf] <- NA
      rpt[rpt==-Inf] <- NA
      
      
      # Write out the file for this aggregation interval
      tmi <- base::gsub(
        pattern=' ',
        replacement='0',
        x=base::format(
          base::as.character(WndwAgr[idxWndwAgr]),
          width=3,
          justify='right'))
      NameFileOutStat <- 
        NEONprocIS.base::def.file.name.out(nameFileIn = idxFileData, 
                                           prfx = base::paste0(dirOutStat, '/'),
                                           sufx = base::paste0('_basicStats_',tmi)
        )
      rptWrte <- base::try(NEONprocIS.base::def.wrte.parq(data=rpt,
                                                          NameFile=NameFileOutStat,
                                                          NameFileSchm=NULL,
                                                          Schm=SchmStat),
                           silent=TRUE)
      if(base::class(rptWrte) == 'try-error'){
        log$error(base::paste0('Cannot write basic statistics file ', NameFileOutStat,'. ',attr(rptWrte,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Basic statistics written successfully in file: ',NameFileOutStat))
      }
      
      
    } # End loop around aggregation intervals
    
  } # End loop around files
  
  return()
  
} # End function

  