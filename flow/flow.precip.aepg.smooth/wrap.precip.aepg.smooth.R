##############################################################################################
#' @title Compute average depth of individual strain gauges, smooth, and compute precipitation for Belfort AEPG600m sensor

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Compute average depth related QC for 
#' Belfort AEPG600m sensor, then apply smoothing algorithm of the average depth over multiple days and
#' compute precipitation. 
#' 

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path is the folder:
#'         /data
#'         /threshold
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/li191r_data_source_kafka/li191r/2023/03/01/11346/data/ with nested file:
#'    li191r_11346_2023-03-05_13275082_13534222.parquet
#'    li191r_11346_2023-03-05_13534225_13534273.parquet
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param WndwAgr Difftime value. The aggregation interval for which to smooth the data prior to precip calc. 
#' 
#' @param ThshCountHour Numeric value. How many hours does precip need to be increasing to be considered valid?
#' 
#' @param Envelope Numeric value. Daily noise range of precipitation depth
#' 
#' @param ThshChange Numeric value. Expected sensitivity of instrument between individual points
#' 
#' @param ChangeFactor Numeric fraction by which Envelope is multiplied to determine if change in depth is precipitation, default is 0.9
#' 
#' @param Recharge Numeric value. If raw data drops by much or more, assume it is a bucket empty/recalibration

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the merged and filtered Kafka output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn <- '/scratch/pfs/li191r_data_source_kafka/li191r/2023/03/02/27733'
#' DirOutBase <- '/scratch/pfs/out'
#' FileSchmL0 <- '~/R/avro_schemas/schemas/li191r/li191r.avsc' # L0 schema
#' wrap.kfka.comb(DirIn,DirOutBase,FileSchmL0)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame & Cove Sturtevant (2024-06-25)
#     Initial creation
##############################################################################################
wrap.precip.aepg.smooth <- function(DirIn,
                                    DirOutBase,
                                    WndwAgr = '5 min',
                                    ThshCountHour = 6,
                                    Envelope = 3,
                                    ThshChange = 0.2,
                                    ChangeFactor = 0.9,
                                    Recharge = 250, #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
                                    DirSubCopy=NULL,
                                    log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirInFlags <- fs::path(DirIn,'flags')
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutData <- fs::path(dirOut,'data')
  dirOutFlags <- fs::path(dirOut,'flags')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('data','flags'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('data','flags')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # Take stock of our data files.
  # !! Try to make more generic, while excluding the manifest.txt file
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  

  # Aggregate depth streams into a single depth. 
  data <- data %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, strainGauge2Depth, strainGauge3Depth), na.rm = T))  
  
  # Do time averaging
  strainGaugeDepthAgr <- data %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
    dplyr::group_by(startDateTime,endDateTime) %>%
    dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T))
  
  # !!!! Do/add summarization of stability, temp stuff, flags (in different data frame) !!!!
  
  #adjust thresholds based on WndwAgr unit
  WndwAgrNumc <- as.numeric(stringr::str_extract(string = WndwAgr, pattern = '[0-9]+'))
  if(stringr::str_detect(WndwAgr, 'min')) {
    ThshCount <- ThshCountHour * (60/WndwAgrNumc) 
    rangeSize <- 24*(60/WndwAgrNumc)   #!!! POTENTIAL FOR MAKING AN INPUT VARIABLE !!!
  } else if ((stringr::str_detect(WndwAgr, 'hour')) ){
    ThshCount <- ThshCountHour/WndwAgrNumc
    rangeSize <- 24/WndwAgrNumc #account for evap in last 24 hours
  } else {
    log$fatal('averaging unit needs to be in minutes (min) or hours (hour)')
    stop()
  }
  
  #start counters
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day
  
  #initialize fields
  strainGaugeDepthAgr$bench <- NA
  strainGaugeDepthAgr$precip <- FALSE #add TRUE when rain detected
  strainGaugeDepthAgr$precipType <- NA
  
  #!!! Needs logic for NA start
  strainGaugeDepthAgr$bench[1:currRow] <-  stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[1:currRow],.8,na.rm=TRUE)
  
  ##loop through data to establish benchmarks 
  skipping <- FALSE
  for (i in 1:nrow(strainGaugeDepthAgr)){
    
    #if(currRow == 865){stop()} 
    
    # Check for at least 1/2 a day of non-NA values. 
    # If not, get to the next point at which we have at least 1/2 day and start fresh
    if(base::sum(base::is.na(strainGaugeDepthAgr$strainGaugeDepth[i:currRow])) > .5*rangeSize){
      skipping <- TRUE
      currRow <- currRow + 1
      next
    } else if (skipping) {
      # Re-establish the benchmark
      strainGaugeDepthAgr$bench[i:currRow] <- stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[i:currRow],.8,na.rm=TRUE)
      timeSincePrecip <- NA
      skipping <- FALSE
    }
    
    
    if(!is.na(timeSincePrecip)){
      timeSincePrecip <- timeSincePrecip + 1
    }
    
    # #establish nth min/max of range
    # nMin <- sort(strainGaugeDepthAgr$strainGaugeDepth[i:currRow], decreasing = FALSE)[nthVal]
    # nMax <- sort(strainGaugeDepthAgr$strainGaugeDepth[i:currRow], decreasing = TRUE)[nthVal]
    recentPrecip <- base::any(strainGaugeDepthAgr$precip[i:currRow])
    

    #establish benchmark
    bench <- strainGaugeDepthAgr$bench[currRow-1]
    raw <- strainGaugeDepthAgr$strainGaugeDepth[currRow]
    precip <- FALSE
    precipType <- NA
    
    # !!!! THIS NEEDS ATTENTION. 
    #missing data handling
    raw <- ifelse(is.na(raw), bench, raw) #if the initial benchmark creation was NA then this may still be a problem. 
    
    #how many measurements in a row has raw been >= bench?
    if (raw >= bench){
      rawCount <- rawCount + 1
    }else{
      rawCount <- 0
    }
    
    # Compute median over last range size (i.e. 1 day)
    raw_med_lastDay <- quantile(strainGaugeDepthAgr$strainGaugeDepth[i:currRow],.8,na.rm=TRUE)
    
    
    #if precip total increased check to see by how much
    if (raw > bench){
      rawChange <- raw - bench
      #if change was bigger than 90% of range of noise in the data or greater and > 0.2 mm  or if this is the ThshCount+1 change in a row, update benchmark
      #all values supplied by original algorithm
      if ((rawChange > (ChangeFactor * Envelope) & rawChange > ThshChange )){ #0.2 should probably be a threshold to check
        bench <- raw
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'volumeThresh'
      } else if (grepl('volumeThresh',x=strainGaugeDepthAgr$precipType[currRow-1]) && rawChange > ThshChange){
        bench <- raw
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'volumeThreshCont'
      } else if (rawCount == ThshCount){
        # For drizzle (triggering count threshold), go back to the start of the drizzle
        #   and set the bench to increasing raw values
        for (idx in (currRow-ThshCount+2):currRow) {
          rawIdx <- strainGaugeDepthAgr$strainGaugeDepth[idx]
          rawIdxMinus1 <- strainGaugeDepthAgr$strainGaugeDepth[idx-1]
          if(rawIdx > rawIdxMinus1){
            # We've gone up, increase the bench
            strainGaugeDepthAgr$bench[idx] <- rawIdx
          } else {
            # We've gone down, keep the bench the same as previous
            strainGaugeDepthAgr$bench[idx] <- rawIdxMinus1
          }
        }
      } else if ( rawCount >= ThshCount){
        bench <- raw 
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
        
      } else if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize){
        bench <- raw_med_lastDay
        
        strainGaugeDepthAgr$bench[i:currRow] <- bench
        
        # Set the top of the rain event to the median of the next day in order to 
        # remove the noise from the top of the rain event
        
        # !!!! TO-DO !!!!!
        # This doesn't seem to work well when the ThshCount is triggered (drizzle). 
        # !!!!!
        idxBgn <- i-1
        keepGoing <- TRUE
        while(keepGoing == TRUE && idxBgn >= (i-rangeSize+1)){
          if(strainGaugeDepthAgr$bench[idxBgn] > bench){
            strainGaugeDepthAgr$bench[idxBgn] <- bench
            idxBgn <- idxBgn - 1
          } else {
            keepGoing <- FALSE
          }
        }
      }
    # One day after rain ends, back-adjust the benchmark to the median of the last day to avoid overestimating actual precip
    } else if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize){  
      bench <- raw_med_lastDay
      strainGaugeDepthAgr$bench[i:currRow] <- bench
      
      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE && idxBgn >= (i-rangeSize+1)){
        #print(idxBgn)
        if(strainGaugeDepthAgr$bench[idxBgn] > bench){
          strainGaugeDepthAgr$bench[idxBgn] <- bench
          idxBgn <- idxBgn - 1
        } else {
          keepGoing <- FALSE
        }
      }    
    
      # If it hasn't rained in at least 1 day, check for evaporation & reset benchmark if necessary
    } else if ((-1*(raw_med_lastDay-bench) > (ChangeFactor * Envelope)) && !recentPrecip){
      bench <- raw_med_lastDay
      rawCount <- 0
      
      #next check if the difference is > a Recharge rate, I think this is for bucket emptying and restarting
      #TB adding code to account for precip empty
    } else if ((bench - raw) > Recharge){
      strainGaugeDepthAgr$strainGaugeDepth[currRow:nrow(strainGaugeDepthAgr)] <- strainGaugeDepthAgr$strainGaugeDepth[currRow:nrow(strainGaugeDepthAgr)]+bench 
      raw <- strainGaugeDepthAgr$strainGaugeDepth[currRow]
      bench <- raw
    } 
    
    #update in the data
    strainGaugeDepthAgr$bench[currRow] <- bench
    strainGaugeDepthAgr$precip[currRow] <- precip
    strainGaugeDepthAgr$precipType[currRow] <- precipType
    #move to next row
    currRow <- currRow + 1
    
    #stop at end of data frame
    if (currRow == nrow(strainGaugeDepthAgr)){
      strainGaugeDepthAgr$bench[currRow] <- bench
      break()
    }
  }

  # TESTING ONLY
df <- data.table::melt(strainGaugeDepthAgr[,c(1,3,4)],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')

  
  
  
  
  
  
  
  
  
  
  
  
  # Take stock of our flags files. 
  fileFlags<- base::list.files(dirInFlags,full.names=FALSE)
  
  flags <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInFlags,fileFlags),
                                             VarTime='readout_time',
                                             RmvDupl=TRUE,
                                             Df=TRUE, 
                                             log=log)
  
  
  # Write out the combined dataset to file
  fileOut <- fs::path(dirOutData,nameFileOut)

  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
        data = data,
        NameFile = fileOut,
        log=log
    ),
    silent = TRUE)
  if ('try-error' %in% base::class(rptWrte)) {
    log$error(base::paste0(
      'Cannot write output to ',
      fileOut,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0(
      'Combined kafka output written to file ',
      fileOut
      ))
  }

  return()
} 
