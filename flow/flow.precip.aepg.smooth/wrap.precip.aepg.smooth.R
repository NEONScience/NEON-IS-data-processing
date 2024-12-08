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
#' @param ChangeFactorEvap Numeric fraction by which Envelope is multiplied to determine when a negative change in depth is considered evaporation, and the benchmark moved down accordingly. Default is 0.5
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
                                    SchmData=NULL,
                                    # WndwAgr = '5 min',
                                    # RangeSizeHour = 24, #  Period of evaluation (e.g. 24 for 1 day)
                                    # Envelope = 3,
                                    # ThshCountHour = 15,
                                    # Quant = 0.5, # Where is the benchmark set (quantile) within the envelope (diel variation)
                                    # ThshChange = 0.2,
                                    # ChangeFactor = 1,
                                    # ChangeFactorEvap = 0.5,
                                    # Recharge = 20, #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
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
  dirThsh <- base::paste0(DirIn,'/threshold')
  fileThsh <- base::dir(dirThsh)
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
  
  # Read in the thresholds file (read first file only, there should only be 1)
  if(base::length(fileThsh) > 1){
    fileThsh <- fileThsh[1]
    log$info(base::paste0('There is more than one threshold file in ',dirThsh,'. Using ',fileThsh))
  }
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df((NameFile=base::paste0(dirThsh,'/',fileThsh)))
  
  # !!!!!!!!!!!! LOTS OF FUDGING HERE FOR TESTING. CORRECT WHEN FINAL THRESHOLDS ARE CREATED !!!!!!!!!!!!!!!!!!!
  # Verify that the term(s) needed in the input parameters are included in the threshold files
  termTest <- "priPrecipBulk"
  exstThsh <- termTest %in% base::unique(thsh$term_name) # Do the terms exist in the thresholds
  if(base::sum(exstThsh) != base::length(termTest)){
    log$error(base::paste0('Thresholds for term(s): ',base::paste(termTest[!exstThsh],collapse=','),' do not exist in the thresholds file. Cannot proceed.')) 
    stop()
  }
  # Assign thresholds
  thshIdxTerm <- thsh[thsh$term_name == termTest,]

  WndwAgr = thshIdxTerm$string_value[thshIdxTerm$threshold_name == 'Despiking Method']
  RangeSizeHour = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Step Test value']
  Envelope = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking maximum (%) missing points per window']
  ThshCountHour = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Range Threshold Hard Max']
  Quant = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking window size - points']
  ThshChange = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking maximum consecutive points (n)']
  ChangeFactor = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking MAD']
  ChangeFactorEvap = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking window step - points.']
  Recharge = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Range Threshold Soft Max']
  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  
  
  
  
  
  # Take stock of our data files.
  # !! Try to make more generic, while excluding the manifest.txt file
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  

  
  
  # -------------- BEGIN EXPERIMENTAL ---------------
  if(FALSE){
    
    # Attempt a daily fit, testing for high enough R2 and positive slope
    # Note - could also apply this to the envelope, falling back on the pre-defined threshold if day(s) without rain cannot be determined
    setNoRain <- NULL
    timeDay <- lubridate::floor_date(as.POSIXct(data$readout_time, tz = 'UTC'),unit='day')
    tempRegr <- data.frame(day=unique(timeDay),
                           slopeTemp1=as.numeric(NA),
                           rsq1=as.numeric(NA),
                           slopeTemp2=as.numeric(NA),
                           rsq2=as.numeric(NA),
                           slopeTemp3=as.numeric(NA),
                           rsq3=as.numeric(NA)
    )
    for (idxDay in unique(timeDay)){
      try({
        setDay <- timeDay == idxDay # row indices for this day
        fit1 <- lm(strainGauge1Depth ~ strain_gauge1_temperature,data[setDay,],na.action="na.omit")
        fit2 <- lm(strainGauge2Depth ~ strain_gauge2_temperature,data[setDay,],na.action="na.omit")
        fit3 <- lm(strainGauge3Depth ~ strain_gauge3_temperature,data[setDay,],na.action="na.omit")
        
        idxOut <- tempRegr$day == idxDay
        tempRegr$slopeTemp1[idxOut] <- fit1$coefficients[2]
        tempRegr$rsq1[idxOut] <- summary(fit1)$r.squared
        tempRegr$slopeTemp2[idxOut] <- fit2$coefficients[2]
        tempRegr$rsq2[idxOut] <- summary(fit2)$r.squared
        tempRegr$slopeTemp3[idxOut] <- fit3$coefficients[2]
        tempRegr$rsq3[idxOut] <- summary(fit3)$r.squared
      })
    }
    
    # Keep only the excellent regressions
    rsq_min <- 0.9 # minimum R-squared to accept regression
    tempRegrKeep <- tempRegr
    tempRegrKeep$slopeTemp1[tempRegrKeep$rsq1 < rsq_min] <- NA
    tempRegrKeep$slopeTemp2[tempRegrKeep$rsq2 < rsq_min] <- NA
    tempRegrKeep$slopeTemp3[tempRegrKeep$rsq3 < rsq_min] <- NA
    
    # Also require that there be no change in depth between the first and last hours of the day 
    # that is greater than the pre-defined envelope
    dataHourly <- data %>%
      dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = 'hour')) %>%
      dplyr::group_by(startDateTime) %>%
      dplyr::summarise(strainGauge1Depth = median(strainGauge1Depth, na.rm = T),
                       strainGauge2Depth = median(strainGauge2Depth, na.rm = T),
                       strainGauge3Depth = median(strainGauge3Depth, na.rm = T))
    dataDaily <- dataHourly %>%
      dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'day')) %>%
      dplyr::group_by(startDateTime) %>%
      dplyr::summarise(strainGauge1DepthChg = tail(strainGauge1Depth,1)-head(strainGauge1Depth,1),
                       strainGauge2DepthChg = tail(strainGauge2Depth,1)-head(strainGauge2Depth,1),
                       strainGauge3DepthChg = tail(strainGauge3Depth,1)-head(strainGauge3Depth,1))
    tempRegrKeep$slopeTemp1[is.na(dataDaily$strainGauge1DepthChg) | abs(dataDaily$strainGauge1DepthChg) > Envelope] <- NA
    tempRegrKeep$slopeTemp2[is.na(dataDaily$strainGauge2DepthChg) | abs(dataDaily$strainGauge2DepthChg) > Envelope] <- NA
    tempRegrKeep$slopeTemp3[is.na(dataDaily$strainGauge3DepthChg) | abs(dataDaily$strainGauge3DepthChg) > Envelope] <- NA  
    
    # Use the non-rain days identified by the sensor with the most accepted regressions
    # Remove the temperature relationship
    numDaysNoRain <- colSums(!is.na(tempRegrKeep))[c('slopeTemp1','slopeTemp2','slopeTemp3')]
    if (any(numDaysNoRain > 0)){
      
      idxSensMax <- which.max(numDaysNoRain) # strain gauge to use
      setNoRain <- !is.na(tempRegrKeep[[paste0('slopeTemp',idxSensMax)]]) # no-rain days
      
      # Average the slopes for the no-rain days for each sensor
      # tempRegrNoRain <- colMeans(tempRegr[setNoRain,-1])
      
      # Average the slopes that meet the threshold R-squared
      tempRegrNoRain <- colMeans(tempRegrKeep[,-1],na.rm=TRUE)
      
      # Remove the temp relationship
      if(!is.na(tempRegrNoRain["slopeTemp1"])){
        data$strainGauge1Depth <- data$strainGauge1Depth - tempRegrNoRain["slopeTemp1"]*data$strain_gauge1_temperature
      }
      if(!is.na(tempRegrNoRain["slopeTemp2"])){
        data$strainGauge2Depth <- data$strainGauge2Depth - tempRegrNoRain["slopeTemp2"]*data$strain_gauge2_temperature
      }
      if(!is.na(tempRegrNoRain["slopeTemp3"])){
        data$strainGauge3Depth <- data$strainGauge3Depth - tempRegrNoRain["slopeTemp3"]*data$strain_gauge3_temperature
      }
      
    }
    
  }
  
  # -------------- END EXPERIMENTAL ---------------
  

  
  
  
  # Aggregate depth streams into a single depth. 
  data <- data %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, strainGauge2Depth, strainGauge3Depth), na.rm = F))  
  
  # Do time averaging
  strainGaugeDepthAgr <- data %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
    dplyr::group_by(startDateTime,endDateTime) %>%
    dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T),
                     strainGauge1DepthMean = mean(strainGauge1Depth, na.rm = T),
                     strainGauge2DepthMean = mean(strainGauge2Depth, na.rm = T),
                     strainGauge3DepthMean = mean(strainGauge3Depth, na.rm = T))
  
  
  
  # -------------- BEGIN EXPERIMENTAL ---------------
  
  # Use the average strain gauge depth to recompute days without rain
  dataHourly <- strainGaugeDepthAgr %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'hour')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepth = median(strainGaugeDepth, na.rm = T))
  dataDaily <- dataHourly %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'day')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepthChg = tail(strainGaugeDepth,1)-head(strainGaugeDepth,1))
  setNoRain <- (dataDaily$strainGaugeDepthChg < 0.25*Envelope) & (dataDaily$strainGaugeDepthChg > -1*Envelope)
  setNoRain[is.na(setNoRain)] <- FALSE
  
  # Recompute the envelope if we have determined days without rain
  if(any(setNoRain)){
    dayNoRain <- dataDaily$startDateTime[setNoRain]
    timeDay <- lubridate::floor_date(strainGaugeDepthAgr$startDateTime,unit='day')
    
    envelopeComp <- data.frame(day=unique(timeDay),envelope=as.numeric(NA))
    
    for (idxDay in unique(timeDay)){
      if(!(idxDay %in% dayNoRain)){
        next
      }
      setDay <- timeDay == idxDay # row indices for this day
      envelopeIdx <- max(strainGaugeDepthAgr$strainGaugeDepth[setDay],na.rm=TRUE)-min(strainGaugeDepthAgr$strainGaugeDepth[setDay],na.rm=TRUE)
      envelopeComp$envelope[envelopeComp$day == idxDay] <- envelopeIdx
    }
    
    # Take the max envelope
    envelopeMax<- max(envelopeComp$envelope,na.rm=TRUE)
    if(!is.na(envelopeMax)){
      Envelope <- envelopeMax
    }
    
    # if Envelope is larger than Recharge threshold adjust recharge. 
    if(Envelope > Recharge){
      Recharge <- 1.25*Envelope
    }
  }
  
  # -------------- END EXPERIMENTAL ---------------
  
  
  # !!!! Do/add summarization of stability, temp stuff, flags (in different data frame) !!!!
  
  #adjust thresholds based on WndwAgr unit
  WndwAgrNumc <- as.numeric(stringr::str_extract(string = WndwAgr, pattern = '[0-9]+'))
  if(stringr::str_detect(WndwAgr, 'min')) {
    ThshCount <- ThshCountHour * (60/WndwAgrNumc) 
    rangeSize <- RangeSizeHour*(60/WndwAgrNumc)   #!!! POTENTIAL FOR MAKING AN INPUT VARIABLE !!!
  } else if ((stringr::str_detect(WndwAgr, 'hour')) ){
    ThshCount <- ThshCountHour/WndwAgrNumc
    rangeSize <- RangeSizeHour/WndwAgrNumc #account for evap in last 24 hours
  } else {
    log$fatal('averaging unit needs to be in minutes (min) or hours (hour)')
    stop()
  }
  
  #start counters
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day
  
  #initialize fields
  strainGaugeDepthAgr$bench <- as.numeric(NA)
  strainGaugeDepthAgr$precip <- FALSE #add TRUE when rain detected
  strainGaugeDepthAgr$precipType <- as.character(NA)
  
  #!!! Needs logic for NA start
  strainGaugeDepthAgr$bench[1:currRow] <-  stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[1:currRow],Quant,na.rm=TRUE)
  
  ##loop through data to establish benchmarks 
  skipping <- FALSE
  numRow <- nrow(strainGaugeDepthAgr)
  for (i in 1:numRow){
    
    #if(currRow == 865){stop()} 
    
    # Check for at least 1/2 a day of non-NA values. 
    # If not, get to the next point at which we have at least 1/2 day and start fresh
    if(base::sum(base::is.na(strainGaugeDepthAgr$strainGaugeDepth[i:currRow])) > .5*rangeSize){
      
      # Find the last non-NA value 
      setEval <- i:currRow
      idxEnd <- setEval[tail(which(!is.na(strainGaugeDepthAgr$strainGaugeDepth[setEval])),1)]
      
      if(length(idxEnd) > 0){
        # Remove the benchmark extending into the gap
        strainGaugeDepthAgr$bench[(idxEnd+1):currRow] <- NA
      }
      
      # Skip until there is enough data
      skipping <- TRUE
      currRow <- currRow + 1

      #stop at end of data frame
      if (currRow == numRow){
        break()
      } else {
        next
      }
      
    } else if (skipping) {
      
      # Find the first non-NA value to begin at
      setEval <- i:currRow
      idxBgnNext <- setEval[head(which(!is.na(strainGaugeDepthAgr$strainGaugeDepth[setEval])),1)]
      
      # Re-establish the benchmark
      strainGaugeDepthAgr$bench[idxBgnNext:currRow] <- stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[idxBgnNext:currRow],Quant,na.rm=TRUE)
      timeSincePrecip <- NA
      skipping <- FALSE
    }
    
    
    if(!is.na(timeSincePrecip)){
      timeSincePrecip <- timeSincePrecip + 1
    }
    
    # #establish nth min/max of range
    recentPrecip <- base::any(strainGaugeDepthAgr$precip[i:currRow])
    

    #establish benchmark
    bench <- strainGaugeDepthAgr$bench[currRow-1]
    raw <- strainGaugeDepthAgr$strainGaugeDepth[currRow]
    precip <- FALSE
    precipType <- NA
    
    # missing data handling
    # !!!! THIS NEEDS ATTENTION. If the initial benchmark creation was NA then this may still be a problem. 
    # If the current depth value is NA, set it to the benchmark
    raw <- ifelse(is.na(raw), bench, raw) 
    
    #how many measurements in a row has raw been >= bench?
    if (raw >= bench){
      rawCount <- rawCount + 1
    }else{
      rawCount <- 0
    }
    
    # Compute median over last range size (i.e. 1 day)
    # !!! Write a note about what we're going to use this for. !!!
    raw_med_lastDay <- quantile(strainGaugeDepthAgr$strainGaugeDepth[i:currRow],Quant,na.rm=TRUE)
    raw_min_lastDay <- min(strainGaugeDepthAgr$strainGaugeDepth[i:currRow],na.rm=TRUE)
    
    
    # if precip total increased check to if any precip triggers are reached
    if (raw > bench){
      rawChange <- raw - bench
      
      if ((rawChange > (ChangeFactor * Envelope) & rawChange > ThshChange )){ 
        # If change was bigger than 90% of diel range of noise in the data and also 
        #   greater than the expected instrument sensitivity to rain, then it rained!
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE 
        timeSincePrecip <- 0
        precipType <- 'volumeThresh'
        # SHOULD WE RESET THE rawCount HERE???
        # rawCount <- 0
        
      } else if (grepl('volumeThresh',x=strainGaugeDepthAgr$precipType[currRow-1]) && rawChange > ThshChange){
        # Or, if is has been raining with the volume threshold and the precip depth continues to increase 
        #   above the expected instrument sensitivity, continue to say it is raining.
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'volumeThreshContinued'
        # SHOULD WE RESET THE rawCount HERE???
        # rawCount <- 0
        
      } else if (rawCount == ThshCount){
        
        # Or, if the precip depth has been above the benchmark for exactly the time threshold 
        #   considered for drizzle (ThshCount), say that it rained (i.e. drizzle), and 
        #   continue to count.
        bench <- raw # Update the benchmark for the next data point
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
        
        # Now go back to the start of the drizzle and set the bench to increasing 
        #   raw values and continue to count.
        benchNext <- bench
        for (idx in (currRow-1):(currRow-ThshCount+2)) {
          rawIdx <- strainGaugeDepthAgr$strainGaugeDepth[idx]
          rawIdx <- ifelse(is.na(rawIdx), benchNext, rawIdx) 
          if(rawIdx < benchNext){
            benchNext <- rawIdx
            strainGaugeDepthAgr$bench[idx] <- rawIdx
          } else {
            strainGaugeDepthAgr$bench[idx] <- benchNext
          }
          
          # Record rain stats
          strainGaugeDepthAgr$precip[idx] <- precip
          strainGaugeDepthAgr$precipType[idx] <- 'ThshCountBackFilledToStart'
        }
        
      } else if (rawCount >= ThshCount){
        # Or, if it continues to drizzle and raw is continuing to rise, keep saying that it's raining
        bench <- raw 
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
        
      }
        
    } else if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (strainGaugeDepthAgr$bench[i-1]-Recharge)){  # Maybe use Envelope instead of Recharge?
      # Exactly one day after rain ends, and if the depth hasn't dropped precipitously (as defined by the Recharge threshold),
      # back-adjust the benchmark to the Quant of the last day to avoid overestimating actual precip
      # Under heavy evaporation, this has the effect of removing spurious precip, potentially also small real precip events
      bench <- raw_med_lastDay
      strainGaugeDepthAgr$bench[i:currRow] <- bench
      strainGaugeDepthAgr$precipType[i:currRow] <- "postPrecipAdjToMedNextDay"

      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE) {

        if(is.na(strainGaugeDepthAgr$precip[idxBgn]) || strainGaugeDepthAgr$precip[idxBgn] == FALSE){
          # Stop if we are past the point where the precip started
          keepGoing <- FALSE
        } else if(strainGaugeDepthAgr$bench[idxBgn] > bench){
          strainGaugeDepthAgr$bench[idxBgn] <- bench
          strainGaugeDepthAgr$precipType[idxBgn] <- paste0(strainGaugeDepthAgr$precipType[idxBgn],"BackAdjToMedNextDay")
          idxBgn <- idxBgn - 1
        } else {
          keepGoing <- FALSE
        }
      }
    } else if ((bench-raw_med_lastDay) > ChangeFactorEvap*Envelope && !recentPrecip){
      # If it hasn't rained in at least 1 day, check for evaporation & reset benchmark if necessary
      # bench <- raw_med_lastDay
      bench <- raw_min_lastDay # Set to the min of the last day to better track evap
      precipType <- 'EvapAdj'
      
    } else if ((bench - raw) > Recharge){
      # If the raw depth has dropped precipitously (as defined by the recharge rage), assume bucket was emptied. Reset benchmark.
      bench <- raw
      
      # Get rid of a couple hours before the recharge. This is when calibrations are occuring and strain gauges are being replaced.
      # Set the benchmark constant to the point 2 hours before the recharge
      setAdj <- strainGaugeDepthAgr$startDateTime > (strainGaugeDepthAgr$startDateTime[currRow] -as.difftime(3,units='hours')) &
        strainGaugeDepthAgr$startDateTime < strainGaugeDepthAgr$startDateTime[currRow]
      idxSet <- head(which(setAdj),1) - 1
      if (idxSet < 1){
        strainGaugeDepthAgr$bench[setAdj] <- as.numeric(NA)
        strainGaugeDepthAgr$precip[setAdj] <- as.logical(NA)
        strainGaugeDepthAgr$precipType[setAdj] <- as.character(NA)
      } else {
        strainGaugeDepthAgr$bench[setAdj] <- strainGaugeDepthAgr$bench[idxSet]
        strainGaugeDepthAgr$precip[setAdj] <- strainGaugeDepthAgr$precip[idxSet]
        strainGaugeDepthAgr$precipType[setAdj] <- "ExcludeBeforeRecharge"
        # !!!!!!!!!! CREATE INFORMATIONAL FLAG !!!!!!!!!!!
      }

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

  # Compute precip
  strainGaugeDepthAgr$precipBulk <- as.numeric(strainGaugeDepthAgr$bench - lag(strainGaugeDepthAgr$bench, 1))
  strainGaugeDepthAgr <- strainGaugeDepthAgr %>% mutate(precipBulk = ifelse(precipBulk < 0, 0, precipBulk))

  # For the central day and adjacent days on either side of the central day, output a file with the results. A later module will average output for each day
  dayOut <- InfoDirIn$time+as.difftime(c(-1,0,1),units='days')
  
  for(idxDayOut in seq_len(length(dayOut))){
    
    dayOutIdx <- dayOut[idxDayOut]
    # Get the records for this date
    setOut <- strainGaugeDepthAgr$startDateTime >= dayOutIdx & 
      strainGaugeDepthAgr$startDateTime < (dayOutIdx + as.difftime(1,units='days'))
    strainGaugeDepthAgrIdx <- strainGaugeDepthAgr[setOut,]
    
    # Replace the date in the output path structure with the current date
    dirOutDataIdx <- sub(pattern=format(InfoDirIn$time,'%Y/%m/%d'),
                         replacement=format(dayOutIdx,'%Y/%m/%d'),
                         x=dirOutData,
                         fixed=TRUE)
    base::dir.create(dirOutDataIdx,recursive = TRUE)

    # Get the filename for this day
    nameFileIdx <- fileData[grepl(format(dayOutIdx,'%Y-%m-%d'),fileData)][1]

    if(!is.na(nameFileIdx)){
      # Append the center date to the end of the file name to know where it came from
      nameFileIdxSplt <- base::strsplit(nameFileIdx,'.',fixed=TRUE)[[1]]
      nameFileIdxSplt <- c(paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt)-1)],
                                  '_from_',
                                  format(InfoDirIn$time,'%Y-%m-%d')),
                           utils::tail(nameFileIdxSplt,1))
      nameFileOutIdx <- base::paste0(nameFileIdxSplt,collapse='.')
      
      # Write out the data to file
      fileOutIdx <- fs::path(dirOutDataIdx,nameFileOutIdx)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = strainGaugeDepthAgrIdx,
          NameFile = fileOutIdx,
          NameFileSchm=NULL,
          Schm=SchmData,
          log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileOutIdx,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote computed precipitation to file ',
          fileOutIdx
        ))
      }
      
    } else {
      log$warn(paste0(nameFileIdx,' is not able to be output because this data file was not found in the input.'))
    }
    
  }
  
  
  # # Take stock of our flags files. 
  # fileFlags<- base::list.files(dirInFlags,full.names=FALSE)
  # 
  # flags <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInFlags,fileFlags),
  #                                            VarTime='readout_time',
  #                                            RmvDupl=TRUE,
  #                                            Df=TRUE, 
  #                                            log=log)
  # 
  
  
  return()
} 
