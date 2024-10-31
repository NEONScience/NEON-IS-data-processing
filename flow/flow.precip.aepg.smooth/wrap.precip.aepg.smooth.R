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
  Recharge = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Persistence (change)']
  ExtremePrecipMax = thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Range Threshold Soft Max']
  # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  
  # Adjust thresholds based on WndwAgr unit
  WndwAgrNumc <- as.numeric(stringr::str_extract(string = WndwAgr, pattern = '[0-9]+'))
  if(stringr::str_detect(WndwAgr, 'min')) {
    ThshCount <- ThshCountHour * (60/WndwAgrNumc) 
    rangeSize <- RangeSizeHour*(60/WndwAgrNumc)   
    
    if(WndwAgrNumc > 60 | WndwAgrNumc < 5){
      log$error('averaging unit must be between 5 minutes and one hour')
      stop()
    }
  } else if ((stringr::str_detect(WndwAgr, 'hour')) ){
    ThshCount <- ThshCountHour/WndwAgrNumc
    rangeSize <- RangeSizeHour/WndwAgrNumc
    
    if(WndwAgrNumc > 1 | WndwAgrNumc < (5/60)){
      log$error('averaging unit must be between 5 minutes and one hour')
      stop()
    }
  } else {
    log$error('averaging unit needs to be in minutes (min) or hours (hour)')
    stop()
  }
  
  # Take stock of our data files.
  # !! Try to make more generic, while excluding the manifest.txt file
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)
  fileFlagsPlau <- base::list.files(dirInFlags,pattern='Plausibility.parquet',full.names=FALSE)
  fileFlagsCal <- base::list.files(dirInFlags,pattern='Cal.parquet',full.names=FALSE)
  
  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  flagsPlau <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInFlags,fileFlagsPlau),
                                                 VarTime='readout_time',
                                                 RmvDupl=TRUE,
                                                 Df=TRUE,
                                                 log=log)
  
  flagsCal <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInFlags,fileFlagsCal),
                                                VarTime='readout_time',
                                                RmvDupl=TRUE,
                                                Df=TRUE,
                                                log=log)
  
  flags <- dplyr::full_join(flagsPlau, flagsCal, by =  'readout_time')
  
  
  #combine three gauges into one flagging variable. If any are 1 all flagged, any -1 all flagged, else not flagged
  
  flagNames <- names(flags)[grepl(unique(names(flags)), pattern = 'strainGauge')]
  
  flagNames <- unique(sub(pattern='strainGauge[1-3]Depth',replacement='',x=flagNames))
  
  qfs<- flags[, 'readout_time', drop = F]
  for (name in flagNames){
    flags_sub <- flags[,grepl(names(flags), pattern = name)]
    flagVar <- paste0('strainGaugeDepth', name)
    qfs[[flagVar]] <- NA
    flag_0 <- rowSums(flags_sub == 0, na.rm = T)
    qfs[[flagVar]][flag_0 == ncol(flags_sub)] <- 0
    flag_1 <- rowSums(flags_sub == 1, na.rm = T)
    qfs[[flagVar]][flag_1 >=1] <- 1
    flags_neg1 <- rowSums(flags_sub == -1, na.rm = T)
    qfs[[flagVar]][is.na(qfs[[flagVar]]) & flags_neg1 >=1] <- -1
    qfs[[flagVar]][is.na(qfs[[flagVar]])] <- -1
  }
  
  # Aggregate depth streams into a single depth, and remove values where not all three are available/good
  data$strain_gauge1_stability[is.na(data$strainGauge1Depth)] <- as.numeric(NA)
  data$strain_gauge2_stability[is.na(data$strainGauge2Depth)] <- as.numeric(NA)
  data$strain_gauge3_stability[is.na(data$strainGauge3Depth)] <- as.numeric(NA)
  data <- data %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, 
                                                                                 strainGauge2Depth, 
                                                                                 strainGauge3Depth), na.rm = F),
                                 strainGaugeStability = base::rowSums(x=base::cbind(strain_gauge1_stability, 
                                                                                    strain_gauge2_stability, 
                                                                                    strain_gauge3_stability), na.rm = F)==3)
  data$strainGaugeDepth[data$strainGaugeStability == FALSE] <- as.numeric(NA)
  
  # if there are no heater streams add them in as NA
  if(!('internal_temperature' %in% names(data))){data$internal_temperature <- as.numeric(NA)}
  if(!('inlet_temperature' %in% names(data))){data$inlet_temperature <- as.numeric(NA)}
  if(!('orifice_heater_flag' %in% names(data))){data$orifice_heater_flag <- as.numeric(NA)}
  

  # Do time averaging
  strainGaugeDepthAgr <- data %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
    dplyr::group_by(startDateTime,endDateTime) %>%
    dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T),
                     strainGaugeStability = dplyr::if_else(all(is.na(strainGaugeStability)),as.numeric(NA),all(strainGaugeStability==TRUE, na.rm = T)),
                     inletTemperature = mean(inlet_temperature, na.rm = T),
                     internalTemperature = mean(internal_temperature, na.rm = T), 
                     orificeHeaterFlag = max(orifice_heater_flag, na.rm = F), #used to see if heater was on when temps were above heating threshold (heaterErrorQF)
                     inletHeater1QM = round((length(which(orifice_heater_flag == 100))/dplyr::n())*100,0),
                     inletHeater2QM = round((length(which(orifice_heater_flag == 110))/dplyr::n())*100,0), 
                     inletHeater3QM = round((length(which(orifice_heater_flag == 111))/dplyr::n())*100,0),
                     inletHeaterNAQM = round((length(which(is.na(orifice_heater_flag)))/dplyr::n())*100,0)) 
  
  # Initialize time-aggregated flags
  flagsAgr <- strainGaugeDepthAgr %>% dplyr::select(startDateTime, endDateTime)
  flagsAgr$insuffDataQF <- 0
  flagsAgr$extremePrecipQF <- 0
  flagsAgr$dielNoiseQF <- 0
  flagsAgr$strainGaugeStabilityQF <- 0
  flagsAgr$strainGaugeStabilityQF[strainGaugeDepthAgr$strainGaugeStability == FALSE] <- 1 
  flagsAgr$heaterErrorQF <- 0
  flagsAgr$evapDetectedQF <- 0
  flagsAgr$heaterErrorQF[strainGaugeDepthAgr$internalTemperature > -6 & 
                           strainGaugeDepthAgr$internalTemperature < 2 & 
                           strainGaugeDepthAgr$inletTemperature < strainGaugeDepthAgr$internalTemperature] <- 1
  flagsAgr$heaterErrorQF[strainGaugeDepthAgr$internalTemperature > 6 & strainGaugeDepthAgr$orificeHeaterFlag > 0] <- 1
  flagsAgr$heaterErrorQF[is.na(strainGaugeDepthAgr$internalTemperature) | 
                         is.na(strainGaugeDepthAgr$inletTemperature) | 
                         is.na(strainGaugeDepthAgr$orificeHeaterFlag)] <- -1
  
  
  

  # Dynamic Envelope
  # Do computation of no-rain days in order to apply a dynamic envelope calculation
  # Require that there be no change in depth between the start and end of the day that is
  # greater than the pre-defined envelope set in the thresholds
  dataHourly <- strainGaugeDepthAgr %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'hour')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepthMed = median(strainGaugeDepth, na.rm = T),
                     strainGaugeDepthMin = min(strainGaugeDepth, na.rm = T), 
                     strainGaugeDepthMax = max(strainGaugeDepth, na.rm = T))
  dataDaily <- dataHourly %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'day')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepthChg = tail(strainGaugeDepthMax,1)-head(strainGaugeDepthMin,1))
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
    if(Envelope > Recharge/3){
      Recharge <- 3*Envelope
    }
  }
  
  
  #initialize fields
  strainGaugeDepthAgr$bench <- as.numeric(NA)
  strainGaugeDepthAgr$precip <- FALSE # TRUE when rain detected
  strainGaugeDepthAgr$precipType <- as.character(NA)
  strainGaugeDepthAgr$bench <- as.numeric(NA)
  
  # Initialize & pre-allocate surrogate data for uncertainty analysis
  numRow <- nrow(strainGaugeDepthAgr)
  nSurr <- 30
  surr <- matrix(as.numeric(NA),nrow=numRow,ncol=nSurr)
  nameVarDepthS <- paste0('strainGaugeDepthS',seq_len(nSurr))
  nameVarBenchS <- paste0('benchS',seq_len(nSurr))
  nameVarPrecipS <- paste0('precipS',seq_len(nSurr))
  nameVarPrecipTypeS <- paste0('precipTypeS',seq_len(nSurr))
  nameVarPrecipBulkS <- paste0('precipBulkS',seq_len(nSurr))
  strainGaugeDepthAgr[,nameVarDepthS] <- as.numeric(NA)
  strainGaugeDepthAgr[,nameVarBenchS] <- as.numeric(NA)
  strainGaugeDepthAgr[,c(nameVarPrecipS)] <- FALSE
  strainGaugeDepthAgr[,nameVarPrecipTypeS] <- as.character(NA)
  strainGaugeDepthAgr[,nameVarPrecipBulkS] <- as.numeric(NA)

  for(idxSurr in c(0,seq_len(nSurr))){
    
    if (idxSurr == 0){
      message(paste0('Running original timeseries'))
      nameVarDepth <- 'strainGaugeDepth'
      nameVarBench <- 'bench'
      nameVarPrecip <- 'precip'
      nameVarPrecipType <- 'precipType'
      nameVarPrecipBulk <- 'precipBulk'
      
      strainGaugeDepthS <- strainGaugeDepthAgr$strainGaugeDepth
      
    } else {
      message(paste0('Running Surrogate ',idxSurr))
      nameVarDepth <- paste0('strainGaugeDepthS',idxSurr)
      nameVarBench <- paste0('benchS',idxSurr)
      nameVarPrecip <- paste0('precipS',idxSurr)
      nameVarPrecipType <- paste0('precipTypeS',idxSurr)
      nameVarPrecipBulk <- paste0('precipBulkS',idxSurr)
      
      # If this is the first surrogate, create them
      if(idxSurr == 1){
        depthMinusBench <- strainGaugeDepthAgr$strainGaugeDepth - strainGaugeDepthAgr$bench # remove the computed benchmark
        setNotNa <- !is.na(depthMinusBench) # Remove all NA
        surrFill <- multifractal::iaaft(x=depthMinusBench[setNotNa],N=nSurr)
        strainGaugeDepthAgr[setNotNa,nameVarDepthS] <- strainGaugeDepthAgr$bench + surrFill    # Add the surrogates to the benchmark
      }
      
      strainGaugeDepthS <- strainGaugeDepthAgr[[nameVarDepth]]
      
    }  
    
    # Use standalone variables when running through the loop for speed
    varBench <- strainGaugeDepthAgr[[nameVarBench]]
    varPrecip <- strainGaugeDepthAgr[[nameVarPrecip]]
    varPrecipType <- strainGaugeDepthAgr[[nameVarPrecipType]]
    startDateTime <- strainGaugeDepthAgr$startDateTime
  
    # start counters for smoothing algorithm
    rawCount <- 0
    timeSincePrecip <- NA
    currRow <- rangeSize 
    
    #initialize fields. 
    varBench[1:currRow] <-  stats::quantile(strainGaugeDepthS[1:currRow],Quant,na.rm=TRUE)
    
    ##loop through data to establish benchmarks 
    skipping <- FALSE
    for (i in 1:numRow){
      
      # Check for at least 1/2 a day of non-NA values. 
      # If not, get to the next point at which we have at least 1/2 day and start fresh
      if(base::sum(base::is.na(strainGaugeDepthS[i:currRow])) > .5*rangeSize){
        
        # Find the last non-NA value 
        setEval <- i:currRow
        idxEnd <- setEval[tail(which(!is.na(strainGaugeDepthS[setEval])),1)]
        
        if(length(idxEnd) > 0){
          # Remove the benchmark extending into the gap
          varBench[(idxEnd+1):currRow] <- NA
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
        idxBgnNext <- setEval[head(which(!is.na(strainGaugeDepthS[setEval])),1)]
        
        # Re-establish the benchmark
        varBench[idxBgnNext:currRow] <- stats::quantile(strainGaugeDepthS[idxBgnNext:currRow],Quant,na.rm=TRUE)
        timeSincePrecip <- NA
        skipping <- FALSE
      }
      
      
      if(!is.na(timeSincePrecip)){
        timeSincePrecip <- timeSincePrecip + 1
      }
      recentPrecip <- base::any(varPrecip[i:currRow])

      #establish benchmark
      bench <- varBench[currRow-1]
      raw <- strainGaugeDepthS[currRow]
      precip <- FALSE
      precipType <- NA
      
      # missing data handling
      raw <- ifelse(is.na(raw), bench, raw) 
      
      #how many measurements in a row has raw been >= bench?
      if (raw >= bench){
        rawCount <- rawCount + 1
      }else{
        rawCount <- 0
      }
      
      # Compute the selected quantile over last range size (i.e. 1 day)
      # This will be used for correcting overactive benchmark changes
      raw_med_lastDay <- quantile(strainGaugeDepthS[i:currRow],Quant,na.rm=TRUE)
      raw_min_lastDay <- min(strainGaugeDepthS[i:currRow],na.rm=TRUE)
      
      
      # if precip total increased check to if any precip triggers are reached
      if (raw > bench){
        rawChange <- raw - bench
        
        if ((rawChange > (ChangeFactor * Envelope) & rawChange > ThshChange )){ 
          # If change was bigger than the diel range of noise (envelope) in the data and also 
          #   greater than the expected instrument sensitivity to rain, then it rained!
          bench <- raw # Update the benchmark for the next data point
          precip <- TRUE 
          timeSincePrecip <- 0
          precipType <- 'volumeThresh'
  
        } else if (grepl('volumeThresh',x=varPrecipType[currRow-1]) && rawChange > ThshChange){
          # Or, if is has been raining with the volume threshold and the precip depth continues to increase 
          #   above the expected instrument sensitivity, continue to say it is raining.
          bench <- raw # Update the benchmark for the next data point
          precip <- TRUE
          timeSincePrecip <- 0
          precipType <- 'volumeThreshContinued'
  
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
            rawIdx <- strainGaugeDepthS[idx]
            rawIdx <- ifelse(is.na(rawIdx), benchNext, rawIdx)
            if(rawIdx < benchNext){
              benchNext <- rawIdx
              varBench[idx] <- rawIdx
            } else {
              varBench[idx] <- benchNext
            }
            
            # Record rain stats
            varPrecip[idx] <- precip
            varPrecipType[idx] <- 'ThshCountBackFilledToStart'
          }
          
        } else if (rawCount >= ThshCount){
          # Or, if it continues to drizzle and raw is continuing to rise, keep saying that it's raining
          bench <- raw 
          precip <- TRUE
          timeSincePrecip <- 0
          precipType <- 'ThshCount'
        }
      }
      if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (varBench[i-1]-Recharge)){  # Maybe use Envelope instead of Recharge?
        
        # Exactly one rangeSize after rain ends, and if the depth hasn't dropped precipitously (as defined by the Recharge threshold),
        # back-adjust the benchmark to the Quant of the last day to avoid overestimating actual precip
  
        bench <- raw_med_lastDay
        varBench[i:currRow] <- bench
        varPrecipType[i:currRow] <- "postPrecipAdjToMedNextDay"
        
        idxBgn <- i-1
        keepGoing <- TRUE
        while(keepGoing == TRUE) {
  
          if(is.na(varPrecip[idxBgn]) || varPrecip[idxBgn] == FALSE){
            # Stop if we are past the point where the precip started
            keepGoing <- FALSE
          } else if(varBench[idxBgn] > bench){
            varBench[idxBgn] <- bench
            varPrecipType[idxBgn] <- paste0(varPrecipType[idxBgn],"BackAdjToMedNextDay")
            idxBgn <- idxBgn - 1
          } else {
            keepGoing <- FALSE
          }
        }
      } else if ((raw < bench) && (bench-raw_med_lastDay) > ChangeFactorEvap*Envelope && !recentPrecip){
        # If it hasn't rained in at least 1 day, check for evaporation & reset benchmark if necessary
  
        bench <- raw_min_lastDay # Set to the min of the last day to better track evap
        precipType <- 'EvapAdj'
        if(idxSurr == 0){
          flagsAgr$evapDetectedQF[i:currRow] <- 1
        }
        
      } else if ((bench - raw) > Recharge){
        # If the raw depth has dropped precipitously (as defined by the recharge rage), assume bucket was emptied. Reset benchmark.
        bench <- raw
        
        # Get rid of a couple hours before the recharge. This is when calibrations are occuring and strain gauges are being replaced.
        # Set the benchmark constant to the point 2 hours before the recharge
        setAdj <- startDateTime > (startDateTime[currRow] - as.difftime(3,units='hours')) &
          startDateTime < startDateTime[currRow]
        idxSet <- head(which(setAdj),1) - 1
        if (idxSet < 1){
          varBench[setAdj] <- NA
          varPrecip[setAdj] <- NA
          varPrecipType[setAdj] <- NA
        } else {
          varBench[setAdj] <- varBench[idxSet]
          varPrecip[setAdj] <- varPrecip[idxSet]
          varPrecipType[setAdj] <- "ExcludeBeforeRecharge"
        }
        
      } 
      
      #update in the data
      varBench[currRow] <- bench
      varPrecip[currRow] <- precip
      varPrecipType[currRow] <- precipType
      
      #move to next row
      currRow <- currRow + 1
      
      #stop at end of data frame
      if (currRow == numRow){
        varBench[currRow] <- bench
        break()
      }
    }

    # Reassign outputs
    strainGaugeDepthAgr[[nameVarBench]] <- varBench
    strainGaugeDepthAgr[[nameVarPrecip]] <- varPrecip 
    strainGaugeDepthAgr[[nameVarPrecipType]] <- varPrecipType
    
    # Compute precip
    strainGaugeDepthAgr[[nameVarPrecipBulk]] <- c(diff(varBench),as.numeric(NA))
    strainGaugeDepthAgr[[nameVarPrecipBulk]][strainGaugeDepthAgr[[nameVarPrecipBulk]] < 0] <- 0
    strainGaugeDepthAgr[[nameVarPrecipType]] <- c(strainGaugeDepthAgr[[nameVarPrecipType]][2:numRow],as.numeric(NA)) # Shift precip type to align with precip
    
  } # End loop around surrogates
  
  # Compute the uncertainty in precip based on the variability in computed benchmark of the surrogates
  # The uncertainty of a sum or difference is equal to their individual uncertainties added in quadrature.
  nameVar <- names(strainGaugeDepthAgr)
  nameVarBenchS <- nameVar[grepl('benchS[0-9]',nameVar)]
  strainGaugeDepthAgr$benchS_std <- matrixStats::rowSds(as.matrix(strainGaugeDepthAgr[,nameVarBenchS]))
  strainGaugeDepthAgr$precipS_std <- sqrt(strainGaugeDepthAgr$benchS_std^2 + lag(strainGaugeDepthAgr$benchS_std, 1)^2)
  strainGaugeDepthAgr$precipBulk_u95 <- strainGaugeDepthAgr$precipS_std*2
  
  
  # Post-precip computation 
  flagsAgr$insuffDataQF[is.na(strainGaugeDepthAgr$precipBulk)] <- 1
  # Soft flag for max precip over 60-min - NEED TO MOVE THIS AFTER AGGREGATE TO HOURLY
  flagsAgr$extremePrecipQF[strainGaugeDepthAgr$precipBulk > ExtremePrecipMax] <- 1 # Soft flag for max precip over 60-min
  
  # Envelope == Massive --> Flag all the data
  if(Envelope > 10){
    flagsAgr$dielNoiseQF <- 1
  }

  
  
  # AGGREGATE TO HOURLY
  
  
  
  # For the central day and adjacent days on either side of the central day, output a file with the results. A later module will average output for each day
  # Report total precip, and compute uncertainty for the central day
  # We can use the same equation here, adding the uncertainties for the start and
  # end of the day in quadrature, with the caveat that the benchmark does not drop 
  # over the course of the day. If this occurs we need to compute for each leg of 
  # a flat or increasing benchmark, summing the legs in quadrature
  dayOut <- InfoDirIn$time+as.difftime(c(-1,0,1),units='days')
  
  for(idxDayOut in seq_len(length(dayOut))){
    
    dayOutIdx <- dayOut[idxDayOut]
    # Get the records for this date
    setDayUcrt <- which(strainGaugeDepthAgr$startDateTime >= dayOutIdx & 
                          strainGaugeDepthAgr$startDateTime <= (dayOutIdx + as.difftime(1,units='days'))) # include first point of next day, because that is the point from which the difference is taken
    setOut <- strainGaugeDepthAgr$startDateTime >= dayOutIdx & 
      strainGaugeDepthAgr$startDateTime < (dayOutIdx + as.difftime(1,units='days'))
    
    # Compute uncertainty for each differencing leg (i.e. period of same or increasing benchmark)
    benchDiff <- diff(strainGaugeDepthAgr$bench[setDayUcrt])
    setBrk <- c(0,which(is.na(benchDiff) | benchDiff < 0),length(setDayUcrt))
    ucrtDayBrk <- rep(0,length(setBrk)-1)
    for (idxBrk in seq_len(length(setBrk)-1)){
      idxLegBgn <- setDayUcrt[setBrk[idxBrk]+1]
      idxLegEnd <- setDayUcrt[setBrk[idxBrk+1]]
      if((idxLegEnd-idxLegBgn) == 0){
        next
      }
      ucrtDayBrk[idxBrk] <- sqrt(strainGaugeDepthAgr$precipS_std[idxLegBgn]^2 + strainGaugeDepthAgr$precipS_std[idxLegEnd]^2)
    }
    UcrtDay <- sqrt(sum(ucrtDayBrk^2))
    PrecipDay <- sum(strainGaugeDepthAgr$precipBulk[setOut])
    
    # CREATE DAILY OUTPUT
    
    
    # Constrain to desired output - ADD UNCERTAINTY
    strainGaugeDepthAgrIdx <- strainGaugeDepthAgrIdx[setOut,c('startDateTime',
                                                        'endDateTime',
                                                        'strainGaugeDepth',
                                                        'orificeHeaterFlag',
                                                        'inletHeater1QM',
                                                        'inletHeater2QM',
                                                        'inletHeater3QM',
                                                        'inletHeaterNAQM',
                                                        'bench',
                                                        'precip',
                                                        'precipType',
                                                        'precipBulk',
                                                        'precipBulk_u95')]
    flagsAgrIdx <- flagsAgr [setOut,]
    
    
    # Replace the date in the output path structure with the current date
    dirOutDataIdx <- sub(pattern=format(InfoDirIn$time,'%Y/%m/%d'),
                         replacement=format(dayOutIdx,'%Y/%m/%d'),
                         x=dirOutData,
                         fixed=TRUE)
    dirOutFlagsIdx <- sub(pattern=format(InfoDirIn$time,'%Y/%m/%d'),
                         replacement=format(dayOutIdx,'%Y/%m/%d'),
                         x=dirOutFlags,
                         fixed=TRUE)
    base::dir.create(dirOutDataIdx,recursive = TRUE)
    base::dir.create(dirOutFlagsIdx,recursive = TRUE)
    
    # Get the filename for this day
    nameFileIdx <- fileData[grepl(format(dayOutIdx,'%Y-%m-%d'),fileData)][1]

    if(!is.na(nameFileIdx)){
      
      # Append the center date to the end of the file name to know where it came from
      nameFileIdxSplt <- base::strsplit(nameFileIdx,'.',fixed=TRUE)[[1]]
      nameFileDataIdxSplt <- c(paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt)-1)],
                                  '_from_',
                                  format(InfoDirIn$time,'%Y-%m-%d')),
                           utils::tail(nameFileIdxSplt,1))
      nameFileFlagsIdxSplt <- c(paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt)-1)],
                                  '_flagsSmooth',
                                  '_from_',
                                  format(InfoDirIn$time,'%Y-%m-%d')),
                           utils::tail(nameFileIdxSplt,1))
      nameFileDataOutIdx <- base::paste0(nameFileDataIdxSplt,collapse='.')
      nameFileFlagsOutIdx <- base::paste0(nameFileFlagsIdxSplt,collapse='.')
      
      # Write out the data to file
      fileDataOutIdx <- fs::path(dirOutDataIdx,nameFileDataOutIdx)

      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = strainGaugeDepthAgrIdx,
          NameFile = fileDataOutIdx,
          NameFileSchm=NULL,
          Schm=SchmData,
          log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileDataOutIdx,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote computed precipitation to file ',
          fileDataOutIdx
        ))
      }
      
      # Write out the flags to file
      fileFlagsOutIdx <- fs::path(dirOutFlagsIdx,nameFileFlagsOutIdx)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = flagsAgrIdx,
          NameFile = fileFlagsOutIdx,
          NameFileSchm=NULL,
          Schm=NULL,
          log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileFlagsOutIdx,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote flags to file ',
          fileFlagsOutIdx
        ))
      }
      
    } else {
      log$warn(paste0(nameFileIdx,' and associated flags files are not able to be output because this data file was not found in the input.'))
    }
    
  }

  return()
} 
