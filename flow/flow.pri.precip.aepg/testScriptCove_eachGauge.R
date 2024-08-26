# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/15/precip-weighing_ARIK900000/aepg600m_heated/CFGLOC101675"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_BLUE900000/aepg600m_heated/CFGLOC103882"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_BONA900000/aepg600m_heated/CFGLOC112155"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/02/15/precip-weighing_CLBJ900000/aepg600m_heated/CFGLOC105127"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/06/01/precip-weighing_CPER900000/aepg600m_heated/CFGLOC101864"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_GUAN900000/aepg600m/CFGLOC104412"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_HARV900000/aepg600m_heated/CFGLOC108455"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/03/01/precip-weighing_KONZ900000/aepg600m_heated/CFGLOC109787"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2022/11/01/precip-weighing_ONAQ900000/aepg600m_heated/CFGLOC107416"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/01/15/precip-weighing_REDB900000/aepg600m_heated/CFGLOC112599"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/07/01/precip-weighing_PRIN900000/aepg600m_heated/CFGLOC104101"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_SRER900000/aepg600m/CFGLOC104646"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/03/01/precip-weighing_OSBS900000/aepg600m/CFGLOC102875"
DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/05/01/precip-weighing_SCBI900000/aepg600m_heated/CFGLOC103160"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/03/01/precip-weighing_SJER900000/aepg600m_heated/CFGLOC113350"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_TALL900000/aepg600m/CFGLOC108877"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/03/01/precip-weighing_TOOL900000/aepg600m_heated/CFGLOC106786"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_UNDE900000/aepg600m_heated/CFGLOC107634"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/09/15/precip-weighing_WOOD900000/aepg600m_heated/CFGLOC107003"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/10/01/precip-weighing_WREF900000/aepg600m_heated/CFGLOC112933"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_YELL900000/aepg600m_heated/CFGLOC113591"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_ORNL900000/aepg600m_heated/CFGLOC103016"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/04/01/precip-weighing_NIWO900000/aepg600m_heated/CFGLOC109533"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_PUUM900000/aepg600m/CFGLOC113779"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/15/precip-weighing_HQTW900000/aepg600m_heated/CFGLOC114310"

DirOutBase <- "/scratch/pfs/outCove"
DirSubCopy <- NULL
WndwAgr <- '5 min'
RangeSizeHour <- 24
Envelope <- 1.3
ThshCountHour <- 15
Quant <- 0.5 # Where is the benchmark set (quantile) within the envelope (diel variation)
ThshChange <- 0.2
ChangeFactor <- 1
ChangeFactorEvap <- 0.5
Recharge <- 20

# Testing belfort calibration - OSBS (source ID 16768) 2023-01-11T15:48:49.0000
A1 <- 0.515340 # kg/kHz
B1 <- 2.850230 # kg/kHz^2
A2 <- 0.460100
B2 <- 2.845210
A3 <- 0.537800
B3 <- 2.795680
kgZero <- 4.945257 # kg at zero precip

# Testing belfort calibration - SCBI (source ID 48231) 2023-03-27
A1 <- 0.492740 # kg/kHz
B1 <- 2.780140 # kg/kHz^2
A2 <- 0.492650
B2 <- 2.833610
A3 <- 0.481350
B3 <- 2.749470
kgZero <- 13.288919 # kg at zero precip (looks like they didn't update the zero after de-winterization) 
A1N1 <- 0.037663224954804 # cm/Hz
A2N1 <- 0.000018386294759 # cm/Hz^2
F0N1 <- 1090.13 #1283 # kHz at zero; 1069.50 (P0)
A1N2 <- 0.037249057178719 # cm/Hz
A2N2 <- 0.000019339755261 # cm/Hz^2
F0N2 <- 1078.41 # 1284 # kHz at zero; 1096.80 (P0)
A1N3 <- 0.039012135122498 # cm/Hz
A2N3 <- 0.000018846301851 # cm/Hz^2
F0N3 <- 1092.04 # 1293 # Hz at zero; 1053.05 (P0)



if (FALSE) {
  # Testing belfort calibration - WOOD (source ID 30717) 2023-04-10
  A1 <- 0.444884 # kg/kHz
  B1 <- 2.830336 # kg/kHz^2
  A2 <- 0.486620
  B2 <- 2.778610
  A3 <- 0.472717
  B3 <- 2.935589
  kgZero <- 11.223530 # kg at zero precip 
  A1N1 <- 0.038106961204386 # cm/Hz
  A2N1 <- 0.000017680830992 # cm/Hz^2
  F0N1 <- 1069.50 #1531 # kHz at zero; 1069.50 (P0)
  A1N2 <- 0.038055800869281 # cm/Hz
  A2N2 <- 0.000018218258375 # cm/Hz^2
  F0N2 <- 1096.80 # 1572 # kHz at zero; 1096.80 (P0)
  A1N3 <- 0.039224874513605 # cm/Hz
  A2N3 <- 0.000022414980641 # cm/Hz^2
  F0N3 <- 1053.05 # 1534 # Hz at zero; 1053.05 (P0)
}

# Start logging
log <- NEONprocIS.base::def.log.init(Lvl='debug')

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
if (!('internal_temperature' %in% names(data))){
  data$internal_temperature <- as.numeric(NA)
}

# Apply Belfort's calibration to each gauge (use the _compensated streams since they have no cal applied)
# 50 mm of rain is 1 kg
data <- data %>% dplyr::mutate(strainGauge1DepthBf = ((A1*(strain_gauge1_frequency_compensated/1000)+B1*(strain_gauge1_frequency_compensated/1000)^2)-kgZero)*50,
                               strainGauge2DepthBf = ((A2*(strain_gauge2_frequency_compensated/1000)+B2*(strain_gauge2_frequency_compensated/1000)^2)-kgZero)*50,
                               strainGauge3DepthBf = ((A3*(strain_gauge3_frequency_compensated/1000)+B3*(strain_gauge3_frequency_compensated/1000)^2)-kgZero)*50,
                               strainGauge1DepthNn = ((A1N1*(strain_gauge1_frequency_compensated-F0N1)+A2N1*(strain_gauge1_frequency_compensated-F0N1)^2))*10,
                               strainGauge2DepthNn = ((A1N2*(strain_gauge2_frequency_compensated-F0N2)+A2N2*(strain_gauge2_frequency_compensated-F0N2)^2))*10,
                               strainGauge3DepthNn = ((A1N3*(strain_gauge3_frequency_compensated-F0N3)+A2N3*(strain_gauge3_frequency_compensated-F0N3)^2))*10
)




# Attempt a daily fit, testing for high enough R2 and positive slope
# Note - could also apply this to the envelope, falling back on the pre-defined threshold if day(s) without rain cannot be determined
setNoRain <- NULL
if (FALSE) {
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
    },silent=TRUE)
  }
  
  # Keep only the excellent regressions
  rsq_min <- 0.9 # minimum R-squared to accept regression
  tempRegrKeep <- tempRegr
  tempRegrKeep$slopeTemp1[tempRegrKeep$rsq1 < rsq_min] <- NA
  tempRegrKeep$slopeTemp2[tempRegrKeep$rsq2 < rsq_min] <- NA
  tempRegrKeep$slopeTemp3[tempRegrKeep$rsq3 < rsq_min] <- NA
  
  # Also require that there be no change in depth between the start and end of the day that is 
  # greater than the pre-defined envelope
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
  
  
  
  
  # Regressions for some sensors are better than others.
  # Also, the regression for some sensors is transient, applicable only within a day
  # Use the non-rain days identified by the sensor with the most accepted regressions
  # and require at least 2 days without rain, optionally recomputing
  # the temperature regression for the longest consecutive no-rain period
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

# Aggregate depth streams into a single depth. 
data <- data %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, strainGauge2Depth, strainGauge3Depth), na.rm = F),
                               strainGaugeDepthBf = base::rowMeans(x=base::cbind(strainGauge1DepthBf, strainGauge2DepthBf, strainGauge3DepthBf), na.rm = F),
                               strainGaugeDepthNn = base::rowMeans(x=base::cbind(strainGauge1DepthNn, strainGauge2DepthNn, strainGauge3DepthNn), na.rm = F),
                               strainGaugeTemperature = base::rowMeans(x=base::cbind(strain_gauge1_temperature, strain_gauge2_temperature, strain_gauge3_temperature), na.rm = F))  



# Do time averaging
strainGaugeDepthAgr <- data %>%
  dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
  dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
  dplyr::group_by(startDateTime,endDateTime) %>%
  dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T),
                   strainGaugeDepthBf = mean(strainGaugeDepthBf, na.rm = T),
                   strainGaugeDepthNn = mean(strainGaugeDepthNn, na.rm = T),
                   strainGauge1Depth = mean(strainGauge1Depth, na.rm = T),
                   strainGauge2Depth = mean(strainGauge2Depth, na.rm = T),
                   strainGauge3Depth = mean(strainGauge3Depth, na.rm = T),
                   strainGauge1DepthBf = mean(strainGauge1DepthBf, na.rm = T),
                   strainGauge2DepthBf = mean(strainGauge2DepthBf, na.rm = T),
                   strainGauge3DepthBf = mean(strainGauge3DepthBf, na.rm = T),
                   strainGauge1DepthNn = mean(strainGauge1DepthNn, na.rm = T),
                   strainGauge2DepthNn = mean(strainGauge2DepthNn, na.rm = T),
                   strainGauge3DepthNn = mean(strainGauge3DepthNn, na.rm = T),
                   strainGaugeTemperature = mean(strainGaugeTemperature, na.rm = T),
                   strainGauge1Temperature = mean(strain_gauge1_temperature, na.rm = T),
                   strainGauge2Temperature = mean(strain_gauge2_temperature, na.rm = T),
                   strainGauge3Temperature = mean(strain_gauge3_temperature, na.rm = T),
                   internalTemperature = mean(internal_temperature, na.rm=T))


# Re-do computation of no-rain days in order to apply a dynamic envelope calculation 
# Require that there be no change in depth between the start and end of the day that is 
# greater than the pre-defined envelope
if(TRUE){
  dataHourly <- strainGaugeDepthAgr %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'hour')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepth = median(strainGaugeDepth, na.rm = T))
  dataDaily <- dataHourly %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, unit = 'day')) %>%
    dplyr::group_by(startDateTime) %>%
    dplyr::summarise(strainGaugeDepthChg = tail(strainGaugeDepth,1)-head(strainGaugeDepth,1))
  setNoRain <- dataDaily$strainGaugeDepthChg < 0.25*Envelope
}

# Get the envelope if we have determined days without rain
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
}

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

strainGaugeDepthAgr$strainGaugeMeanDepth <- strainGaugeDepthAgr$strainGaugeDepth
strainGaugeDepthAgr$strainGaugeMeanDepthBf <- strainGaugeDepthAgr$strainGaugeDepthBf
strainGaugeDepthAgr$strainGaugeMeanDepthNn <- strainGaugeDepthAgr$strainGaugeDepthNn

for(idxName in c('strainGauge1Depth','strainGauge2Depth','strainGauge3Depth','strainGaugeMeanDepth','strainGauge1DepthBf','strainGauge2DepthBf','strainGauge3DepthBf','strainGaugeMeanDepthBf','strainGauge1DepthNn','strainGauge2DepthNn','strainGauge3DepthNn','strainGaugeMeanDepthNn')){

  strainGaugeDepthAgr$strainGaugeDepth <- strainGaugeDepthAgr[[idxName]]

  #start counters
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day
  
  #initialize fields
  strainGaugeDepthAgr$bench <- NA
  strainGaugeDepthAgr$precip <- FALSE #add TRUE when rain detected
  strainGaugeDepthAgr$precipType <- NA
  
  #!!! Needs logic for NA start
  strainGaugeDepthAgr$bench[1:currRow] <-  stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[1:currRow],Quant,na.rm=TRUE)
  
  ##loop through data to establish benchmarks 
  skipping <- FALSE
  numRow <- nrow(strainGaugeDepthAgr)
  for (i in 1:numRow){
    
    #if(currRow == 4574){stop()} 
    
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
    # nMin <- sort(strainGaugeDepthAgr$strainGaugeDepth[i:currRow], decreasing = FALSE)[nthVal]
    # nMax <- sort(strainGaugeDepthAgr$strainGaugeDepth[i:currRow], decreasing = TRUE)[nthVal]
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
      # back-adjust the benchmark to the median of the last day to avoid overestimating actual precip
      # Under heavy evaporation, this has the effect of removing spurious precip, potentially also small real precip events
      #stop()
      bench <- raw_med_lastDay
      strainGaugeDepthAgr$bench[i:currRow] <- bench
      strainGaugeDepthAgr$precipType[i:currRow] <- "postPrecipAdjToMedNextDay"

      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE) {
        #print(idxBgn)
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
      bench <- raw_min_lastDay
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
        strainGaugeDepthAgr$bench[setAdj] <- NA
        strainGaugeDepthAgr$precip[setAdj] <- NA
        strainGaugeDepthAgr$precipType[setAdj] <- NA
      } else {
        strainGaugeDepthAgr$bench[setAdj] <- strainGaugeDepthAgr$bench[idxSet]
        strainGaugeDepthAgr$precip[setAdj] <- strainGaugeDepthAgr$precip[idxSet]
        strainGaugeDepthAgr$precipType[setAdj] <- "ExcludeBeforeRecharge"
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



  # TESTING ONLY
  strainGaugeDepthAgr$precipBulk <- strainGaugeDepthAgr$bench - lag(strainGaugeDepthAgr$bench, 1)
  strainGaugeDepthAgr <- strainGaugeDepthAgr %>% mutate(precipBulk = ifelse(precipBulk < 0, 0, precipBulk))

  
  
  # Reassign outputs
  if(idxName == 'strainGauge1Depth'){
    strainGaugeDepthAgr$bench1 <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip1 <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType1 <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk1 <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge2Depth'){
    strainGaugeDepthAgr$bench2 <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip2 <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType2 <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk2 <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge3Depth'){
    strainGaugeDepthAgr$bench3 <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip3 <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType3 <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk3 <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGaugeMeanDepth'){
    strainGaugeDepthAgr$benchMean <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precipMean <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipTypeMean <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulkMean <- strainGaugeDepthAgr$precipBulk
  } else if(idxName == 'strainGauge1DepthBf'){
    strainGaugeDepthAgr$bench1Bf <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip1Bf <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType1Bf <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk1Bf <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge2DepthBf'){
    strainGaugeDepthAgr$bench2Bf <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip2Bf <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType2Bf <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk2Bf <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge3DepthBf'){
    strainGaugeDepthAgr$bench3Bf <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip3Bf <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType3Bf <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk3Bf <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGaugeMeanDepthBf'){
    strainGaugeDepthAgr$benchMeanBf <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precipMeanBf <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipTypeMeanBf <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulkMeanBf <- strainGaugeDepthAgr$precipBulk
  } else if(idxName == 'strainGauge1DepthNn'){
    strainGaugeDepthAgr$bench1Nn <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip1Nn <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType1Nn <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk1Nn <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge2DepthNn'){
    strainGaugeDepthAgr$bench2Nn <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip2Nn <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType2Nn <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk2Nn <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGauge3DepthNn'){
    strainGaugeDepthAgr$bench3Nn <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precip3Nn <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipType3Nn <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulk3Nn <- strainGaugeDepthAgr$precipBulk
  } else if (idxName == 'strainGaugeMeanDepthNn'){
    strainGaugeDepthAgr$benchMeanNn <- strainGaugeDepthAgr$bench
    strainGaugeDepthAgr$precipMeanNn <- strainGaugeDepthAgr$precip
    strainGaugeDepthAgr$precipTypeMeanNn <- strainGaugeDepthAgr$precipType
    strainGaugeDepthAgr$precipBulkMeanNn <- strainGaugeDepthAgr$precipBulk
  }
  
  
}

# df <- data.table::melt(strainGaugeDepthAgr[,c(1,3,4,7)],id.vars=c('startDateTime'))
df <- data.table::melt(strainGaugeDepthAgr[,c('startDateTime','strainGauge1Depth','bench1','strainGauge2Depth','bench2','strainGauge3Depth','bench3','strainGaugeMeanDepth','benchMean','strainGauge1DepthBf','bench1Bf','strainGauge2DepthBf','bench2Bf','strainGauge3DepthBf','bench3Bf','strainGaugeMeanDepthBf','benchMeanBf','strainGauge1DepthNn','bench1Nn','strainGauge2DepthNn','bench2Nn','strainGauge3DepthNn','bench3Nn','strainGaugeMeanDepthNn','benchMeanNn')],id.vars=c('startDateTime'))
# df <- data.table::melt(strainGaugeDepthAgr[,c('startDateTime','strainGauge1Depth','bench1','strainGauge2Depth','bench2','strainGauge3Depth','bench3','strainGaugeMeanDepth','benchMean')],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')

if (FALSE){
  # Look at the relationship with gauge temperature
  plotly::plot_ly(data=strainGaugeDepthAgr,x=~strainGaugeTemperature,y=~strainGaugeDepth,type = 'scatter', mode = 'markers') %>%
    plotly::add_trace(x=~strainGauge1Temperature,y=~strainGauge1Depth,type = 'scatter', mode = 'markers') %>%
    plotly::add_trace(x=~strainGauge2Temperature,y=~strainGauge2Depth,type = 'scatter', mode = 'markers') %>%
    plotly::add_trace(x=~strainGauge3Temperature,y=~strainGauge3Depth,type = 'scatter', mode = 'markers')
  
  # Look at the relationship with internal temperature
  # plotly::plot_ly(data=strainGaugeDepthAgr,x=~internalTemperature,y=~strainGaugeDepth,type = 'scatter', mode = 'markers') %>%
  #   plotly::add_trace(x=~internalTemperature,y=~strainGauge1Depth,type = 'scatter', mode = 'markers') %>%
  #   plotly::add_trace(x=~internalTemperature,y=~strainGauge2Depth,type = 'scatter', mode = 'markers') %>%
  #   plotly::add_trace(x=~internalTemperature,y=~strainGauge3Depth,type = 'scatter', mode = 'markers')
}


dataDaily <- strainGaugeDepthAgr %>%
  dplyr::mutate(startDate = lubridate::floor_date(startDateTime, unit = 'day')) %>%
  dplyr::group_by(startDate) %>%
  dplyr::summarise(precipBulk1 = sum(precipBulk1),
                   precipBulk2 = sum(precipBulk2),
                   precipBulk3 = sum(precipBulk3),
                   precipBulkMean = sum(precipBulkMean),
                   precipBulk1Bf = sum(precipBulk1Bf),
                   precipBulk2Bf = sum(precipBulk2Bf),
                   precipBulk3Bf = sum(precipBulk3Bf),
                   precipBulkMeanBf = sum(precipBulkMeanBf),
                   precipBulk1Nn = sum(precipBulk1Nn),
                   precipBulk2Nn = sum(precipBulk2Nn),
                   precipBulk3Nn = sum(precipBulk3Nn),
                   precipBulkMeanNn = sum(precipBulkMeanNn))

dfpr_long <- data.table::melt(dataDaily,id.vars=c('startDate'))
p <- plotly::plot_ly(data=dfpr_long,x=~startDate,y=~value,color=~variable, type = 'bar', mode = 'markers') %>% 
  plotly::layout(title = paste0('Belfort vs. NEON daily precip by gauge')) 
print(p)
