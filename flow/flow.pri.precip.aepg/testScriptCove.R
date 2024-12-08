# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2022/08/01/precip-weighing_ARIK900000/aepg600m_heated/CFGLOC101675"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_BLUE900000/aepg600m_heated/CFGLOC103882"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_BONA900000/aepg600m_heated/CFGLOC112155"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/06/15/precip-weighing_CLBJ900000/aepg600m_heated/CFGLOC105127"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother_P0/2023/08/01/precip-weighing_CPER900000/aepg600m_heated/CFGLOC101864"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_GUAN900000/aepg600m/CFGLOC104412"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/30/precip-weighing_HARV900000/aepg600m_heated/CFGLOC108455"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/03/01/precip-weighing_KONZ900000/aepg600m_heated/CFGLOC109787"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother_P0/2022/02/15/precip-weighing_ONAQ900000/aepg600m_heated/CFGLOC107416"
DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother_P0/2022/08/07/precip-weighing_REDB900000/aepg600m_heated/CFGLOC112599"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/07/01/precip-weighing_PRIN900000/aepg600m_heated/CFGLOC104101"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother_P0/2023/08/07/precip-weighing_SRER900000/aepg600m/CFGLOC104646"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2022/11/08/precip-weighing_OSBS900000/aepg600m/CFGLOC102875"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2022/11/01/precip-weighing_SCBI900000/aepg600m_heated/CFGLOC103160"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/03/01/precip-weighing_SJER900000/aepg600m_heated/CFGLOC113350"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_TALL900000/aepg600m/CFGLOC108877"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/03/01/precip-weighing_TOOL900000/aepg600m_heated/CFGLOC106786"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_UNDE900000/aepg600m_heated/CFGLOC107634"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/09/precip-weighing_WOOD900000/aepg600m_heated/CFGLOC107003"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/10/01/precip-weighing_WREF900000/aepg600m_heated/CFGLOC112933"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother_P0/2022/05/28/precip-weighing_YELL900000/aepg600m_heated/CFGLOC113591"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2024/05/30/precip-weighing_ORNL900000/aepg600m_heated/CFGLOC103016"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2022/02/15/precip-weighing_NIWO900000/aepg600m_heated/CFGLOC109533"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/08/30/precip-weighing_PUUM900000/aepg600m/CFGLOC113779"
# DirIn <- "/scratch/pfs/precipWeighing_ts_pad_smoother/2023/01/15/precip-weighing_HQTW900000/aepg600m_heated/CFGLOC114310"

DirOutBase <- "/scratch/pfs/outCove"
DirSubCopy <- NULL
WndwAgr <- '5 min'
RangeSizeHour <- 24
Envelope <- 13.33333333
ThshCountHour <- 15
Quant <- 0.5 # Where is the benchmark set (quantile) within the envelope (diel variation)
ThshChange <- 0.2
ChangeFactor <- 1
ChangeFactorEvap <- 0.5
Recharge <- 20

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


# Aggregate depth streams into a single depth.
data <- data %>% dplyr::mutate(strainGaugeDepth = base::rowMeans(x=base::cbind(strainGauge1Depth, strainGauge2Depth, strainGauge3Depth), na.rm = F))

data$total_precipitation_depth <- data$total_gauge_weight*50
data$total_precipitation_depth[is.na(data$strainGaugeDepth)] <- NA




# Load in flags





# Do time averaging
strainGaugeDepthAgr <- data %>%
  dplyr::mutate(startDateTime = lubridate::floor_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr)) %>%
  dplyr::mutate(endDateTime = lubridate::ceiling_date(as.POSIXct(readout_time, tz = 'UTC'), unit = WndwAgr,change_on_boundary=TRUE)) %>%
  dplyr::group_by(startDateTime,endDateTime) %>%
  dplyr::summarise(strainGaugeDepth = mean(strainGaugeDepth, na.rm = T))
  # dplyr::summarise(strainGaugeDepth = mean(total_precipitation_depth, na.rm = T))


# Aggregate flags


# Decision process for aggregated data. How much data went into the time average? Flag or below threshold, or don't use in the smoothing algorithm.
# Something like 25% of the average.

# -------------- BEGIN EXPERIMENTAL ---------------

# Do computation of no-rain days in order to apply a dynamic envelope calculation
# Require that there be no change in depth between the start and end of the day that is
# greater than the pre-defined envelope
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
  
  # if envelope is larger than Recharge threshold adjust recharge.
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

# df <- data.table::melt(strainGaugeDepthAgr[,c(1,3,4,7)],id.vars=c('startDateTime'))
df <- data.table::melt(strainGaugeDepthAgr[,c(1,3,4)],id.vars=c('startDateTime'))
plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines')


# Post-precip computation
# Hard and soft thresholds for max precip over the averaging interval

