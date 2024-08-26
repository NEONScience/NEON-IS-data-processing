# Gather info about the input directory and create the output directory.
InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
dirInData <- fs::path(DirIn,'data')
dirInFlags <- fs::path(DirIn,'flags')
# dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
# dirOutData <- fs::path(dirOut,'data')
# dirOutFlags <- fs::path(dirOut,'flags')
# NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
#                               DirSub = c('data','flags'),
#                               log = log)

# Copy with a symbolic link the desired subfolders 
# DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('data','flags')))
# if(base::length(DirSubCopy) > 0){
#   
#   NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
#                                      DirDest=dirOut,
#                                      LnkSubObj=FALSE,
#                                      log=log)
# }    

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
    strainGaugeDepthAgr$bench[i:currRow] <- stats::quantile(strainGaugeDepthAgr$strainGaugeDepth[i:currRow],Quant,na.rm=TRUE)
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
    
  } else if (!is.na(timeSincePrecip) && timeSincePrecip == rangeSize && raw > (bench-Envelope)){  # Maybe use Envelope instead of Recharge?
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
    bench <- raw_med_lastDay
    precipType <- 'EvapAdj'
    
  } else if ((bench - raw) > Recharge){
    # If the raw depth has dropped precipitously (as defined by the recharge rage), assume bucket was emptied. Reset benchmark.
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
strainGaugeDepthAgr$weighPrecipBulk <- strainGaugeDepthAgr$bench - lag(strainGaugeDepthAgr$bench, 1)
strainGaugeDepthAgr <- strainGaugeDepthAgr %>% mutate(weighPrecipBulk = ifelse(weighPrecipBulk < 0, 0, weighPrecipBulk))

df <- data.table::melt(strainGaugeDepthAgr[,c(1,3,4,7)],id.vars=c('startDateTime'))
print(plotly::plot_ly(data=df,x=~startDateTime,y=~value,color=~variable,mode='lines'))

# ### pull in prism data
# site <- stringr::str_extract(DirIn, pattern= '[A-Z]{4}')
# prism_files <- list.files('/scratch/prism', full.names = T)
# 
# file <- stringr::str_subset(prism_files, pattern = site)
# prism <- readr::read_csv(file)
# strainGaugeDepthAgr_prism <- strainGaugeDepthAgr %>%
#   mutate(startDateTime = startDateTime +12*60*60,
#           endDateTime = startDateTime + 12*60*60) %>%
#   mutate(startDate = lubridate::floor_date(startDateTime, '1 day')) %>%
#   group_by(startDate) %>%
#   summarise(dailyPrecipNEON = sum(weighPrecipBulk))
# 
# dfpr <- left_join(strainGaugeDepthAgr_prism, prism, by = 'startDate')
# dfpr <- data.table::melt(dfpr,id.vars=c('startDate'))
# print(plotly::plot_ly(data=dfpr,x=~startDate,y=~value,color=~variable, type = 'bar'))
# print(plotly::plot_ly(data=dfpr,x=~ppt,y=~dailyPrecipNEON, type = 'scatter'))


