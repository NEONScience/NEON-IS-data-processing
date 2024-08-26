##############################################################################################
#' @title Primary Precipitation Noise removal Algorithm

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr


#' @description Function to adjust primary precipitation data to account for noise in the sensor
#' The needed input is a data frame with timestamps with variable 'tmi' and a precip variable 'depth'
#' Function and defaults based on Ralph Wright tool. 
#' 
#' This function also includes code to adjust 'raw' values in the event that a bucket is emptied. 
#' NEON addition also includes ability to match 5 minute granularity of current data product.
#' If a bucket is emptied all remaining raw values have current benchmark added to them. 
#' adds a rain = TRUE variable whenever rain was detected and countThreshold was exceeded

#' @param  raw_df raw_df  #raw data to run through function
#' @param  avg_adj unit to average raw data, defaults to 5 minutes, like previous precip data. Fed into lubridate::round_date
#' @param  changeFactor factor by which nth-range is multiplied to determine if change in val = precip, default is 0.9
#' @param  nthVal how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
#' @param  countThresh  how many time averages does precip need to be increasing to be considered valid, default is 6
#' @param  rawThresh  #expected sensitivity of instrument between individual points, default is 0.2mm
#' @param  recharge  #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25, default is 250)#' 
#' 

#' @return Cumulative primary precipitation data adjusted for diurnal temperature driven noise and 
#' evaporation, a data frame.
#' @references Ralph Wright VBA tool,Wright, R. Weighing Gauge Time Series Analysis and Noise Filtering Tool;
#'  Alberta Agriculture and Forestry: Edmonton, AB, Canada, 2021.

# changelog and author contributions / copyrights
#   Teresa Burlingame(2023-12-10)
#     original creation
#   Teresa Burlingame(2024-02-17)
#     Make original function more appropriate for different time windows for lookback and amount of rain.
#     designed to take evaporation handling as a next step and second function.
#     added precip and precipType variable. TRUE when precip detected and type of precip 'countThresh' or 'volumeThresh'
##############################################################################################

noise_removal = function(
  raw_df = raw_df, #raw data to run through function
  avg_adj = '5 minute' , #unit to average raw data
  changeFactor = 0.9, # factor by which nth-range is multiplied to determine if change in val = precip
  nthVal = 1, #how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
  countThresh = 6, #how many hours does precip need to be increasing to be considered valid
  rawThresh = 0.2, #expected sensitivity of instrument between individual points
  recharge = 250 #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
){

  # # #convert to time average of interest to match tool
  raw_df$summ <- lubridate::round_date(as.POSIXct(raw_df$tmi, tz = 'UTC'), unit = avg_adj)
  raw_df_summ <- raw_df %>% dplyr::group_by(summ) %>% dplyr::summarise(raw = mean(depth, na.rm = T))
  avgWindow <- as.numeric(stringr::str_extract(string = avg_adj, pattern = '[0-9]+'))
  
  #TODO reconsider ability to change range size for algorithm outside of 24 hour threshold. 
  
  #adjust thresholds based on avg_adj unit
  if(stringr::str_detect(avg_adj, 'minute')) {
      countThresh <- countThresh * (60/avgWindow) 
      rangeSize <- 24*(60/avgWindow)
  } else if ((stringr::str_detect(avg_adj, 'hour')) ){
      countThresh <- countThresh/avgWindow
      rangeSize <- 24/avgWindow #account for evap in last 24 hours
  } else if (!(stringr::str_detect(avg_adj, 'minute|hour'))) {
      stop('averaging unit needs to be minutes or hours')}
  
  print(rangeSize)
  print(countThresh) 
  print(avgWindow)
      
  #start counters
  rawCount <- 0
  currRow <- rangeSize #instead of 24 for hourly this will be how ever many rows encompass one day

  #initialize fields
  raw_df_summ$bench <- NA
  raw_df_summ$precip <- FALSE #add TRUE when rain detected
  raw_df_summ$precipType <- NA

  #fill in bench and as first raw data point
  raw_df_summ$bench[1:currRow] <-  raw_df_summ$raw[1] 
  
  ##loop through data to establish benchmarks 
  for (i in 1:nrow(raw_df_summ)){
    #establish nth min/max of range
    nMin <- sort(raw_df_summ$raw[i:currRow], decreasing = FALSE)[nthVal]
    nMax <- sort(raw_df_summ$raw[i:currRow], decreasing = TRUE)[nthVal]
    
    #establish envelope
    envelope <- nMax - nMin
    
    #establish benchmark
    bench <- raw_df_summ$bench[currRow-1]
    raw <- raw_df_summ$raw[currRow]
    precip <- FALSE
    precipType <- NA
    
    #missing data handling
    raw <- ifelse(is.na(raw), bench, raw) #if the initial benchmark creation was NA then this may still be a problem. 
    
    #how many measurements in a row has raw been >= bench?
    if (raw >= bench){
      rawCount <- rawCount + 1
      }else{
      rawCount <- 0
      }
    
    #if precip total increased check to see by how much
    if (raw > bench){
      rawChange <- raw - bench
      #if change was bigger than 90% of range of noise in the data or greater and > 0.2 mm  or if this is the countThresh+1 change in a row, update benchmark
      #all values supplied by original algorithm
        if ((rawChange > (changeFactor * envelope) & rawChange > rawThresh )){ #0.2 should probably be a threshold to check
          bench <- raw
          precip <- TRUE
          precipType <- 'volumeThresh'
        } else if ( rawCount > countThresh){
          bench <- raw 
          precip <- TRUE
          precipType <- 'countThresh'
          }      
    #next check if the difference is > a recharge rate, I think this is for bucket emptying and restarting
      #TB adding code to account for precip empty
    } else if ((bench - raw) > recharge){
      raw_df_summ$raw[currRow:nrow(raw_df_summ)] <- raw_df_summ$raw[currRow:nrow(raw_df_summ)]+bench 
      raw <- raw_df_summ$raw[currRow]
      bench <- raw
      } 
    
    #update in the data
    raw_df_summ$bench[currRow] <- bench
    raw_df_summ$precip[currRow] <- precip
    raw_df_summ$precipType[currRow] <- precipType
    #move to next row
    currRow <- currRow + 1
    
    #stop at end of data frame
    if (currRow == nrow(raw_df_summ)){
      raw_df_summ$bench[currRow] <- bench
      break()
    }
  }
  
  return(raw_df_summ)
}



