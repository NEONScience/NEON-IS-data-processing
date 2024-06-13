##############################################################################################
#' @title Primary Precipitation Noise removal Algorithm

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr


#' @description Function to adjust primary precipitation data to account for noise and evaporation in the sensor
#' The needed input is a data frame with timestamps with variable 'tmi' and a precip variable 'depth'
#' Function and defaults based on Ralph Wright tool. Evaporation adjustment a NEON add. 

#' @param  raw_df raw_df  #raw data to run through function
#' @param  avg_adj unit to average raw data, defaults to hourly
#' @param  rangeSize number of cells of data to compare, default is 24 
#' @param  changeFactor factor by which nth-range is multiplied to determine if change in val = precip, default is 0.9
#' @param  nthVal how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
#' @param  countThresh  how many time averages does precip need to be increasing to be considered valid, default is 6
#' @param  rawThresh  #expected sensitivity of instrument between individual points, default is 0.2mm
#' @param  recharge  #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25, default is 250)
#' nth Median. Default to median
#' 
#' 

#' @return Cumulative primary precipitation data adjusted for diurnal temperature driven noise and 
#' evaporation, a data frame.
#' @references Ralph Wright VBA tool,Wright, R. Weighing Gauge Time Series Analysis and Noise Filtering Tool;
#'  Alberta Agriculture and Forestry: Edmonton, AB, Canada, 2021.

# changelog and author contributions / copyrights
#   Teresa Burlingame(2023-12-10)
#     original creation
#   Teresa Burlingame(2024-02-17)
#     save file to just be original function from paper, no adjustments (except needed handling of empty bucket)
##############################################################################################


noise_removal = function(
  raw_df = raw_df, #raw data to run through function
  rangeSize = 24, #number of hours of data to compare
  changeFactor = 0.9, # factor by which nth-range is multiplied to determine if change in val = precip
  nthVal = 1, #how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
  countThresh = 6, #how many hours does precip need to be increasing to be considered valid
  rawThresh = 0.2, #expected sensitivity of instrument between individual points
  avg_adj = 'hour', #unit to average raw data
  recharge = 250 #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
){

  # # #convert to hourly to match tool
  raw_df$summ <- lubridate::floor_date(as.POSIXct(raw_df$tmi), unit = avg_adj)
  raw_df_summ <- raw_df %>% dplyr::group_by(summ) %>% dplyr::summarise(raw = mean(depth, na.rm = T))
  
  #start counters
  rawCount <- 0
  currRow <- rangeSize 

  #initialize fields
  raw_df_summ$bench <- NA
  
  #fill in bench and as first raw data point
  raw_df_summ$bench[1:currRow] <-  raw_df_summ$raw[1] 

  
  ##loop through data to establish benchmarks 
  for (i in 1:nrow(raw_df_summ)){
    #establish nth min/max of range
    nMin <- sort(raw_df_summ$raw[i:currRow], decreasing <- FALSE)[nthVal]
    nMax <- sort(raw_df_summ$raw[i:currRow], decreasing <- TRUE)[nthVal]
    nMean <- mean(raw_df_summ$raw[i:currRow], na.rm = T)
    nMedian <- median(raw_df_summ$raw[i:currRow], na.rm = T)
    
    #establish envelope
    envelope <- nMax - nMin
    
    #establish benchmark
    bench <- raw_df_summ$bench[currRow-1]
    raw <- raw_df_summ$raw[currRow]
    
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
        if ((rawChange > (changeFactor * envelope) & rawChange > rawThresh ) | rawCount > countThresh){ #0.2 should probably be a threshold to check
          bench <- raw 
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



