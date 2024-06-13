##############################################################################################
#' @title Primary Precipitation Noise removal Algorithm

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr


#' @description Function to adjust primary precipitation data to account for noise and evaporation in the sensor
#' The needed input is a data frame with timestamps with variable 'tmi' and a precip variable 'depth'
#' Function and defaults based on Ralph Wright tool. Evaporation adjustment a NEON add. 
#' This function also includes code to adjust 'raw' values in the event that a bucket is emptied. 
#' If a bucket is emptied all remaining raw values have current benchmark added to them. 

#' @param  raw_df raw_df  #raw data to run through function
#' @param  avg_adj unit to average raw data, defaults to hourly
#' @param  rangeSize number of cells of data to compare, default is 24 
#' @param  changeFactor factor by which nth-range is multiplied to determine if change in val = precip, default is 0.9
#' @param  nthVal how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
#' @param  countThresh  how many time averages does precip need to be increasing to be considered valid, default is 6
#' @param  rawThresh  #expected sensitivity of instrument between individual points, default is 0.2mm
#' @param  recharge  #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25, default is 250)
#' @param  evap_var #which statistic to use to adjust evaporation accounting on precipitation event, choice between nth Min, nthMean  
#' nth Median. Default to median
#' 
#' 

#' @return Cumulative primary precipitation data adjusted for diurnal temperature driven noise and 
#' evaporation, a data frame.
#' @references Ralph Wright VBA tool,Wright, R. Weighing Gauge Time Series Analysis and Noise Filtering Tool;
#'  Alberta Agriculture and Forestry: Edmonton, AB, Canada, 2021.

# changelog and author contributions / copyrights
#   Teresa Burlingame(2023-12-10)
#     original creation, has evaporation handling
##############################################################################################

noise_removal = function(
  raw_df = raw_df, #raw data to run through function
  rangeSize = 24, #number of hours of data to compare
  changeFactor = 0.9, # factor by which nth-range is multiplied to determine if change in val = precip
  nthVal = 1, #how wide of a range do we want to compare for noise (1 is essentially min and max/full range)
  countThresh = 6, #how many hours does precip need to be increasing to be considered valid
  rawThresh = 0.2, #expected sensitivity of instrument between individual points
  avg_adj = '1 hour', #unit to average raw data
  recharge = 250, #if raw data was this much less than bench mark likely a bucket empty/recalibration (original was 25)
  evap_var = c('nMean', 'nMedian', 'nMin')[2] #which variable to use to adjust evaporation accounting on precipitation event
){

  # # #convert to hourly to match tool
  raw_df$summ <- lubridate::floor_date(as.POSIXct(raw_df$tmi), unit = avg_adj)
  raw_df_summ <- raw_df %>% dplyr::group_by(summ) %>% dplyr::summarise(raw = mean(depth, na.rm = T))
  
  #start counters
  rawCount <- 0
  currRow <- rangeSize 
  evap <- 0
  
  evap_list <- c('nMean', 'nMedian', 'nMin')
  evap_ind <- unlist(which(evap_list == evap_var))
  
  #initialize fields
  raw_df_summ$bench <- NA
  raw_df_summ$adj_bench <- NA
  raw_df_summ$nMin <- NA
  raw_df_summ$nMean <- NA
  raw_df_summ$nMedian <- NA
  raw_df_summ$precip <- FALSE #add TRUE when rain detected
  raw_df_summ$precipType <- NA
  raw_df_summ$evap <- FALSE 
  
  
  #fill in bench and adjunch bench as first raw data point
  raw_df_summ$bench[1:currRow] <-  raw_df_summ$raw[1] 
  raw_df_summ$adj_bench[1:currRow] <-  raw_df_summ$raw[1] 
  
  
  ##loop through data to establish benchmarks 
  for (i in 1:nrow(raw_df_summ)){
    #establish nth min/max of range
    nMin <- sort(raw_df_summ$raw[i:currRow], decreasing <- FALSE)[nthVal]
    nMax <- sort(raw_df_summ$raw[i:currRow], decreasing <- TRUE)[nthVal]
    nMean <- mean(raw_df_summ$raw[i:currRow], na.rm = T)
    nMedian <- median(raw_df_summ$raw[i:currRow], na.rm = T)
    
    #adjust to test corrections. 
    evapAdj <- c(nMean, nMedian, nMin)[evap_ind]
    
    #establish envelope
    envelope <- nMax - nMin
    
    #establish benchmark
    bench <- raw_df_summ$bench[currRow-1]
    adj_bench <-  raw_df_summ$adj_bench[currRow -1]
    prev_bench <- raw_df_summ$bench[currRow-1]
    raw <- raw_df_summ$raw[currRow]
    precip <- FALSE
    precipType <- NA
    evap <- FALSE
    
    
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
    
  #### TB added evaporation handling
    #benchmark got adjusted to raw & it was not adjusted in the previous 24hrs
    if ((precip & !any(raw_df_summ$evap[i:currRow-1]))){ #was evap accounted for in last 24h?
      print(raw_df_summ$summ[currRow])
      print(paste0('evap accounting, bench adjusted by evap: ', prev_bench - evapAdj, ' and rain: ',  bench - prev_bench))
      # add the evap in and #get the new bench diff from the old bench
      adj_bench <- adj_bench + (prev_bench - evapAdj) + (bench - prev_bench)# add the evap in and #get the new bench diff from the old bench
      evap <- TRUE
      raw_df_summ$nMin[currRow] <- nMin #just for plotting
      raw_df_summ$nMedian[currRow] <- nMedian #just for plotting
      raw_df_summ$nMean[currRow] <- nMean #just for plotting
      
    } else if  (bench == raw & any(raw_df_summ$raw[i:currRow-1] == raw_df_summ$bench[i:currRow-1])) {
      print(raw_df_summ$summ[currRow])
      print(paste0('evap accounted in previous 24h, bench adjusted by rain only: ',  bench - prev_bench))
      adj_bench <-  adj_bench + (bench - prev_bench) #just rain adjustment bc a previous rain got the evap
    } else {
      adj_bench <- adj_bench #else maintain
    }
    
    ###end evap handling
    
    
    #update in the data
    raw_df_summ$bench[currRow] <- bench
    raw_df_summ$adj_bench[currRow] <- adj_bench
    #update in the data
    raw_df_summ$bench[currRow] <- bench
    raw_df_summ$adj_bench[currRow] <- adj_bench
    raw_df_summ$precip[currRow] <- precip
    raw_df_summ$precipType[currRow] <- precipType
    raw_df_summ$evap[currRow] <- evap
    #move to next row
    currRow <- currRow + 1

    #stop at end of data frame
    if (currRow == nrow(raw_df_summ)){
      raw_df_summ$bench[currRow] <- bench
      raw_df_summ$adj_bench[currRow] <- adj_bench
      print(paste0('Evaporation adjustment added in ', adj_bench - bench, ' mm precip.'))
      break()
    }
  }
  
  return(raw_df_summ)
}



