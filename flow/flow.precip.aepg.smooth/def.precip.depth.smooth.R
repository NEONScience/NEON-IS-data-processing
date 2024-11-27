##############################################################################################
#' @title Apply smoothing algorithm to noisy precipitation collector depths 

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Definition function. Smooth noisy depth time series measured from a weighing gauge 
#' precipitation collector in order to compute precipitation from it.

#' @param dateTime POSIXct vector. Date-times associated with the gaugeDepth timeseries
#' @param gaugeDepth Numeric vector. Depth timeseries of the precipitation collector 
#' (typically in mm, but it doesn't matter) 
#' @param RangeSize Integer value. The number of data points considered to span a 
#' full cycle of periodic noise, typically the number of data points in 1 day, 
#' but could be multiples of 1 day. Changes in the quantile specifiec in the input Quant  
#' over this window generally drive changes in the benchmark output by this function. 
#' @param Quant Numeric fraction (single value). The quantile to track over the RangeSize throughout 
#' the timeseries. Use 0.5 for median (default)
#' @param ThshCount Numeric value. How many data points does the precip depth need to be elevated to
#' consider a precipitation event to have occurred (at some point in the past)? 
#' @param Envelope Numeric value. Daily noise range of precipitation depth
#' @param ThshChange Numeric value. Expected sensitivity of instrument between individual points. 
#' Default is 0.2 (mm). 
#' @param ChangeFactor Numeric fraction (single value) by which Envelope is multiplied to 
#' determine if change in depth is precipitation, default is 1.
#' @param ChangeFactorEvap Numeric fraction (single value) by which Envelope is multiplied 
#' to determine when a negative change in depth is considered evaporation, and the benchmark 
#' moved down accordingly. Default is 0.5.
#' @param Recharge Numeric value. If raw data drops by this much or more, assume a bucket 
#' empty/recalibration has occurred and reset the benchmark.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A data frame of the following:
#' bench: Numeric. Smoothed precipitation depth benchmark, in which increases 
#'        represent precipitation events and decreases should be ignored
#' precip: Logical. TRUE to indicate that precipitation has occurred 
#' precipType: Character. A description of what caused the change. Values include:
#'             volumeThresh: The change in depth was larger than the noise envelope and also 
#'                           greater than the expected instrument sensitivity to rain
#'             volumeThreshContinued: It had been raining due to volumeThresh and the gauge depth 
#'                           continued to increase above the expected instrument sensitivity
#'             ThshCount: The gauge depth exceeded the previous benchmark for the time threshold
#'                        (ThshCount) or longer
#'             ThshCountBackFilledToStart: The period of time before triggering ThshCount that the
#'                        algorithm determined the precipitation to already have been occurring
#'             BackAdjToMedNextDay and postPrecipAdjToMedNextDay: A back-adjusted benchmark to the 
#'                        specified quantile (Quant) over the specific RangeSize to avoid 
#'                        overestimating actual precip
#'             EvapAdj: Benchmark adjusted downward due to detected evaporation
#'             ExcludeBeforeRecharge: The period of time in which the benchmark was held constant
#'                                    before a recharge event to avoid corruption by bucket emptying
#'                                    and recalibration procedures
#'  evapDetectedQF: Binary 0 or 1. Set to 1 when the benchmark was downward-adjusted due to detected
#'                  evaporation

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' This algorithm was adapted from code provided by Ralph Wright and described in:
#' Barnes, C.; Hopkinson, C. Quality Control Impacts on Total Precipitation Gauge Records for 
#' Montane Valley and Ridge Sites in SW Alberta, Canada. Data 2022, 7 (6), 73. 
#' https://doi.org/10.3390/data7060073.

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame & Cove Sturtevant (2024-06-25)
#     Initial creation
##############################################################################################
def.precip.depth.smooth <- function(dateTime,
                                    gaugeDepth,
                                    RangeSize,
                                    Quant=0.5,
                                    ThshCount,
                                    Envelope,
                                    ThshChange=0.2,
                                    ChangeFactor=1,
                                    ChangeFactorEvap=0.5,
                                    Recharge
){
  
  # start counters for smoothing algorithm
  rawCount <- 0
  timeSincePrecip <- NA
  currRow <- RangeSize 
  
  # Intialize
  numData <- base::length(strainGaugeDepth)
  outBench <- base::rep(base::as.numeric(NA),numData)
  outBench[1:currRow] <-  stats::quantile(gaugeDepth[1:currRow],Quant,na.rm=TRUE)
  outPrecip <- base::rep(FALSE,numData)
  outPrecipType <- base::rep(as.character(NA),numData)
  evapDetectedQF <- base::rep(0,numData)
  
  
  ##loop through data to establish benchmarks 
  skipping <- FALSE
  for (i in base::seq_len(1:numData)){
    
    # Check for at least 1/2 a day of non-NA values. 
    # If not, get to the next point at which we have at least 1/2 day and start fresh
    if(base::sum(base::is.na(gaugeDepth[i:currRow])) > .5*RangeSize){
      
      # Find the last non-NA value 
      setEval <- i:currRow
      idxEnd <- setEval[tail(which(!is.na(gaugeDepth[setEval])),1)]
      
      if(length(idxEnd) > 0){
        # Remove the benchmark extending into the gap
        outBench[(idxEnd+1):currRow] <- NA
      }
      
      # Skip until there is enough data
      skipping <- TRUE
      currRow <- currRow + 1
      
      #stop at end of data frame
      if (currRow == numData){
        break()
      } else {
        next
      }
      
    } else if (skipping) {
      
      # Find the first non-NA value to begin at
      setEval <- i:currRow
      idxBgnNext <- setEval[head(which(!is.na(gaugeDepth[setEval])),1)]
      
      # Re-establish the benchmark
      outBench[idxBgnNext:currRow] <- stats::quantile(gaugeDepth[idxBgnNext:currRow],Quant,na.rm=TRUE)
      timeSincePrecip <- NA
      skipping <- FALSE
    }
    
    
    if(!is.na(timeSincePrecip)){
      timeSincePrecip <- timeSincePrecip + 1
    }
    recentPrecip <- base::any(outPrecip[i:currRow])
    
    #establish benchmark
    bench <- outBench[currRow-1]
    raw <- gaugeDepth[currRow]
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
    raw_med_lastRngSz <- quantile(gaugeDepth[i:currRow],Quant,na.rm=TRUE)
    raw_min_lastRngSz <- min(gaugeDepth[i:currRow],na.rm=TRUE)
    
    
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
        
      } else if (grepl('volumeThresh',x=outPrecipType[currRow-1]) && rawChange > ThshChange){
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
          rawIdx <- gaugeDepth[idx]
          rawIdx <- ifelse(is.na(rawIdx), benchNext, rawIdx)
          if(rawIdx < benchNext){
            benchNext <- rawIdx
            outBench[idx] <- rawIdx
          } else {
            outBench[idx] <- benchNext
          }
          
          # Record rain stats
          outPrecip[idx] <- precip
          outPrecipType[idx] <- 'ThshCountBackFilledToStart'
        }
        
      } else if (rawCount >= ThshCount){
        # Or, if it continues to drizzle and raw is continuing to rise, keep saying that it's raining
        bench <- raw 
        precip <- TRUE
        timeSincePrecip <- 0
        precipType <- 'ThshCount'
      }
    }
    if (!is.na(timeSincePrecip) && timeSincePrecip == RangeSize && raw > (outBench[i-1]-Recharge)){ 
      
      # Exactly one RangeSize after rain ends, and if the depth hasn't dropped precipitously (as defined by the Recharge threshold),
      # back-adjust the benchmark to the Quant of the last day to avoid overestimating actual precip
      
      bench <- raw_med_lastRngSz
      outBench[i:currRow] <- bench
      outPrecipType[i:currRow] <- "postPrecipAdjToMedNextDay"
      
      idxBgn <- i-1
      keepGoing <- TRUE
      while(keepGoing == TRUE) {
        
        if(is.na(outPrecip[idxBgn]) || outPrecip[idxBgn] == FALSE){
          # Stop if we are past the point where the precip started
          keepGoing <- FALSE
        } else if(outBench[idxBgn] > bench){
          outBench[idxBgn] <- bench
          outPrecipType[idxBgn] <- paste0(outPrecipType[idxBgn],"BackAdjToMedNextDay")
          idxBgn <- idxBgn - 1
        } else {
          keepGoing <- FALSE
        }
      }
    } else if ((raw < bench) && (bench-raw_med_lastRngSz) > ChangeFactorEvap*Envelope && !recentPrecip){
      # If it hasn't rained in at least 1 day, check for evaporation & reset benchmark if necessary
      
      bench <- raw_min_lastRngSz # Set to the min of the last day to better track evap
      precipType <- 'EvapAdj'
      if(idxSurr == 0){
        evapDetectedQF[currRow] <- 1
      }
      
    } else if ((bench - raw) > Recharge){
      # If the raw depth has dropped precipitously (as defined by the recharge rage), assume bucket was emptied. Reset benchmark.
      bench <- raw
      
      # Get rid of a few hours before the recharge. This is when calibrations are occurring and strain gauges are being replaced.
      # Set the benchmark constant to the point 3 hours before the recharge
      setAdj <- dateTime > (dateTime[currRow] - as.difftime(3,units='hours')) &
        dateTime < dateTime[currRow]
      idxSet <- head(which(setAdj),1) - 1
      if (idxSet < 1){
        outBench[setAdj] <- NA
        outPrecip[setAdj] <- NA
        outPrecipType[setAdj] <- NA
      } else {
        outBench[setAdj] <- outBench[idxSet]
        outPrecip[setAdj] <- outPrecip[idxSet]
        outPrecipType[setAdj] <- "ExcludeBeforeRecharge"
      }
      
    } 
    
    #update in the data
    outBench[currRow] <- bench
    outPrecip[currRow] <- precip
    outPrecipType[currRow] <- precipType
    
    # Move to next row
    currRow <- currRow + 1
    
    # Stop at end of data frame
    if (currRow == numData){
      outBench[currRow] <- bench
      rpt <- base::data.frame(bench=outBench,
                              precip=outPrecip,
                              precipType=outPrecipType,
                              evapDetectedQF=evapDetectedQF,
                              stringsAsFactors=FALSE
      )
      return(rpt)
    }
    
  } # End loop around data
} # End function
