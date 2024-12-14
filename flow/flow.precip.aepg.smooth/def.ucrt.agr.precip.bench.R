##############################################################################################
#' @title Aggregate uncertainty in precipitation from depth-based algorithm

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Definition function. Aggregate uncertainty estimates derived at higher temporal frequency
#' from a depth-incrementing algorithm in which the errors have high negative autocorrelation (the 
#' error is largely in precipitation timing such that the error in subsequent measurements offsets 
#' when computing the sum of precipitation). The uncertainty in total precipitation over the given window
#' is the sum of uncertainties of the benchmarks at the beginning and end points, added in quadrature. 
#' The exception is when the depth resets for any reason, in which case the uncertainty of the precipitation
#' sum is computed between reset points and added in quadrature.

#' @param bench Numeric array of the depth benchmark (smoothed depth value), constrained to the time period 
#' over which to aggregate the uncertainty. 
#'
#' @param ucrtBench Numeric array matching the length of bench representing the uncertainty (standard deviation)
#' of the benchmark value.  
#'
#' @return A single value representing the uncertainty (standard deviation) of the precipitation sum 
#' over the length of the benchmark. Note: If any benchmarks are NA, the reported uncertainty over the
#' interval will be NA
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' bench <- c(0, 1, 1, 1, 2, 2, 2, 3)
#' ucrtBench <- abs(rnorm(length(bench)))
#' ucrtSum <- def.ucrt.agr.precip.aepg(bench,ucrtBench)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2024-10-31)
#     Initial creation
##############################################################################################
def.ucrt.agr.precip.bench <- function(bench,ucrtBench){
  
  # Are any benchmarks NA? Return NA. 
  # The creation of any benchmark dataset needs to handle missing depth values such that an 
  # acceptable amount of missing raw depth data is smoothed over with a continuous benchmark. 
  # An NA in the benchmark means precip cannot be computed over this aggregation interval.
  if(any(is.na(bench))){
    return(as.numeric(NA))
  }
  
  numData <- length(bench)
  if(numData < 2){
    ucrtAgr <- as.numeric(NA)
    return(ucrtAgr)
  } else if (numData == 2){
    ucrtAgr <- sqrt(ucrtBench[1]^2 + ucrtBench[2]^2)
    return(ucrtAgr)
  }
  
  # Compute uncertainty for each differencing leg (i.e. period of same or increasing benchmark)
  benchDiff <- diff(bench)
  setBrk <- c(0,which(benchDiff < 0),numData)
  ucrtBrk <- rep(as.numeric(NA),length(setBrk)-1)
  for (idxBrk in seq_len(length(setBrk)-1)){
    idxLegBgn <- setBrk[idxBrk]+1
    idxLegEnd <- setBrk[idxBrk+1]
    if((idxLegEnd-idxLegBgn) == 0){
      next
    }
    ucrtBrk[idxBrk] <- sqrt(ucrtBench[idxLegBgn]^2 + ucrtBench[idxLegEnd]^2)
  }
  if(all(is.na(ucrtBrk))){
    ucrtAgr <- as.numeric(NA)
  } else {
    ucrtAgr <- sqrt(sum(ucrtBrk^2,na.rm=T))
  }

  # Add in uncertainty for continuous periods of decreasing benchmark (bench declines each consecutive value)
  # These instances were ignored in the above computation. 
  setBrk <- c(which(benchDiff < 0))
  if(length(setBrk) > 1){
    ucrtBrk <- rep(as.numeric(NA),length(setBrk)) # initialize a maximum set of uncertainties we will add in quadrature. Uncertainties will be populated for the ending index of each leg.
    idxLegBgn <- setBrk[1] # initialize start of the consecutive benchmark declines
    for (idxBrk in seq_len(length(setBrk)-1)){
      # Is the next index consecutive?
      if(setBrk[idxBrk + 1] == (setBrk[idxBrk] + 1)){
        # Next is consecutive. Keep going. 
        next
      } else {
        # We have reached the end of this consecutive decline. Record uncertainty for the continuous leg and reset.
        idxLegEnd <- setBrk[idxBrk]+1
        
        # Must be at least 2 consecutive drops
        if(idxLegEnd-idxLegBgn > 1){
          # Record uncertainty for this leg
          ucrtBrk[idxBrk] <- sqrt(ucrtBench[idxLegBgn]^2 + ucrtBench[idxLegEnd]^2)
        }
        
        # Reset the start of the consecutive benchmark declines
        idxLegBgn <- setBrk[idxBrk+1]
        
      }
    }
    
    # Combine uncertainty with above
    ucrtAgr <- sqrt(sum(ucrtAgr^2, sum(ucrtBrk^2,na.rm=T),na.rm=T))
  }
  
  return(ucrtAgr)
}