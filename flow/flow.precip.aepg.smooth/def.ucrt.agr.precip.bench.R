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
#' over the length of the benchmark. 
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
  
  # Compute uncertainty for each differencing leg (i.e. period of same or increasing benchmark)
  benchDiff <- diff(bench)
  setBrk <- c(0,which(is.na(benchDiff) | benchDiff < 0),length(bench))
  ucrtBrk <- rep(0,length(setBrk)-1)
  for (idxBrk in seq_len(length(setBrk)-1)){
    idxLegBgn <- setBrk[idxBrk]+1
    idxLegEnd <- setBrk[idxBrk+1]
    if((idxLegEnd-idxLegBgn) == 0){
      next
    }
    ucrtBrk[idxBrk] <- sqrt(ucrtBench[idxLegBgn]^2 + ucrtBench[idxLegEnd]^2)
  }
  ucrtAgr <- sqrt(sum(ucrtBrk^2))
  
  return(ucrtAgr)
}