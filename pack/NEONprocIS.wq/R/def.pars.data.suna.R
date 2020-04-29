##############################################################################################
#' @title Parse SUNA data using data pasted in from def.wq.abs.corr.R

#' @author
#' Kaelin M. Cawley \email{kcawley@battelleecology.org}

#' @description
#' Definition function. Given a burst of SUNA data, the code parses the spectrum_channels
#' and returns a vector of averaged transimittance intensities.

#' @param sunaBurst The spectrum channel data from the suna [list of numeric vectors]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A vector of unitless transmittance values

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' # TBD

#' @export

# changelog and author contributions / copyrights
#   Kaelin M. Cawley (2020-03-04)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     adjusted to support data returned by parquet reader
##############################################################################################
def.pars.data.suna <-
  function(sunaBurst = NULL,
           log = NULL) {
    # Intialize logging if needed
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    # Merge the burst data from all observations into one data frame
    parsedBurst <-
      base::lapply(sunaBurstParq, base::as.data.frame, stringsAsFactors = FALSE)
    parsedBurst <- base::do.call(base::cbind, parsedBurst)
    parsedBurst <-
      base::as.data.frame(base::lapply(parsedBurst, base::as.numeric))
    
    # Average observations for each channel
    avgBurst <- base::rowMeans(parsedBurst, na.rm = TRUE)
    avgBurst <- avgBurst[!base::is.nan(avgBurst)]
    
    return(avgBurst)
  }
