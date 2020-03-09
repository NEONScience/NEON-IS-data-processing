##############################################################################################
#' @title Parse SUNA data using data pasted in from def.wq.abs.corr.R

#' @author
#' Kaelin M. Cawley \email{kcawley@battelleecology.org}

#' @description
#' Definition function. Given a burst of SUNA data, the code parses the spectrum_channels 
#' and returns a vector of averaged transimittance intensities.

#' @param sunaBurst The filename assiciated with the desired calibration table [dataframe]
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
##############################################################################################
def.pars.data.suna <-
  function(sunaBurst = NULL,
           log = NULL) {
    # Intialize logging if needed
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
    splitBurst <-
      base::lapply(sunaBurst, base::strsplit, split = '\\[\\{\"int\": |\\}, \\{\"int\": |\\}\\]')
    
    parsedBurst <-
      base::as.data.frame(splitBurst[[1]], stringsAsFactors = FALSE)
    for (i in 2:base::length(parsedBurst)) {
      parsedBurst <-
        base::cbind(parsedBurst,
                    base::as.data.frame(splitBurst[[i]], stringsAsFactors = FALSE))
    }
    
    parsedBurst <- base::apply(parsedBurst, 2, base::as.numeric)
    avgBurst <- base::apply(parsedBurst, 1, base::mean, na.rm = TRUE)
    avgBurst <- avgBurst[!is.nan(avgBurst)]
    
    return(avgBurst)
  }
