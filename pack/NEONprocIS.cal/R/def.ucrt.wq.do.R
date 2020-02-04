##############################################################################################
#' @title Uncertainty for dissolved oxygen (DO) concentration (mg/L) as part of the water
#' quality transition

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create files with uncertainty
#' information based off of the L0 dissolved oxygen (DO) concentration data values according
#' to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param data Dissolved oxygen (DO) concentration data from the flow.cal.conv.R module [vector]

#' @return dataframe with L0 uncertatinty column(s) [dataframe]

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #TBD

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################
def.ucrt.wq.do.conc <- function(data, cal) {
  # Start logging
  log <- NEONprocIS.base::def.log.init()
  
  #Check that we have more than 0 rows of data
  if (length(data) < 1) {
    log$fatal('DO data length less than 1, no data to calculate uncertainty with.')
  }
  
  #Cal is not needed for this function
  #It's just a placeholder input to allow the calibration module to be more generic
  
  #Create the output dataframe
  outputNames <- c("dissolveOxygenConcUnc")
  outputDF <-
    base::as.data.frame(base::matrix(
      nrow = length(data),
      ncol = length(outputNames),
      data = NA
    ),
    stringsAsFactors = FALSE)
  names(outputDF) <- outputNames
  log$debug('Output dataframe for dissolvedOxygenConcUnc created.')
  
  #Create an output dataframe with U_CVALA1 based off of the following rules:
  ### U_CVALA1 = 0.01 if DO is > 0 & <= 20 mg/l according to the manual
  ### U_CVALA1 = 0.05 if DO is >20 mg/l & < 50 mg/l according to the manual
  outputDF$dissolveOxygenConcUnc[outputDF$dissolveOxygenConcUnc <= 20] <-
    0.01
  log$debug('Low range DO uncertainty populated.')
  
  outputDF$dissolveOxygenConcUnc[outputDF$dissolveOxygenConcUnc > 20] <-
    0.05
  log$debug('High range DO uncertainty populated.')
  
  return(outputDF)
  
}
