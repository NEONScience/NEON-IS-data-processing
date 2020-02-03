##############################################################################################
#' @title Uncertainty for dissolved oxygen (DO) as part of the water quality transition

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create files with uncertainty information based off of the L0,
#' regularized dissolved oxygen (DO) data values.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param doData Dissolved oxygen data from the flow.cal.conv.R module [dataframe]

#' @return dataData dataframe with L0 uncertatinty appended as an additional column [dataframe]

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
def.ucrt.wq.do <- function(doData) {
  
  # Start logging
  log <- NEONprocIS.base::def.log.init()
  
  #Expect the doData from the flow.cal.conv.R script as a dataframe
  #Create an output dataframe with U_CVALA1 that can be used by flow.cal.conv.R based off of the following rules:
  ### U_CVALA1 = 0.01 if DO is <= 20 mg/l
  ### U_CVALA1 = 0.05 if DO is >20 mg/l
  doData$U_CVALA1[] <- 0.01
  doData$U_CVALA1[] <- 0.05
  
  return(doData)
  
}
