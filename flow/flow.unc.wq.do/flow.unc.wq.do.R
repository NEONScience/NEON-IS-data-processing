##############################################################################################
#' @title Uncertainty for dissolved oxygen (DO) as part of the water quality transition

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Create files with uncertainty information based off of the L0,
#' regularized dissolved oxygen (DO) data values.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Directories for uncertainty values

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
flow.unc.wq.do <- function(doDataRegularized) {
  # Start logging
  log <- NEONprocIS.base::def.log.init()
  
  #Set some directory information similar to all the other scripts to access the pachyderm repo(s)
  
  #Read in the L0, regularized DO data
  
  #Create an output file of U_CVALA1 that can be used by flow.cal.conv.R based off of the following rules:
  ### U_CVALA1 = 0.01 if DO is <= 20 mg/l
  ### U_CVALA1 = 0.05 if DO is >20 mg/l
  
  #I think this would have to end up being time range based, rather than DO value based
  
}
