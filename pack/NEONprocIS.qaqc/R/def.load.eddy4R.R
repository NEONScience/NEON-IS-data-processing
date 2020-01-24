##############################################################################################
#' @title Dummy function to ensure eddy4R.base & eddy4R.qaqc are captured in the renv lockfile

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Load eddy4R.base and eddy4R.qaqc to ensure that these packages are
#' documented in the renv lockfile.

#' @return eddy4R.base and eddy4R.qaqc are loaded

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-01-23)
#     original creation
##############################################################################################
def.load.eddy4R <- function(){
  
  library(eddy4R.base)
  library(eddy4R.qaqc)
  
  
}
