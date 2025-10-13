
##############################################################################################
#' @title Load needed test dependencies

#' @author
#' Robert Markel\email{rmarkel@battelleecology.org} \cr

#' @description Load needed test dependencies
#'
#'
#' @return No data returned.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' load_test_dependencies()

#' @seealso None
#'
# changelog and author contributions / copyrights
#   Robert Markel (2025-10-13)
#     Original Creation
##############################################################################################

load_test_dependencies <- function() {
  library(rlang)
  library(devtools)
  library(testthat)
  library(usethis)
  library(covr)
  library(dplyr)
  library(arrow)
  library(stringr)
  
  # Needed for install_local function.
  library(remotes)
  
  if (!require("NEONprocIS.base")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.base")
    )
  }

  if (!require("NEONprocIS.cal")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.cal")
    )
  }

  if (!require("NEONprocIS.pub")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.pub")
    )
  }

  if (!require("NEONprocIS.qaqc")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.qaqc")
    )
  }
  
  if (!require("NEONprocIS.stat")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.stat")
    )
  }

  if (!require("NEONprocIS.wq")) {
    suppressPackageStartupMessages(
      install_local("../../../pack/NEONprocIS.wq")
    )
  }
}
