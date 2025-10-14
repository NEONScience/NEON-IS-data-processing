
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

library(rlang)
library(devtools)
library(testthat)
library(usethis)
library(covr)
library(dplyr)
library(arrow)
library(stringr)

# Required to import install_local function.
library(remotes)

load_package <- function(package_name) {
  if (!require(package_name, character.only = TRUE)) {
    path = paste0("../../../pack/", package_name)
    suppressPackageStartupMessages(
      install_local(path)
    )
  }
}

load_neon_base <- function() {
  load_package("NEONprocIS.base")
}

load_neon_calibration <- function() {
  load_package("NEONprocIS.cal")
}

load_neon_publication <- function() {
  load_package("NEONprocIS.pub")
}

load_neon_qaqc <- function() {
  load_package("NEONprocIS.qaqc")
}

load_neon_statistics <- function() {
  load_package("NEONprocIS.stat")
}

load_neon_water_quality <- function() {
  load_package("NEONprocIS.wq")
}
