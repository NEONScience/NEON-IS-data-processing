##############################################################################################
#' @title Unit test of def.pars.data.suna.R, parse SUNA data using data pasted in from def.wq.abs.corr.R
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for def.pars.data.suna.R
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values
#'
#' In def.pars.data.suna.R, given a burst of SUNA data, the code parses the spectrum_channels
#' and returns a vector of averaged transimittance intensities.

#' @param sunaBurst The spectrum channel data from the suna [list of numeric vectors]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A vector of unitless transmittance values

#'
#' @references Currently none
#'
#' @keywords Currently none
#'
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.wq\")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.wq")
#'
# changelog and author contributions / copyrights
#   Mija Choi (2020-09-10)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.pars.data.suna.R\n")

# Unit test of def.pars.data.suna.R
test_that("Unit test of def.pars.data.suna.R", {


  testDir = "testdata/"
  testFile = "sunaBurst.csv"
  testFilesPath <- paste0(testDir, testFile)
  sunaBurst <- c(read.csv(testFilesPath))
  
  avg_burst <- NEONprocIS.wq::def.pars.data.suna (sunaBurst = sunaBurst,log = log) 

  expect_true ((is.vector(avg_burst)) && !(is.null(avg_burst)) && length(avg_burst) == 256)
  
  })