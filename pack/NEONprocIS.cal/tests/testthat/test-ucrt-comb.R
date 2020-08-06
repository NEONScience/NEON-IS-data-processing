##############################################################################################
#' @title Unit test of def.ucrt.comb.R, combine measurement uncertainties by adding them in quadrature

#' @description
#' Run unit tests for def.ucrt.comb.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' @param ucrt Numeric data frame of uncertainties to be combined.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with a single numeric column:\cr
#' \code{ucrtComb} - combined measurement uncertainty resulting by adding in quadrature all the
#' uncertainties provided in the input data frame \code{ucrt}.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords combined uncertainty

#' @examples
#' ucrt <- data.frame(ucrtA=c(1,2,1,1,2),ucrtB=c(5,6,7,8,9),stringsAsFactors=FALSE)
#' NEONprocIS.cal::def.ucrt.comb(data=data,ucrtCoef=ucrtCoef)
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-07-01)
#     original creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.comb.R\n")

# Unit test of def.ucrt.comb.R
test_that("Unit test of def.ucrt.comb.R", {
  # Happy path 1
  
  ucrt <- data.frame(ucrtA=c(1,2,1,1,2),ucrtB=c(5,6,7,8,9),stringsAsFactors=FALSE)
  
  ucrt_comb_dfReturned <- NEONprocIS.cal::def.ucrt.comb(ucrt)
  
  expect_true ((is.data.frame(ucrt_comb_dfReturned)) && !(is.null(ucrt_comb_dfReturned)))
  
  # Happy path 2 - NULL is passed for parameter, ucrtComb
  
  ucrt = NULL
  ucrt_comb_dfReturned <- try (NEONprocIS.cal::def.ucrt.comb(ucrt), silent = TRUE) 
  testthat::expect_true((class(ucrt_comb_dfReturned)[1] == "try-error"))
  
})
