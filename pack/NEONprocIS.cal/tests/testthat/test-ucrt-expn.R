##############################################################################################
#' @title Unit test of def.ucrt.expn.R, compute expanded measurement uncertainty (95% confidence)

#' @description
#' Run unit tests for def.ucrt.expn.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' @param ucrt Numeric data vector of combined measurement uncertainty (1 sigma)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with a single numeric column: \cr
#' \code{ucrtExpn} - expanded measurement uncertainty

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords combined uncertainty

#' @examples
#' ucrtComb <- c(1,2,1,1,2)
#' NEONprocIS.cal::def.ucrt.expn(ucrtComb=ucrtComb)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.cnst}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.mult}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.comb}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-07-01)
#     original creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.expn.R\n")

# Unit test of def.ucrt.expn.R
test_that("Unit test of def.ucrt.expn.R", {
  # Happy path 1
  
  ucrtComb <- c(1, 2, 1, 1, 2)
  
  ucrt_expn_dfReturned <- NEONprocIS.cal::def.ucrt.expn(ucrtComb)
  expect_true ((is.data.frame(ucrt_expn_dfReturned)) && !(is.null(ucrt_expn_dfReturned)))
  
  # Happy path 2 - NULL is passed for parameter, ucrtComb
  
  ucrtComb = NULL
  ucrt_expn_dfReturned <- try (NEONprocIS.cal::def.ucrt.expn(ucrtComb), silent = TRUE) 
  testthat::expect_true((class(ucrt_expn_dfReturned)[1] == "try-error"))
  
})
