##############################################################################################
#' @title Unit test of def.ucrt.fdas.rstc.R

#' @description
#' Run unit tests for def.ucrt.fdas.rstc.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.fdas.rstc.R for the details of the function.

#
#' @param data Numeric vector of raw resistance measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal and info$ucrt,
#' which are data frames of calibration coefficients and uncertainty coeffcients, respectively.
#' Columns of these data frames are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrt$raw} - raw reading value (i.e. same as input data)\cr
#' \code{ucrt$dervCal} - 1st derivative of calibration function evaluated at raw reading value (e.g.
#' partial derivative of a temperature measurement with respect to the resistance reading)\cr
#' \code{ucrt$ucrtFdas} - standard uncertainty of individual measurement introduced by the Field DAS \cr

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2020-06-15)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.fdas.rstc.R\n")

# Unit test of def.ucrt.fdas.rstc.R
test_that("Unit test of def.ucrt.fdas.rstc.R", {
  # Happy path
  #
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  fileCal = "calibration3.xml"
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile = fileCal, Vrbs = TRUE)
  data = c(0.9, 0.88)
  
  # Happy Path- All params passed
  ufrstcDf_returned <- NEONprocIS.cal::def.ucrt.fdas.rstc (data = data,
                                        infoCal = infoCal)
  
  expect_true ((is.data.frame(ufrstcDf_returned)) &&
                 !(is.null(ufrstcDf_returned)))
  # The output is a data frame having Name, Value, and .attrs
  #  Sad path - no parameters passed
  
  ufrstcDf_returned <- NEONprocIS.cal::def.ucrt.fdas.rstc ()
  
  expect_true ((is.data.frame(ufrstcDf_returned)) &&
                 (nrow(ufrstcDf_returned) == 0))
  
  # Sad path 2 - calibration does not have right values for "resistance" calibration
  # the calibration should have (U_CVALR1,U_CVALR4) to be the voltage calibration
  
  fileCal = "calibration4.xml"
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
  data = c(0.9, 0.88)
  ufrstcDf_returned <- try(NEONprocIS.cal::def.ucrt.fdas.rstc (data = data,
                                                               infoCal = infoCal), silent = TRUE)
  testthat::expect_true((class(ufrstcDf_returned)[1] == "try-error")) 
})
