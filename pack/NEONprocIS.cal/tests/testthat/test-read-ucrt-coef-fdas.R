##############################################################################################
#' @title Unit test of def.read.ucrt.coef.fdas.R

#' @description
#' Run unit tests for def.read.ucrt.coef.fdas.R. The input json for the test is ucrt-coef-fdas-input.json.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to def.read.ucrt.coef.fdas for the details of the function.

#' @param NameFile String. Name (including relative or absolute path) of json file.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of FDAS uncertainty coefficients:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr

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
#   Mija Choi (2020-06-03)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.read.ucrt.coef.fdas.R\n")

# Unit test of def.read.ucrt.coef.fdas.R
test_that("Unit test of def.read.ucrt.coef.fdas.R", {
  
  # The input json has Name, Value, and .attrs
  
  NameFile = "ucrt-coef-fdas-input.json"
  
  #===================================================================
  dfUcrtCoef_returned <-
    NEONprocIS.cal::def.read.ucrt.coef.fdas (NameFile = NameFile, log =
                                               NULL)
  # The output data frame has Name, Value, and .attrs
  expect_true ((is.data.frame(dfUcrtCoef_returned)) &&
                 !(is.null(dfUcrtCoef_returned)))
})
