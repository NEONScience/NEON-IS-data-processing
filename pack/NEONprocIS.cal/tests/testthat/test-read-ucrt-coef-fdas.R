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
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
##############################################################################################
# Define test context
context("\n                       Unit test of def.read.ucrt.coef.fdas.R\n")

# Unit test of def.read.ucrt.coef.fdas.R
test_that("Unit test of def.read.ucrt.coef.fdas.R", {
  # Happy path
  #
  # The input is a json with elements of Name, Value, and .attrs
  
  testDir = "testdata/"
  testJson = "ucrt-coef-fdas-input.json"
  testJsonPath <- paste0(testDir, testJson)
  
  #===================================================================
  ucrtDf_returned <-
    NEONprocIS.cal::def.read.ucrt.coef.fdas (NameFile = testJsonPath, log = NULL)
  # The output is a data frame having Name, Value, and .attrs
  expect_true ((is.data.frame(ucrtDf_returned)) &&
                 !(is.null(ucrtDf_returned)))
  
  # Sad path - test with a bad json
  
  testJson = "ucrt-coef-fdas-input-bad.json"
  testJsonPath <- paste0(testDir, testJson)
  
  #===================================================================
  ucrtDf_returned <- try(NEONprocIS.cal::def.read.ucrt.coef.fdas (NameFile = testJsonPath, log = NULL),
                         silent = TRUE)
  
  expect_true (base::class(ucrtDf_returned) == 'try-error')
})
