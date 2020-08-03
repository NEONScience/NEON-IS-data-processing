##############################################################################################
#' @title Unit test of def.ucrt.wq.temp.do.R

#' @description
#' Run unit tests for def.ucrt.wq.temp.do.R, 
#'            "Uncertainty for dissolved oxygen (DO) concentration (mg/L) as part of the water"
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.wq.temp.do.R for the details of the function.

#' @param data Dissolved oxygen (DO) concentration data [vector]
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]
#' 
#' Written to potentially plug in to def.cal.conv.R
#' ucrt <- def.ucrt.wq.do.conc(data = data, cal = NULL)

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' 

# changelog and author contributions / copyrights
#   Mija Choi (2020-08-03)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.wq.temp.do.R\n")

# Unit test of def.ucrt.wq.temp.do.R
test_that("Unit test of def.ucrt.wq.temp.do.R", {
  # Happy Path 1 - All params passed
  
  testDir = "testdata/"
  
  testFileCal = "calibration222.xml"
  testFileCalPath <- paste0(testDir, testFileCal)
  
  infoCal <-
    NEONprocIS.cal::def.read.cal.xml(NameFile = testFileCalPath, Vrbs = TRUE)
  
  # data is temperature in Celsius
  
  ### output = 0.01 if data is <= 35 Celsius according to the manual
  ### output = 0.05 if data is >35 Celsius according to the manual
  
  temp = c(37, 30, 38, 20, 40, 15)
  out_Data = c(0.05, 0.01, 0.05, 0.01, 0.05, 0.01)
 
  col_List = c('ucrtMeas')
 
  #The cal input is not a requireed parameter 

  outputDF_returned <-
    NEONprocIS.cal::def.ucrt.wq.temp.do (data = temp, infoCal = infoCal, log = NULL)
  
  expect_true ((is.data.frame(outputDF_returned)) &&
                 !(is.null(outputDF_returned)))
  expect_true (all (names(outputDF_returned) == col_List) &&
                 all(outputDF_returned$ucrtMeas == out_Data))
  
  # Happy path 2 - infoCal is not passed
  
  outputDF_returned <- NEONprocIS.cal::def.ucrt.wq.temp.do (data = temp)
  
  expect_true ((is.data.frame(outputDF_returned)) && !(is.null(outputDF_returned)))
  expect_true (all (names(outputDF_returned) == col_List ) && all(outputDF_returned$ucrtMeas == out_Data))
  
  # Sad path 1 - data is NULL
  
  # There will be no output since data has no value to separate 
 
  temp = c()
  outputDF_returned <- try (NEONprocIS.cal::def.ucrt.wq.temp.do (data = temp), silent = TRUE) 
  testthat::expect_true((class(outputDF_returned)[1] == "try-error")) 
})
