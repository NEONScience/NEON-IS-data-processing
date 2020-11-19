##############################################################################################
#' @title Unit test of def.ucrt.wq.temp.conc.R

#' @description
#' Run unit tests for def.ucrt.wq.temp.conc.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.wq.temp.conc.R for the details of the function.

#' @param data Numeric vector of raw resistance measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal and info$ucrt,
#' which are data frames of calibration coefficients and uncertainty coeffcients, respectively.
#' Columns of these data frames are:\cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variable, ucrtMeas\cr

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
#   Mija Choi (2020-07-21)
#     Original Creation
#   Mija Choi (2020-09-24)
#     adjusted inputs to conform to the change made in def.ucrt.wq.temp.conc.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.wq.temp.conc.R\n")

# Unit test of def.ucrt.wq.temp.conc.R
test_that("Unit test of def.ucrt.wq.temp.conc.R", {
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
  temp = data.frame(temp=temp)
  
  out_Data = c(0.05, 0.01, 0.05, 0.01, 0.05, 0.01)
 
  col_List = c('ucrtMeas')
 
  #The cal input is not a requireed parameter 

  outputDF_returned <-
    NEONprocIS.cal::def.ucrt.wq.temp.conc (data = temp, infoCal = infoCal, log = NULL)
  
  expect_true ((is.data.frame(outputDF_returned)) &&
                 !(is.null(outputDF_returned)))
  expect_true (all (names(outputDF_returned) == col_List) &&
                 all(outputDF_returned$ucrtMeas == out_Data))
  
  # Happy path 2 - infoCal is not passed
  
  outputDF_returned <- NEONprocIS.cal::def.ucrt.wq.temp.conc (data = temp)
  
  expect_true ((is.data.frame(outputDF_returned)) && !(is.null(outputDF_returned)))
  expect_true (all (names(outputDF_returned) == col_List ) && all(outputDF_returned$ucrtMeas == out_Data))
  
  # Sad path 1 - data is NULL
  
  # There will be no output since data has no value to separate 
 
  temp = c()
  temp = data.frame(temp=temp)
  
  outputDF_returned <- try (NEONprocIS.cal::def.ucrt.wq.temp.conc (data = temp), silent = TRUE) 
  testthat::expect_true((class(outputDF_returned)[1] == "try-error")) 
})
