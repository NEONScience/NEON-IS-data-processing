##############################################################################################
#' @title Unit test of def.ucrt.meas.mult.R

#' @description
#' Run unit tests for def.ucrt.meas.mult.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.meas.mult.R for the details of the function.
#' @param data Numeric vector of raw measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and ii) uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

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
#   Mija Choi (2020-06-22)
#     Original Creation
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
#   Mija Choi (2020-09-24)
#     adjusted inputs to conform to the change made in def.ucrt.meas.mult.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.meas.mult.R\n")

# Unit test of def.ucrt.meas.mult.R
test_that("Unit test of def.ucrt.meas.mult.R", {
  # Happy path
  #
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration

  testDir = "testdata/"
  testFileCal = "calibration.xml"
  testFileCalPath <- paste0(testDir, testFileCal)
  
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=testFileCalPath,Vrbs=TRUE)
  data = c(0.9)
  data = data.frame(data=data)
 
  # Happy Path 1 - All params passed
  umeas_cnstDf_returned <- NEONprocIS.cal::def.ucrt.meas.mult (data = data, infoCal = infoCal)

  expect_true ((is.data.frame(umeas_cnstDf_returned)) &&
                 !(is.null(umeas_cnstDf_returned)))
  # The output is a data frame having Name, Value, and .attrs
  # Happy path 2 - no parameters passed
  
  umeas_multDf_returned <- NEONprocIS.cal::def.ucrt.meas.mult ()
  
  expect_true ((is.data.frame(umeas_multDf_returned)) &&
                 (nrow(umeas_multDf_returned) == 0))
  
  # Happy path 3 - More than one matching uncertainty coefficient was found for U_CVALA1. 
  #            - Will use the first if more than one matching uncertainty coefficient was found
  # calibration44.xml below has 2 entries for U_CVALA1
 
  testFileCal = "calibration44.xml"
  testFileCalPath <- paste0(testDir, testFileCal)
  
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile = testFileCalPath, Vrbs = TRUE)
  data = c(0.7)
  data = data.frame(data=data)
  
  umeas_multDf_returned <- NEONprocIS.cal::def.ucrt.meas.mult (data = data, infoCal = infoCal)
  
  # Check to see if only the first was returned from calibration44.xml
  # Return the individual measurement uncertainty is just U_CVALA1 multiplied by data
  
  expect_true (umeas_multDf_returned$ucrtMeas == base::as.numeric(infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',][1,]$Value)*data)
  #
  # Sad path - Check format of infoCal 
  # the calibration222.xml does not have 'U_CVALA1' in tha names of Uncertainty
  
  testFileCal = "calibration222.xml"
  testFileCalPath <- paste0(testDir, testFileCal)
  
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=testFileCalPath,Vrbs=TRUE)
  data = c(0.9)
  data = data.frame(data=data)
  
  umeas_multDf_returned <- try (NEONprocIS.cal::def.ucrt.meas.mult (data = data, infoCal = infoCal), silent = TRUE)
  expect_true (base::class(umeas_multDf_returned) == 'try-error')
  })
