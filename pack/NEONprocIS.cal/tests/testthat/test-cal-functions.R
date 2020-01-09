##############################################################################################
#' @title Unit test of NEON calibration conversion

#' @author
#' Robert Markel \email{rmarkel@BattelleEcology.org}
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Run unit tests for calibration conversion function. The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param data Numeric vector of data to apply calibration to
#' @param cal Data frame of calibration coefficients. Must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALA[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param coefUcrtMeas Combined uncertainty of an individual measurement (U_CVALA1) in units of converted value
#' (e.g. temperature). Defaults to NULL, in which case uncertainty will not be calculated.
#' @param coefUcrtFdas Combined, relative FDAS uncertainty of an individual measurement (U_CVALR1 or U_CVALV1, unitless).
#' Defaults to NULL, in which case uncertainty will not be calculated.
#' @param coefUcrtFdasOfst offset imposed by the FDAS for e.g. resistance or voltage readings (U_CVALR4 or U_CVALV4).
#' Defaults to NULL, in which case uncertainty will not be calculated.

#' @return TRUE when a test passes. Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Robert Markel (2019-12-10)
#     original creation
#   Mija Choi (2020-01-07)
#     Added negative testing
##############################################################################################
# Define test context
context("\n                       calibration conversion\n")

# Test calibration conversion
test_that("testing calibration conversion", {
  # Create data to calibrate
  data <- as.numeric(c("0", "0", "0", "0"))
  print(data)
  
  # Create calibration coefficients
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "1", "1", "1")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  # Calibrate the data
  # coefUcrtMeas, coefUcrtFdas, coefUcrtFdasOfst need to be passed to def.cal.conv.R
  # validate the correct values with Cove
  #  0.3, 0.2, 0.33 are used atm.
  
  ##########
  ##########  Happy path:::: data and cal not empty and have valid values
  ##########
  
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  print(calibrated)
  
  # Check the zeroed data and calibrated data are equal
  expect_equal(data, calibrated$data)
  
  cat("\n       |=====================================================+++===|\n")
  cat("\n       |------ data and cal are not empty and have valid values    |\n")
  cat("\n       |------ Calibration ran successfully!                       |\n")
  cat("\n       |===========================================================|\n")
  ##########
  ########## Sad path #1 - data (vector) is empty
  ##########
  data <- vector(mode = "numeric", length = 0)
  cat("\n")
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  #  expect_equal(data, calibrated$data, tolerance = 10)
  
  ##########
  ########## Sad path #2 - when cal (data frame) is empty
  ##########
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c()
  Value <- vector(mode = "numeric", length = 0)
  
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  cat("\n")
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  #expect_equal(data, calibrated$data, tolerance = 10)
  
  ##########
  ########## Sad path #3 - when cal has invalid values
  ##########
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = as.numeric(c("ab.1b", "1", "1", "0.000196", "0.0000229", "0.0067"))
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  cat("\n")
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  #expect_equal(data, calibrated$data, tolerance = 10)
  
})

