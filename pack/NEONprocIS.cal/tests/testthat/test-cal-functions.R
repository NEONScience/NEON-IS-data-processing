#' @description
#' Run tests for calibration functions.
#'
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# Define test context
context("calibration conversion")

# Test calibration conversion
test_that("calibration conversion works", {
  # Create data to calibrate
  data <- as.numeric(c("1", "0.1", "1", "1"))
  cat("\n\n  ======================== Happy path - data with valid input and valid cal ==========================\n\n\n")
  print(data)
  cat("\n\n ")
  # Create calibration coefficients
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "0.000196", "0.0000229", "0.0067")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  # Calibrate the data
  # coefUcrtMeas, coefUcrtFdas, coefUcrtFdasOfst need to be passed to def.cal.conv.R
  # validate the correct values with Cove
  #  0.3, 0.2, 0.33 are used atm.
  calibrated <- NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  cat("\n  calibrated\n\n")
  print(calibrated)
  cat("\n ")
  # Happy path - positive testing 
  # Check the zeroed data and calibrated data are equal
  expect_equal(data, calibrated$data, tolerance=10)
 
  # Sad path #1 - negative testing with valid data and invalid calibration 
  # Check the zeroed data and calibrated data are equal
 
   data <- as.numeric(c("0.1", "0.1", "1", "1"))
  
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("a.bc", "1", "1", "0.000196", "0.0000229", "0.0067")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  cat("\n  ======================== Sad path #1 - valid data and invalid cal ========================== \n\n")
  print(data)
  cat("\n\n ")
  calibrated <- NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  cat("\n  calibrated\n")
  print(calibrated)
  cat("\n ")
  expect_equal(data, calibrated$data, tolerance=10)
  
  # Sad path #2 - negative testing with invaild data
  # Check the zeroed data and calibrated data are equal
  data <- as.numeric(c("fde.a", "0.1", "1", "1"))
  
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "0.000196", "0.0000229", "0.0067")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  cat("\n  ======================== Sad path #2 - invalid data and vaild cal ========================== \n\n")
  print(data)
  cat("\n\n ")
  calibrated <- NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  cat("\n  calibrated\n")
  print(calibrated)
  cat("\n ")
  expect_equal(data, calibrated$data, tolerance=10)
  
  # Sad path #3 - data is empty
  data <- as.numeric(c())
  cat("\n  ======================== Sad path #3 - data with empty file ==========================\n\n")
  print(data)
  cat("\n ")
  calibrated <- NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  cat("\n  calibrated\n\n")
  print(calibrated)
  cat("\n\n ")
  expect_equal(data, calibrated$data, tolerance=10)
})
