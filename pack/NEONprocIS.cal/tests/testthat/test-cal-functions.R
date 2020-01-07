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
test_that("testing calibration conversion", {
  # Create data to calibrate
  
  cat("\n\n  |-- Happy path - with valid data and valid cal ---|\n")
  
  # Create calibration coefficients
  
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "0.000196", "0.0000229", "0.0067")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  # Calibrate the data
  # coefUcrtMeas, coefUcrtFdas, coefUcrtFdasOfst need to be passed to def.cal.conv.R
  # validate the correct values with Cove
  #  0.3, 0.2, 0.33 are used atm.
  
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  

  expect_equal(data, calibrated$data, tolerance = 10)
  cat("\n       ||------ Calibration ran successfully! \n")
  
  ########## Sad path #1 - data (vector) is empty
  
  cat("\n\n  |-- Sad path #1 - when data (vector) is empty --|\n\n")
  
  data <- vector(mode = "numeric", length = 0)
  
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  #  expect_equal(data, calibrated$data, tolerance = 10)
  
  ########## Sad path #2 - when cal (data frame) is empty
  
  cat("\n\n  |-- Sad path #2 - when cal (data frame) is empty --|\n\n")
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c()
  Value <- vector(mode = "numeric", length = 0)
  
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  #expect_equal(data, calibrated$data, tolerance = 10)
  
  
  ########## Sad path #3 - negative testing with valid data and invalid calibration
  
  cat(
    "\n\n  |-- Sad path #3 - when data frame having invalid value(s) --|\n\n"
  )
  
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = as.numeric(c("ab.1b", "1", "1", "0.000196", "0.0000229", "0.0067"))
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  calibrated <-
    NEONprocIS.cal::def.cal.conv(data = data, cal = cal, 0.3, 0.2, 0.33)
  
  cat("\n\n") 
  #expect_equal(data, calibrated$data, tolerance=10)
})
