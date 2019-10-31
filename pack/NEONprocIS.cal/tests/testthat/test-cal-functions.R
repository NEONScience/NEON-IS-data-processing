#' @description
#' Run tests for calibration functions.
#'
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")


# Define test context
context("calibration conversion")

# Test calibration conversion
test_that("calibration conversion works", {
  # Create data to calibrate
  data <- as.numeric(c("0", "0", "0", "0"))
  print(data)
  
  # Create calibration coefficients
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "1", "1", "1")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  
  # Calibrate the data
  calibrated <- NEONprocIS.cal::def.cal.conv(data = data, cal = cal)
  print(calibrated)
  
  # Check the zeroed data and calibrated data are equal
  expect_equal(data, calibrated)
})
