##############################################################################################
#' @title Unit test of NEON calibration conversion (def.cal.conv.poly.h)

#' @author
#' Robert Markel \email{rmarkel@BattelleEcology.org}
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Run unit tests for calibration conversion function. The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param data Data frame of raw, uncalibrated measurements. This data frame must have a column
#' called "readout_time" with POSIXct timestamps
#' @param varConv A character array of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv.dp0p for what this input is.

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
#   Cove Sturtevant (2020-02-17)
#     Updated tests for function edits
#   Mija Choi (2020-09-23)
#     adjusted inputs to conform to the change made in def.cal.conv.poly.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-11-17)
#     Revise unit test for changed inputs
##############################################################################################
# Define test context
context("\n                       calibration conversion\n")

# Test calibration conversion
test_that("testing calibration conversion", {
  
  testDir = "calibrations/voltage/"
  testFileCal = c("calibration33_MH.xml","calibration33_MH_validAfter.xml")
  testFileCalPath <- fs::path(testDir, testFileCal)
  
  
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2019-06-12',tz='GMT')
  TimeEnd <- base::as.POSIXct('2019-07-10',tz='GMT')
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))

  # Create data to calibrate
  data <- c(1,2,3,4,500,600)
  data2 <- as.character(c(2,4,6,8,100,120))
  readout_time <- as.POSIXct(c('2019-06-12 17:48:35','2019-06-14 00:00:00','2019-06-15 00:00:00','2019-06-16 00:00:00','2019-06-17 00:00:00','2019-07-07 17:48:35'),tz='GMT')
  data = data.frame(readout_time=readout_time,data=data,data2=data2)

  ##########
  ##########  Happy paths:::: data and cal not empty and have valid values
  ##########
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values    |\n")

  calibrated <-
    NEONprocIS.cal::def.cal.conv.poly.h(data = data, varConv='data', calSlct=calSlct)

  # Check the data inside the valid date range are calibrated correctly
  testthat::expect_equal(c(0.0492, 0.0738, 0.0984, 12.3), calibrated$data[2:5])
  
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ valid calibration date range inclusive of start date, exclusive of end date    |\n")
  

  # Check the first and last dates, which fall on the boundaries of the valid cal periods
  # First date should get the first cal, last date should get the second cal
  testthat::expect_equal(c(0.0246, 120), c(calibrated$data[1],calibrated$data[6]))
  
  

  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ data is before the valid date range of the cal. Return NA values. |\n\n")
  
  data$readout_time <- as.POSIXct(c('2018-06-13','2018-06-14','2018-06-15','2018-06-16','2018-06-17','2018-06-18'),tz='GMT')
  
  calibrated <- NEONprocIS.cal::def.cal.conv.poly.h(data = data, varConv='data', calSlct=calSlct)
  
  testthat::expect_true(all(is.na(calibrated$data)))
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cals specified for 'data'. Returns NA |\n\n")
  calSlctNoVar <- list(voltage=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  calibrated <- NEONprocIS.cal::def.cal.conv.poly.h(data = data, 
                                                  varConv='data', 
                                                  calSlct=calSlctNoVar)
  testthat::expect_true (all(is.na(calibrated$data)))

  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot calibrate character variable   |\n\n")
  #

  testFileCal = "calibration44.xml"
  testFileCalPath <- fs::path(testDir, testFileCal)

  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2020-06-12',tz='GMT')
  TimeEnd <- base::as.POSIXct('2020-07-10',tz='GMT')
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  data$readout_time <- as.POSIXct(c('2020-06-12 17:48:35','2020-06-14 00:00:00','2020-06-15 00:00:00','2020-06-16 00:00:00','2020-06-17 00:00:00','2020-07-07 17:48:35'),tz='GMT')
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.h(data = data, varConv='data2', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ cal is has no polynomial coefficients                             |\n\n")
  #
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.h(data = data, varConv='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ data missing readout_time variable    |\n\n")
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.h(data = data[,-1], varConv='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ readout_time not POSIXt    |\n\n")
  data$readout_time <- as.character(data$readout_time)
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.h (data = data, 
                                                       varConv='data', 
                                                       calSlct=calSlct),
                    silent=TRUE)
  testthat::expect_true ("try-error" %in% class(calibrated))
  
  
  
})
