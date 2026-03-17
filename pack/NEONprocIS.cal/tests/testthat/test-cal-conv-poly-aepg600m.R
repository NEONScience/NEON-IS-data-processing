##############################################################################################
#' @title Unit test of NEON calibration conversion for def.cal.conv.poly.aepg600m

#' @author
#' Robert Markel \email{rmarkel@BattelleEcology.org}
#' Mija Choi \email{choim@batelleEcology.org}
#' Cove Sturtevant \email{csturtevant@batelleEcology.org}

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
#     adjusted inputs to conform to the change made in def.cal.conv.poly.aepg600m.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-11-17)
#     Revise unit test for changed inputs
##############################################################################################
# Define test context
context("\n                       calibration conversion\n")

# Test calibration conversion
test_that("testing calibration conversion for def.cal.conv.poly.aepg600m", {
  
  testDir = "testdata/aepg600m/"
  testFileCal = c("cal_aepg600m.xml","cal_aepg600m_validAfter.xml")
  testFileCalPath <- fs::path(testDir, testFileCal)
  
  
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2023-11-29',tz='GMT')
  TimeEnd <- base::as.POSIXct('2024-11-30',tz='GMT')
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))

  # Create data to calibrate
  data <- c(0,1,2,3,4,5,6)
  data2 <- as.character(c(0,2,4,6,8,10,12))
  readout_time <- as.POSIXct(c('2023-11-29 00:00:00','2023-11-29 17:13:14','2023-12-20 00:00:00','2024-09-20 00:00:00','2024-10-20 00:00:00','2024-11-20 00:00:00','2024-11-27 17:13:14'),tz='GMT')
  data = data.frame(readout_time=readout_time,data=data,data2=data2)

  ##########
  ##########  Happy paths:::: data and cal not empty and have valid values
  ##########
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values    |\n")

  calibrated <-
    NEONprocIS.cal::def.cal.conv.poly.aepg600m(data = data, varConv='data', calSlct=calSlct)

  # Check the data inside the valid date range are calibrated correctly
  testthat::expect_equal(c(-249.3278, -249.3556, -249.3830, -249.4099), calibrated$data[3:6],tolerance = 1E-4)
  
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ valid calibration date range inclusive of start date, exclusive of end date    |\n")
  

  # Check the first and last dates, which fall on the boundaries of the valid cal periods
  # First date should get the first cal, last date should get the second cal
  testthat::expect_equal(c(-249.2997, -396.2939), c(calibrated$data[2],calibrated$data[7]),tolerance = 1E-4)
  
  

  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ data is before the valid date range of the cal. Return NA. |\n\n")
  
  testthat::expect_true(is.na(calibrated$data[1]))
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cals specified for 'data'. Returns NA |\n\n")
  calSlctNoVar <- list(voltage=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  calibrated <- NEONprocIS.cal::def.cal.conv.poly.aepg600m(data = data, 
                                                  varConv='data', 
                                                  calSlct=calSlctNoVar)
  testthat::expect_true (all(is.na(calibrated$data)))

  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot calibrate character variable   |\n\n")
  #

  testDir = "calibrations/voltage/"
  testFileCal = "calibration44.xml"
  testFileCalPath <- fs::path(testDir, testFileCal)

  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))

  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.aepg600m(data = data, varConv='data2', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ cal is has no polynomial coefficients                             |\n\n")
  #
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.aepg600m(data = data, varConv='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ data missing readout_time variable    |\n\n")
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.aepg600m(data = data[,-1], varConv='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(calibrated)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ readout_time not POSIXt    |\n\n")
  data$readout_time <- as.character(data$readout_time)
  calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.aepg600m (data = data, 
                                                       varConv='data', 
                                                       calSlct=calSlct),
                    silent=TRUE)
  testthat::expect_true ("try-error" %in% class(calibrated))
  
  
  
})
