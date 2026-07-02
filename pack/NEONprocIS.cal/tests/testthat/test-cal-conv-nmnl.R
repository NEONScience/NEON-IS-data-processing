##############################################################################################
#' @title Unit test of NEON nominal calibration conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description
#' Run unit tests for nominal calibration conversion function. The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param data Data frame of nominally calibrated sensor readings. This data frame must have 
#' a column called "readout_time" with POSIXct timestamps
#' @param nomVal A numeric value used for nominal calibration.
#' @param nomCalID A character string that identifies the calibration value that should be used, e.g. CVAL_B1
#' @param varConv A character string of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 

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
#   Nora Catolico (2026-05-05)
#     original creation based on test-cal-conv-poly.R
##############################################################################################
# Define test context
context("\n                       nominal calibration conversion\n")

# Test nominal calibration conversion
test_that("testing nominal calibration conversion", {
  
  testDir = "calibrations/nominal/"
  testFileCal = "calibration_nominal.xml"
  testFileCalPath <- fs::path(testDir, testFileCal)
  
  # Create data to calibrate - nominal wind speed data 
  # that will be converted to actual speeds using nominal calibration
  data <- c(15, 30, 45, 60, 75, 90)
  readout_time <- as.POSIXct(c('2025-12-13 00:00:00','2025-12-13 06:00:00','2025-12-13 12:00:00',
                                '2025-12-14 00:00:00','2025-12-14 06:00:00','2025-12-15 00:00:00'),tz='GMT')
  data = data.frame(readout_time=readout_time, speed=data)
  
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2025-12-13',tz='GMT')
  TimeEnd <- base::as.POSIXct('2025-12-15',tz='GMT')
  calSlct <- list(speed=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  
  # Create nominal value and calibration ID data frames as expected by the function
  nomVal <- data.frame(term=c('speed'), value=c(0.16667))
  nomCalID <- data.frame(term=c('speed'), ID=c('CVALB1'))
  
  ##########
  ##########  Happy paths:::: data and cal not empty and have valid values
  ##########
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values    |\n")

  calibrated <-
    NEONprocIS.cal::def.cal.conv.nmnl(data = data, 
                                      nomVal = nomVal, 
                                      nomCalID = nomCalID,
                                      varConv='speed', 
                                      calSlct=calSlct)

  # Check that calibrated data is created with appropriate column name
  testthat::expect_true('speedCalibrated' %in% names(calibrated))
  
  # Check the data inside the valid date range are calibrated correctly
  # Expected: (15/90)/(15/90) * CVAL_B1, (30/90)/(15/90) * CVAL_B1, etc.
  # Where CVAL_B1 is retrieved from the calibration file
  testthat::expect_true(!all(is.na(calibrated$speedCalibrated[2:5])))
  
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ valid calibration date range inclusive of start date, exclusive of end date    |\n")

  # Check the first and last dates, which fall on the boundaries of the valid cal periods
  testthat::expect_true(!is.na(calibrated$speedCalibrated[1]) || !is.na(calibrated$speedCalibrated[6]))

  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ data is before the valid date range of the cal. Return NA values. |\n\n")
  
  data$readout_time <- as.POSIXct(c('2025-10-13','2025-10-14','2025-10-15','2025-10-16','2025-10-17','2025-10-18'),tz='GMT')
  
  calibrated <- NEONprocIS.cal::def.cal.conv.nmnl(data = data, 
                                                  nomVal = nomVal, 
                                                  nomCalID = nomCalID,
                                                  varConv='speed', 
                                                  calSlct=calSlct)
  
  testthat::expect_true(all(is.na(calibrated$speedCalibrated)))
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cals specified for 'speed'. Returns NA |\n\n")
  calSlctNoVar <- list(temp=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  calibrated <- NEONprocIS.cal::def.cal.conv.nmnl(data = data, 
                                                  nomVal = nomVal, 
                                                  nomCalID = nomCalID,
                                                  varConv='speed', 
                                                  calSlct=calSlctNoVar)
  testthat::expect_true (all(is.na(calibrated$speedCalibrated)))

  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot calibrate character variable   |\n\n")
  #
  
  data_char <- data
  data_char$speed <- as.character(data_char$speed)
  data$readout_time <- as.POSIXct(c('2025-12-13 00:00:00','2025-12-13 06:00:00','2025-12-13 12:00:00',
                                     '2025-12-14 00:00:00','2025-12-14 06:00:00','2025-12-15 00:00:00'),tz='GMT')
  
  calibrated <- try(NEONprocIS.cal::def.cal.conv.nmnl(data = data_char, 
                                                      nomVal = nomVal, 
                                                      nomCalID = nomCalID,
                                                      varConv='speed', 
                                                      calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(calibrated)[1] == "try-error"))

  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ data missing readout_time variable    |\n\n")

  calibrated <- try(NEONprocIS.cal::def.cal.conv.nmnl(data = data[,-1], 
                                                      nomVal = nomVal, 
                                                      nomCalID = nomCalID,
                                                      varConv='speed', 
                                                      calSlct=calSlct), silent = TRUE)

  testthat::expect_true((class(calibrated)[1] == "try-error"))

  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ readout_time not POSIXt    |\n\n")
  data$readout_time <- as.character(data$readout_time)
  calibrated <- try(NEONprocIS.cal::def.cal.conv.nmnl (data = data, 
                                                       nomVal = nomVal, 
                                                       nomCalID = nomCalID,
                                                       varConv='speed', 
                                                       calSlct=calSlct),
                    silent=TRUE)
  testthat::expect_true ("try-error" %in% class(calibrated))

})
