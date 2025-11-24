##############################################################################################
#' @title Unit test of def.ucrt.fdas.volt.poly.R

#' @description
#' Run unit tests for def.ucrt.fdas.volt.poly.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.fdas.volt.poly.R for the details of the function.

#
#' @param data Data frame of raw, ununcertainty measurements. This data frame must have a column
#' called "readout_time" with POSIXct timestamps
#' @param varUcrt A character array of the target variables (columns) in the data frame \code{data} for 
#' which uncertainty output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param Meta Named list of metadata for use in this function. Meta is required to contain
#' list element ucrtCoefFdas, which is a data frame of FDAS uncertainty coefficients, as read by 
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr

#' @return A named list of data frames, each list element corresponding to a variable specified in 
#' \code{varUcrt}. Each data frame contains the following data columns:\cr
#' \code{ucrt$raw} - raw reading value (i.e. same as input data)\cr
#' \code{ucrt$dervCal} - 1st derivative of calibration function evaluated at raw reading value (e.g.
#' partial derivative of a temperature measurement with respect to the resistance reading)\cr
#' \code{ucrt$ucrtFdas} - standard uncertainty of individual measurement introduced by the Field DAS \cr

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
#   Mija Choi (2020-06-15)
#     Original Creation
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
#   Mija Choi (2020-09-24)
#     adjusted inputs to conform to the change made in def.ucrt.fdas.volt.poly.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-11-17)
#     Revise unit test for changed inputs/outputs
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.fdas.volt.poly.R\n")

# Unit test of def.ucrt.fdas.volt.poly.R
test_that("Unit test of def.ucrt.fdas.volt.poly.R", {
  # Happy path
  #
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  testDir = "calibrations/voltage/"
  testFileCal = c("calibration33.xml","calibration33_validAfter.xml")
  testFileCalPath <- fs::path(testDir, testFileCal)
  
  
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2019-06-12',tz='GMT')
  TimeEnd <- base::as.POSIXct('2019-07-10',tz='GMT')
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  
  # Get FDAS uncertainty coefficients
  Meta <- list()
  Meta$ucrtCoefFdas <- NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'testdata/ucrt-coef-fdas-input.json')
  
  # Create data to calibrate
  data <- c(1,2,3,4,5,6)
  data2 <- as.character(c(2,4,6,8,10,12))
  readout_time <- as.POSIXct(c('2019-06-12 17:48:35','2019-06-14 00:00:00','2019-06-15 00:00:00','2019-06-16 00:00:00','2019-06-17 00:00:00','2019-07-07 17:48:35'),tz='GMT')
  data = data.frame(readout_time=readout_time,data=data,data2=data2)
  
  ##########
  ##########  Happy paths:::: data and ucrt not empty and have valid values
  ##########
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values    |\n")
  
  uncertainty <-
    NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, varUcrt='data', calSlct=calSlct, Meta=Meta)
  
  col_List = c('raw','dervCal','ucrtFdas')  
  
  # Check the data inside the valid date range are uncertainty correctly
  testthat::expect_equal(data$data, uncertainty$data$raw)
  testthat::expect_true(all(uncertainty$data$dervCal[2:5]==0.0123))
  testthat::expect_equal(c(1.2300615,1.8450615,2.4600615,3.0750615),uncertainty$data$ucrtFdas[2:5],tolerance = 1E-6)
  
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ valid calibration date range inclusive of start date, exclusive of end date    |\n")
  
  
  # Check the first and last dates, which fall on the boundaries of the valid cal periods
  # First date should get the first cal, last date should get the second cal
  testthat::expect_equal(c(0.0123, 0.1000), 
                         c(uncertainty$data$dervCal[1],uncertainty$data$dervCal[6]),
                         tolerance = 1E-6)
  testthat::expect_equal(c(0.6150615, 0.0002324), 
                         c(uncertainty$data$ucrtFdas[1],uncertainty$data$ucrtFdas[6]),
                         tolerance = 1E-6)
  
  
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ data is before the valid date range of the cal. Return NA values. |\n\n")
  
  data$readout_time <- as.POSIXct(c('2018-06-13','2018-06-14','2018-06-15','2018-06-16','2018-06-17','2018-06-18'),tz='GMT')
  
  uncertainty <- NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, varUcrt='data', calSlct=calSlct,Meta=Meta)
  
  testthat::expect_equal(data$data, uncertainty$data$raw)
  testthat::expect_true(all(is.na(uncertainty$data$dervCal)))
  testthat::expect_true(all(is.na(uncertainty$data$ucrtFdas)))
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cals specified for 'data'. Returns NA |\n\n")
  calSlctNoVar <- list(voltage=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  uncertainty <- NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, 
                                                    varUcrt='data', 
                                                    calSlct=calSlctNoVar)
  testthat::expect_equal(data$data, uncertainty$data$raw)
  testthat::expect_true (all(is.na(uncertainty$data$dervCal)))
  testthat::expect_true (all(is.na(uncertainty$data$ucrtFdas)))
  
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot compute uncertainty for variable not present   |\n\n")
  #
  uncertainty <- try(NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, varUcrt='data3', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  

    #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot compute uncertainty for character variable   |\n\n")
  #
  
  testFileCal = "calibration44.xml"
  testFileCalPath <- fs::path(testDir, testFileCal)
  
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
  TimeBgn <- base::as.POSIXct('2020-06-12',tz='GMT')
  TimeEnd <- base::as.POSIXct('2020-07-10',tz='GMT')
  calSlct <- list(data=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
  data$readout_time <- as.POSIXct(c('2020-06-12 17:48:35','2020-06-14 00:00:00','2020-06-15 00:00:00','2020-06-16 00:00:00','2020-06-17 00:00:00','2020-07-07 17:48:35'),tz='GMT')
  
  uncertainty <- try(NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, varUcrt='data2', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ cal is has no applicable uncertainty coefficients.              |\n\n")
  #
  
  uncertainty <- try(NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data, varUcrt='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ data missing readout_time variable    |\n\n")
  
  uncertainty <- try(NEONprocIS.cal::def.ucrt.fdas.volt.poly(data = data[,-1], varUcrt='data', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ readout_time not POSIXt    |\n\n")
  data$readout_time <- as.character(data$readout_time)
  uncertainty <- try(NEONprocIS.cal::def.ucrt.fdas.volt.poly (data = data, 
                                                         varUcrt='data', 
                                                         calSlct=calSlct),
                     silent=TRUE)
  testthat::expect_true ("try-error" %in% class(uncertainty))
  
  
  
  
  
})
