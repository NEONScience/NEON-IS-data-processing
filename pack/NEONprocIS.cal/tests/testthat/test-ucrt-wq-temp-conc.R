##############################################################################################
#' @title Unit test of def.ucrt.wq.temp.conc.R

#' @description
#' Run unit tests for def.ucrt.wq.temp.conc.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.wq.temp.conc.R for the details of the function.

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
#   Mija Choi (2020-08-03)
#     Original Creation
#   Mija Choi (2020-09-24)
#     adjusted inputs to conform to the change made in def.ucrt.wq.temp.conc.R
#     This includes inputting the entire data frame not a vector, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-11-17)
#     Revise unit test for changed inputs/outputs
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.wq.temp.conc.R\n")

# Unit test of def.ucrt.wq.temp.conc.R
test_that("Unit test of def.ucrt.wq.temp.conc.R", {
  # Happy path
  #
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  calSlct <- NULL
  
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
    NEONprocIS.cal::def.ucrt.wq.temp.conc(data = data, varUcrt='data', calSlct=calSlct)
  
  col_List = c('ucrtPercent','ucrtMeas')  
  
  # Check the data inside the valid date range are uncertainty correctly
  testthat::expect_true(all(uncertainty$data$ucrtMeas==0.01))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot compute uncertainty for character variable   |\n\n")
  #
  
  uncertainty <- try(NEONprocIS.cal::def.ucrt.wq.temp.conc(data = data, varUcrt='data2', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ variable to compute uncertainty for is not in the input data      |\n\n")
  #
  
  uncertainty <- try(NEONprocIS.cal::def.ucrt.wq.temp.conc(data = data, varUcrt='data3', calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  
})
