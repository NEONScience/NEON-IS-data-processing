##############################################################################################
#' @title Unit test of wrap.ucrt.dp01.R, wrapper for computing uncertainty for NEON L1 data
#'
#' Compute uncertainty for aggregated/averaged NEON L0' data (calibrated
#' data at native frequency). This function passes all relevant L0' data, including any
#' available uncertainty coefficients and L0' uncertainty data generated during the calibration
#' step, to the function specified for each variable. All functions must accept these inputs
#' regardless of whether they are used within.
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for def.ucrt.dp01.cal.cnst.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values
#'
#' @param data Data frame of L0' (calibrated) data. A single aggregated uncertainty for each
#' specified variable in \code{FuncUcrt} will be computed over the full timeseries.
#' @param FuncUcrt A data frame of the variables for which L1 measurement uncertainty is
#' to be calculated, and the corresponding functions to use. Columns include:\cr
#' \code{var} Character. The variable in \code{data} for which to compute uncertainty. \cr
#' \code{FuncUcrt} A character string indicating the uncertainty
#' function within the NEONprocIS.stat package that should be used to compute the uncertainty.
#' For many NEON data products, this will be NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.fdas.rstc or
#' NEONprocIS.stat::wrap.ucrt.dp01.cal.mult.fdas.volt. Note that any alternative function must accept
#' the arguments as these functions, even if they are unused, and
#' return the outputs in the same format. See one of those functions for details.
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX),
#' end_date(POSIX), Name (of the coefficient - string), and
#' Value (of the coefficient - string or numeric, to be interpreted as numeric).
#' This will be passed into the function(s) specified in FuncUcrt for use there. Defaults to NULL,
#' in which case no variables in FuncUcrt may use uncertainty coefficients in the specified functions.
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data. This will be
#' passed into the function(s) specified in FuncUcrt for use there. Defaults to NULL,
#' in which case no variables in FuncUcrt may use L0' uncertainty data in the specified functions.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to those in FuncUcrt$var and holding a single value representing the uncertainty

#' @references Currently none

#' @keywords Currently none
#' @examples
#'
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.stat")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.stat")

# changelog and author contributions / copyrights
#   Mija Choi (2020-12-14)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp01.R\n")

# Unit test of wrap.ucrt.dp01.R
test_that("Unit test of wrap.ucrt.dp01.R", {
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  data <- data.frame(
    readout_time = as.POSIXct(c('2019-01-01 00:00', '2019-01-01 00:01', '2019-01-01 00:02'),tz = 'GMT'),
    temp = c(.599, .598, .597),
    stringsAsFactors = FALSE)
  FuncUcrt <-  data.frame(var = 'temp',
               FuncUcrt = 'wrap.ucrt.dp01.cal.cnst.fdas.rstc',
               stringsAsFactors = FALSE)
  ucrtCoef <- list(
      list(
        term = 'temp',
        start_date = as.POSIXct('2019-01-01', tz = 'GMT'),
        end_date = as.POSIXct('2019-01-02', tz = 'GMT'),
        Name = 'U_CVALA3',
        Value = '0.0141'
      ),
      list(
        term = 'temp',
        start_date = as.POSIXct('2019-01-01', tz = 'GMT'),
        end_date = as.POSIXct('2019-01-02', tz = 'GMT'),
        Name = 'U_CVALR3',
        Value = '0.000195'
      ),
      list(
        term = 'temp',
        start_date = as.POSIXct('2019-01-01', tz = 'GMT'),
        end_date = as.POSIXct('2019-01-02', tz = 'GMT'),
        Name = 'U_CVALR4',
        Value = '0.0067'
      )
    )
  ucrtData <- data.frame(
      readout_time = as.POSIXct(
        c('2019-01-01 00:00', '2019-01-01 00:01', '2019-01-01 00:02'),
        tz = 'GMT'),
      temp_raw = c(100.187, 100.195, 100.203),
      temp_dervCal = c(2.5483, 2.5481, 2.5484),
      temp_ucrtComb = c(0.06861, 0.06860, 0.06863),
      stringsAsFactors = FALSE
    )
  # Happy Path 1 - All params passed
  ucrt_returned <- NEONprocIS.stat::wrap.ucrt.dp01(
      data = data,
      FuncUcrt = FuncUcrt,
      ucrtCoef = ucrtCoef,
      ucrtData = ucrtData
    )
  
  testthat::expect_true(!is.na(ucrt_returned))
  
  # Sad Path 1 - - column readout_time missing
  
  data_oneLessCol <- subset(data, select = -readout_time)
  
  ucrt_returned <- try(NEONprocIS.stat::wrap.ucrt.dp01(
    data = data_oneLessCol,
    FuncUcrt = FuncUcrt,
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  ),silent = TRUE)
  
  testthat::expect_true(class(ucrt_returned)[1] == "try-error")
 
   # Sad Path 2 - data input is not numeric
  
  data_notNumeric <- data
  data_notNumeric$temp <- as.character(data_notNumeric$temp)
  
  ucrt_returned <- try(NEONprocIS.stat::wrap.ucrt.dp01(
    data = data_notNumeric,
    FuncUcrt = FuncUcrt,
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  ),silent = TRUE)
  
  testthat::expect_true(class(ucrt_returned)[1] == "try-error")
  
  # Sad Path 3 - When all parameters are NULL, returns NA
  
  data_empty <- c()
  ucrt_returned <- try(NEONprocIS.stat::wrap.ucrt.dp01(
    data = data_empty,
    FuncUcrt = FuncUcrt,
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  ), silent = TRUE)
  testthat::expect_true(class(ucrt_returned)[1] == "try-error")
})
