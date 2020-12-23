##############################################################################################
#' @title Unit test of def.ucrt.dp01.cal.mult.R, compute L1 (temporally aggregated) calibration uncertainty represented as a multiplier to the max L0' value

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Definition function. Compute the L1 (temporally aggregated) calibration uncertainty due
#' to the accuracy of the instrumentation in the form of Truth and Trueness. The uncertainty
#' is represented by a percentage multiplier (coefficient provided by CVAL) to the maximum L0' value.

#' @param data Numeric vector of (calibrated) L0' data.
#' A single aggregated uncertainty will be computed for the full timeseries.
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX),
#' end_date(POSIX), Name (of the coefficient - string),
#' and Value (of the coefficient - string or numeric, to be interpreted as numeric).
#' @param NameCoef The name of the coefficient that represents the value to be multiplied with
#' the maximum L0' value. Defaults to U_CVALA3. Note that the units in the calibration file may say percent, but it is really a scale factor.
#' @param VarUcrt A character string of the target variable/term for which uncertainty will be computed
#' (i.e. the name of the term/variable for the data in \code{data}).
#' @param TimeAgrBgn A POSIX time value indicating the beginning of the temporal aggregation interval (inclusive)
#' @param TimeAgrEnd A POSIX time value indicating the end of the temporal aggregation interval (non-inclusive)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty. If the aggregation
#' interval spans multiple coefficient start and end periods, the maximum value of the applicable coefficients
#' is used as the multiplier. Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' data <- c(827.7,827.8,831.3)
#' ucrtCoef <- list(list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'),
#'                  list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA1',Value='0.0143'))
#' ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.mult(data=data,ucrtCoef=ucrtCoef,NameCoef='U_CVALA3',VarUcrt='linePAR',TimeAgrBgn=as.POSIXct('2019-01-01 00:00',tz='GMT'),TimeAgrEnd=as.POSIXct('2019-01-01 00:30',tz='GMT'))

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01.cal.mult.fdas.volt}

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.stat")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.stat")

# changelog and author contributions / copyrights
#   Mija Choi (2020-11-19)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.dp01.cal.mult.R\n")

# Unit test of def.ucrt.dp01.cal.mult.R
test_that("Unit test of def.ucrt.dp01.cal.mult.R", {
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  
  data <- c(827.7, 827.8, 831.3)
  ucrtCoef <- list(
    list(
      term = 'linePAR',
      start_date = as.POSIXct('2019-01-01', tz = 'GMT'),
      end_date = as.POSIXct('2019-01-02', tz = 'GMT'),
      Name = 'U_CVALA3',
      Value = '0.0141'
    ),
    list(
      term = 'linePAR',
      start_date = as.POSIXct('2019-01-03', tz = 'GMT'),
      end_date = as.POSIXct('2019-01-04', tz = 'GMT'),
      Name = 'U_CVALA3',
      Value = '0.0143'
    )
  )
  #  Happy Path 1 - If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
  
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.mult(
    data = data,
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALA3',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-01-01 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-01-05 00:30', tz = 'GMT')
  )
  
  testthat::expect_true(ucrtCal == 0.0143 * max(data))
  
  # Sad Path 1 - When data is not numeric, returns NA
  data_notNumeric <- c('827.7', '827.8', '831.3')
  ucrtCal <- try(NEONprocIS.stat::def.ucrt.dp01.cal.mult(
    data = data_notNumeric,
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALA3',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-02-05 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-02-05 00:30', tz = 'GMT')
  ),
  silent = TRUE)
  
  testthat::expect_true((class(ucrtCal)[1] == "try-error"))
  
  # Sad Path 2 - 
  
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.mult(
    data = data,
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALR3',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-02-05 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-02-05 00:30', tz = 'GMT')
  )
  
  testthat::expect_true(is.na(ucrtCal))
  
  # Sad Path 3 - When all parameters are NULL and data is an empty vector, returns ERROR
  
  data_empty <- c()
  ucrtCal <- try(NEONprocIS.stat::def.ucrt.dp01.cal.mult(
    data = data_empty,
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALR3',
    VarUcrt = NULL,
    TimeAgrBgn = NULL,
    TimeAgrEnd = NULL), silent = TRUE)
  
  testthat::expect_true((class(ucrtCal)[1] == "try-error"))
  
})
