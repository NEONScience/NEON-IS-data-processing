##############################################################################################
#' @title Unit test of def.ucrt.dp01.cal.cnst.R

#' @author
#' Mija Choi \email{choim@battelleecology.org}
#' 
#' @description
#' Run unit tests for def.ucrt.dp01.cal.cnst.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.dp01.cal.cnst.R for the details of the function.
#' 
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' varaible/term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), 
#' and Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' @param NameCoef The name of the coefficient that represents the L1 calibration uncertainty. Defaults
#' to U_CVALA3.
#' @param VarUcrt A character string of the target variable/term for which uncertainty will be computed. 
#' @param TimeAgrBgn A POSIX time value indicating the beginning of the temporal aggregation interval (inclusive)
#' @param TimeAgrEnd A POSIX time value indicating the end of the temporal aggregation interval (non-inclusive)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty. If the aggregation
#' interval spans multiple coefficient start and end periods, the maximum value of the applicable coefficients 
#' is selected. Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' ucrtCoef <- list(list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'),
#'                  list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA1',Value='0.0143'))
#' ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=ucrtCoef,NameCoef='U_CVALA3',VarUcrt='linePAR',TimeAgrBgn=as.POSIXct('2019-01-01 00:00',tz='GMT'),TimeAgrEnd=as.POSIXct('2019-01-01 00:30',tz='GMT'))

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01.cal.cnst.fdas.rstc}

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.stat")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.stat")

# changelog and author contributions / copyrights
#   Mija Choi (2020-11-16)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.dp01.cal.cnst.R\n")

# Unit test of def.ucrt.dp01.cal.cnst.R
test_that("Unit test of def.ucrt.dp01.cal.cnst.R", {
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  
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
  
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALA3',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-01-01 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-01-05 00:30', tz = 'GMT')
  )
  
  expect_true(ucrtCal == 0.0143)
  
  # Happy Path 2 - When no uncertainty coefficients match this time period, returns NA

  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALA3',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-02-05 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-02-05 00:30', tz = 'GMT')
  )
  expect_true(is.na(ucrtCal))
  
  # Happy Path 3 - When no uncertainty coefficients match coefficient name, returns NA
  
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(
    ucrtCoef = ucrtCoef,
    NameCoef = 'U_CVALA2',
    VarUcrt = 'linePAR',
    TimeAgrBgn = as.POSIXct('2019-01-01 00:00', tz = 'GMT'),
    TimeAgrEnd = as.POSIXct('2019-01-05 00:30', tz = 'GMT')
  )
  expect_true(is.na(ucrtCal))
  
  # Sad Path 1 - When all parameters are NULL, returns NA
  
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef = NULL,
                                                      NameCoef = NULL,
                                                      VarUcrt = NULL,
                                                      TimeAgrBgn = NULL,
                                                      TimeAgrEnd = NULL,
                                                      log = NULL)
  expect_true(is.na(ucrtCal))
})
