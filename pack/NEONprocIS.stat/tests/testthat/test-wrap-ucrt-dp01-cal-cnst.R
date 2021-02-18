##############################################################################################
#' @title Unit test of wrap.ucrt.dp01.cal.cnst.R, compute combined and expanded (95% confidence) temporally aggregated L1 uncertainty 
#' due to natural variation and calibration (constant value)
#' 
#' Compute the combined and expanded (95% confidence) temporally aggregated L1 
#' uncertainty for a set of values subject to natural variation and calibration uncertainty. 
#' Uncertainty due to  natural variation is estimated from the 
#' standard error of the mean. Uncertainty due to calibration is represented by a constant value 
#' coefficient provided by CVAL (U_CVALA3).
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for wrap.ucrt.dp01.cal.cnst.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values
#'
#' Input param s to wrap.ucrt.dp01.cal.cnst.R
#' @param data Data frame of L0' (calibrated) data. Must contain columns \code{readout_time} (POSIX) and 
#' whatever variable is specified in input parameter \code{VarUcrt} (numeric).
#' A single aggregated uncertainty for the selected variable \code{VarUcrt} will be computed over the full timeseries.
#' 
#' @param VarUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (all other columns will be ignored in this function).
#'  
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), and 
#' Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' This will be passed into the calibration uncertainty function. Calibration uncertainty 
#' requires the U_CVALA3 coefficient. 
#' 
#' @param ucrtData Unused in this function. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A single numeric value representing the aggregated L1 calibration uncertainty over the full record. 
#' Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' 
#' @keywords calibration, uncertainty, fdas L1, average
#' 
#' 
#' data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                    temp=c(.599,.598,.597))
#' ucrtCoef <- list(list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        temp=c(100.187,100.195,100.203))
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=data,VarUcrt='temp',ucrtCoef=ucrtCoef)
#' 
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{def.ucrt.dp01.cal.cnst}
#' 
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.stat")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.stat")

# changelog and author contributions / copyrights
#   Mija Choi (2021-02-02)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp01.cal.cnst.R\n")

# Unit test of wrap.ucrt.dp01.cal.cnst.R
test_that("Unit test of wrap.ucrt.dp01.cal.cnst.R", {
  log <- NEONprocIS.base::def.log.init()
  
  data <- data.frame(
    readout_time = as.POSIXct(c('2019-01-01 00:00', '2019-01-01 00:01', '2019-01-01 00:02'),tz = 'GMT'),temp = c(.599, .598, .597)
    )
  ucrtCoef <- list(
      list(
        term = 'temp',
        start_date = as.POSIXct('2019-01-01', tz = 'GMT'),
        end_date = as.POSIXct('2019-01-02', tz = 'GMT'),
        Name = 'U_CVALA3',
        Value = '0.0141'
      )
    )
  ucrtData <- data.frame(
    readout_time = as.POSIXct(c('2019-01-01 00:00', '2019-01-01 00:01', '2019-01-01 00:02'),tz = 'GMT'),
    temp = c(100.187, 100.195, 100.203)
    )

  #  Happy Path 1, all the params to wrap.ucrt.dp01.cal.cnst have the correct values
  ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data = data, VarUcrt = 'temp',ucrtCoef = ucrtCoef)
  
  expect_true(is.numeric(ucrt))
  
  #  Sad Path 1, if data input is not numeric then wrap.ucrt.dp01.cal.cnst will not be executed
  
  dataComp <- as.character(data[['temp']])
  
  ucrt <- try(NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data = dataComp,VarUcrt = 'temp',ucrtCoef = ucrtCoef,), silent = TRUE)
  expect_true((class(ucrt)[1] == "try-error"))
})
