##############################################################################################
#' @title Unit test of def.ucrt.dp01.fdas.R, compute L1 (temporally aggregated) uncertainty due to a single-variable FDAS measurement
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
#' @param data Numeric vector of (calibrated) L0' data measured from the FDAS.
#' A single aggregated uncertainty will be computed over the full timeseries.
#' @param VarUcrt A character string of the name of the variable contained in \code{data}.
#' @param TypeFdas A single character representing the type of FDAS measurement, either 'R'
#' for resistance measurement, or 'V' for voltage measurement
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX),
#' end_date(POSIX), Name (of the coefficient - string), and
#' Value (of the coefficient - string or numeric, to be interpreted as numeric).
#' Resistance-based FDAS uncertainty requires U_CVALR3 and U_CVALR4 coefficients. Voltage-based FDAS
#' uncertainty requires U_CVALV3 and U_CVALV4 coefficients.
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data corresponding
#' as generated from one of the FDAS uncertainty functions in the NEONprocIS.cal package, e.g. def.ucrt.fdas.volt.poly.
#' Columns must include:\cr
#' readout_time (POSIX),\cr
#' VarUcrt_raw (numeric) - raw L0 reading prior to calibration (resistance or voltage),\cr
#' VarUcrt_dervCal (numeric) - 1st derivative of calibration function evaluated at raw reading value, and\cr
#' VarUcrt_ucrtComb (numeric) - standard uncertainty of individual measurement introduced by the Field DAS, \cr
#' where VarUcrt is the variable name specified in input \code{VarUcrt}.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 FDAS uncertainty for the . Numeric NA is
#' returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' data <- c(.599,.598,.597)
#' ucrtCoef <- list(list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALR3',Value='0.000195'),
#'                  list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALR4',Value='0.0067'))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        temp_raw=c(100.187,100.195,100.203),
#'                        temp_dervCal=c(2.5483,2.5481,2.5484),
#'                        temp_ucrtComb=c(0.06861,0.06860,0.06863),
#'                        stringsAsFactors=FALSE)
#' ucrtFdas <- NEONprocIS.stat::def.ucrt.dp01.fdas(data=data,VarUcrt='temp',TypeFdas='R',ucrtCoef=ucrtCoef,ucrtData=ucrtData)
#'
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}

#' @examples
#'
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.stat")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.stat")

# changelog and author contributions / copyrights
#   Mija Choi (2020-11-20)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.dp01.fdas.R\n")

# Unit test of def.ucrt.dp01.fdas.R
test_that("Unit test of def.ucrt.dp01.fdas.R", {
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration
  
  data <- c(.599, .598, .597)
  ucrtCoef <- list(
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
      tz = 'GMT'
    ),
    temp_raw = c(100.187, 100.195, 100.203),
    temp_dervCal = c(2.5483, 2.5481, 2.5484),
    temp_ucrtComb = c(0.06861, 0.06860, 0.06863),
    stringsAsFactors = FALSE
  )
  
  #  Happy Path 1 - TypeFdas is 'R' for resistance measurement
  
  ucrtFdas <-
    NEONprocIS.stat::def.ucrt.dp01.fdas(
      data = data,
      VarUcrt = 'temp',
      TypeFdas = 'R',
      ucrtCoef = ucrtCoef,
      ucrtData = ucrtData
    )
  
  testthat::expect_true(!is.na(ucrtFdas))
  
  #  Happy Path 2 - - TypeFdas is 'V' for voltage measurement
  
  ucrtCoef[[1]]$Name = "U_CVALV3"
  ucrtCoef[[2]]$Name = "U_CVALV4"
  ucrtFdas <-
    NEONprocIS.stat::def.ucrt.dp01.fdas(
      data = data,
      VarUcrt = 'temp',
      TypeFdas = 'V',
      ucrtCoef = ucrtCoef,
      ucrtData = ucrtData
    )
  
  testthat::expect_true(!is.na(ucrtFdas))
  
  #  Sad Path 1 - At least one of the expected FDAS uncertainty coefficients is not present for the term and aggregation interval.
  
  ucrtFdas <-  NEONprocIS.stat::def.ucrt.dp01.fdas(
    data = data,
    VarUcrt = 'temp',
    TypeFdas = 'R',
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  )
  
  testthat::expect_true(is.na(ucrtFdas))
  
  #  Sad Path 2 - column readout_time missing
  
  ucrtData_oneLessCol <- subset(ucrtData, select = -readout_time)
  
  ucrtFdas <-  try(NEONprocIS.stat::def.ucrt.dp01.fdas(
    data = data,
    VarUcrt = 'temp',
    TypeFdas = 'V',
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData_oneLessCol
  ), silent = TRUE)
  
  testthat::expect_true((class(ucrtFdas)[1] == "try-error"))
  
  #  Sad Path 3 - num(data) != num(ucrtData)
  
  data_two <- c(.599, .598)
  
  ucrtFdas <- try(NEONprocIS.stat::def.ucrt.dp01.fdas(
    data = data_two,
    VarUcrt = 'temp',
    TypeFdas = 'V',
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  ),
  silent = TRUE)
  
  #  Sad Path 4 - TypeFdas is neither "R", nor "V"
  
  ucrtFdas <-  try(NEONprocIS.stat::def.ucrt.dp01.fdas(
    data = data,
    VarUcrt = 'temp',
    TypeFdas = 'A',
    ucrtCoef = ucrtCoef,
    ucrtData = ucrtData
  ),silent = TRUE)
  
  testthat::expect_true((class(ucrtFdas)[1] == "try-error"))
  
  # Sad Path 5 - When all parameters are NULL, returns NA
  
  ucrtFdas <- try(NEONprocIS.stat::def.ucrt.dp01.fdas(
    data = NULL,
    VarUcrt = NULL,
    TypeFdas = 'R',
    ucrtCoef = NULL,
    ucrtData = NULL
  ),silent = TRUE)
  
  testthat::expect_true((class(ucrtFdas)[1] == "try-error"))
  
})
