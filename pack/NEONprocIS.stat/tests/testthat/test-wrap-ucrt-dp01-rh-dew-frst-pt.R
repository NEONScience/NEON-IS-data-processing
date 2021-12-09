##############################################################################################
#' @title Compute combined and expanded (95% confidence) temporally aggregated L1 uncertainty for dew/frost point

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Wrapper function. Compute the combined and expanded (95% confidence) temporally aggregated L1
#' uncertainty for a set of values subject to natural variation and calibration uncertainty.
#' Uncertainty due to  natural variation is estimated from the
#' standard error of the mean. Uncertainty due to calibration is represented by a multiplier (U_CVALA3) of the partial
#' derivatives of dew/frost point with respect to temperature and relative humidity.

#' @param data Data frame of L0' (calibrated) data. Must contain columns \code{readout_time} (POSIX) and
#' whatever variable is specified in input parameter \code{VarUcrt} (numeric).
#' A single aggregated uncertainty for the selected variable \code{VarUcrt} will be computed over the full timeseries.
#' @param VarUcrt A character string of the target variable (column) in the data frame \code{data} for
#' which uncertainty data will be computed (all other columns will be ignored in this function).
#' @param ucrtCoef Unused in this function.
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data for
#' dew point measurement from NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt.
#' Columns must include:\cr
#' readout_time (POSIX),\cr
#' VarUcrt_ucrt_dfpt_t_L1 (numeric) - 1st derivative of calibration function with respect to temperature multiplied by the L1 uncertainty coefficient, and\cr
#' VarUcrt_ucrt_dfpt_rh_L1 (numeric) - 1st derivative of calibration function with respect to relative humidity multiplied by the L1 uncertainty coefficient, and\cr
#' where VarUcrt is the variable name specified in input \code{VarUcrt}.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 uncertainty due to calibration and natural variation over the full record.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, dew point, frost point, uncertainty, average

#' @examples
#' data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                    dewPoint=c(10,11,9))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        dewPoint_ucrt_dfpt_t_L1=c(0.05,0.06,0.055),
#'                        dewPoint_ucrt_dfpt_rh_L1=c(0.065,0.055,0.054),
#'                        stringsAsFactors=FALSE)
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.rh.dew.frst.pt(data=data,VarUcrt='dewPoint',ucrtData=ucrtData)

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{def.ucrt.dp01.cal.rh.dew.frst.pt}

# changelog and author contributions / copyrights
#   Mija Choi (2021-07-16)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp01.rh.dew.frst.pt.R\n")

# Unit test of wrap.ucrt.dp01.rh.dew.frst.pt.R
test_that("Unit test of wrap.ucrt.dp01.rh.dew.frst.pt.R", {
  data <- data.frame(
      readout_time = as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz = 'GMT'),
      dewPoint = c(10, 11, 9)
      )
  ucrtData <- data.frame(
      readout_time = as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz = 'GMT'),
      dewPoint_ucrt_dfpt_t_L1 = c(0.05, 0.06, 0.055),
      dewPoint_ucrt_dfpt_rh_L1 = c(0.065, 0.055, 0.054),
      stringsAsFactors = FALSE
    )
  
  # positive test:  valid data passed in to
  ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.rh.dew.frst.pt(data = data,
                                                   VarUcrt = 'dewPoint',
                                                   ucrtData = ucrtData)
  
  testthat::expect_true(is.numeric(ucrt))
  
  # negative test 1:  ucrtData does not have the same number of rows of data
  
  ucrtData_subset <- ucrtData[1:2, ]
  ucrt <- try(NEONprocIS.stat::wrap.ucrt.dp01.rh.dew.frst.pt(data = data,
                                                             VarUcrt = 'dewPoint',
                                                             ucrtData = ucrtData_subset),
                                                             silent = TRUE)
  
  testthat::expect_true(class(ucrt)[1] == "try-error")

  # negative test 2:  data does not have dewPoint
  data_relHumidity <- data.frame(
    readout_time = as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz = 'GMT'),
    relHumidity = c(10, 11, 9)
  )
  ucrt <- try(NEONprocIS.stat::wrap.ucrt.dp01.rh.dew.frst.pt(data = data_relHumidity,
                                                             VarUcrt = 'dewPoint',
                                                             ucrtData = ucrtData_subset),
                                                             silent = TRUE)
  
  testthat::expect_true(class(ucrt)[1] == "try-error")

  })
