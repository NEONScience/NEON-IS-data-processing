##############################################################################################
#' @title Unit test of wrap.ucrt.dp01.cal.cnst.envsc.fctry.R, compute combined and expanded (95%
#' confidence) temporally aggregated L1 uncertainty due to natural variation and the fixed
#' manufacturer default calibration uncertainty for enviroscan factory-calibrated output
#' 
#' @author
#' Teresa Burlingame
#'
#' @description
#' Run unit tests for wrap.ucrt.dp01.cal.cnst.envsc.fctry.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values
#'
# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-09-15)
#     original creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp01.cal.cnst.envsc.fctry.R\n")

# Unit test of wrap.ucrt.dp01.cal.cnst.envsc.fctry.R
test_that("Unit test of wrap.ucrt.dp01.cal.cnst.envsc.fctry.R", {

  data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
                     VSWCfactoryMean=c(0.277,0.278,0.281))

  # Happy Path 1, all the params to wrap.ucrt.dp01.cal.cnst.envsc.fctry have the correct values.
  # ucrtCoef is unused (not required) since the calibration term is a fixed constant.
  ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.envsc.fctry(data=data,VarUcrt='VSWCfactoryMean',ucrtCoef=NULL,ucrtData=NULL)

  expect_true(is.numeric(ucrt))
  expect_false(is.na(ucrt))
  # Calibration term should be the fixed manufacturer default, regardless of the data magnitude
  expect_equal(ucrt, 2*sqrt(stats::sd(data$VSWCfactoryMean)^2/3 + 0.1067726^2))

  # Sad Path 1, if VarUcrt data is not numeric then wrap.ucrt.dp01.cal.cnst.envsc.fctry will not be executed

  dataComp <- data
  dataComp$VSWCfactoryMean <- as.character(dataComp$VSWCfactoryMean)

  ucrt <- try(NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.envsc.fctry(data=dataComp,VarUcrt='VSWCfactoryMean',ucrtCoef=NULL), silent = TRUE)
  expect_true((class(ucrt)[1] == "try-error"))
})
