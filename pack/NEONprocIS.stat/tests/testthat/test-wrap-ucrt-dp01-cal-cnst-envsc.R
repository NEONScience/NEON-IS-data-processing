##############################################################################################
#' @title Unit test of wrap.ucrt.dp01.cal.cnst.envsc.R, compute combined and expanded (95% confidence)
#' temporally aggregated L1 uncertainty due to natural variation and calibration (constant value)
#' using the enviroscan soil-specific cval coefficient
#' 
#' @author
#' Teresa Burlingame
#'
#' @description
#' Run unit tests for wrap.ucrt.dp01.cal.cnst.envsc.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values
#'
# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-09-15)
#     Created based off unit tests for wrap.ucrt.dp01.cal.cnst.R and wrap.ucrt.dp01.cal.mult.envsc.R
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp01.cal.cnst.envsc.R\n")

# Unit test of wrap.ucrt.dp01.cal.cnst.envsc.R
test_that("Unit test of wrap.ucrt.dp01.cal.cnst.envsc.R", {

  data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
                     VSWCsoilSpecificMean=c(0.277,0.278,0.281))
  ucrtCoef <- list(list(term='VSWCsoilSpecificMean',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3_soilSpec',Value='0.0388'))

  # Happy Path 1, all the params to wrap.ucrt.dp01.cal.cnst.envsc have the correct values
  ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.envsc(data=data,VarUcrt='VSWCsoilSpecificMean',ucrtCoef=ucrtCoef,ucrtData=NULL)

  expect_true(is.numeric(ucrt))
  expect_false(is.na(ucrt))
  # Calibration term should be the constant coefficient itself, not multiplied by the data
  expect_equal(ucrt, 2*sqrt(stats::sd(data$VSWCsoilSpecificMean)^2/3 + 0.0388^2))

  # Sad Path 1, if VarUcrt data is not numeric then wrap.ucrt.dp01.cal.cnst.envsc will not be executed

  dataComp <- data
  dataComp$VSWCsoilSpecificMean <- as.character(dataComp$VSWCsoilSpecificMean)

  ucrt <- try(NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.envsc(data=dataComp,VarUcrt='VSWCsoilSpecificMean',ucrtCoef=ucrtCoef), silent = TRUE)
  expect_true((class(ucrt)[1] == "try-error"))
})
