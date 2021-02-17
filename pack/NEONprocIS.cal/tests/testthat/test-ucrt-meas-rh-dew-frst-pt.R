##############################################################################################
#' @title Unit test of def.ucrt.meas.rh.dew.frst.pt.R, uncertainty for dew/frost point from the relative humidity sensor as part of the relative
#' humidity transition
#' 
#' Accepts L0 data and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns a vector of individual measurement
#' uncertainties for each data value. 
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for def.ucrt.meas.rh.dew.frst.pt.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values
#'
#' data <- data.frame(relative_humidity=c(1,6,7,0,10), temperature=c(2,3,6,8,5), dew_point=c(1,-1,5,4,4.5))
#' calSlct=list("temperature"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
#' timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "30000000000080_WO29705_157555.xml",id = 157555, expi= FALSE),
#' "relative_humidity"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
#' timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "30000000000080_WO29705_157554.xml",id = 157554, expi= FALSE),
#' "dew_point"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),timeEnd=as.POSIXct("2019-01-02",tz="GMT"),
#' file = "30000000000080_WO29705_157556.xml",id = 157556, expi= FALSE))
#' 
#' def.ucrt.meas.rh.dew.frst.pt(data=data,calSlct=calSlct)
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' 
#' @keywords calibration, uncertainty, fdas L1, average
#' 
#' data <- data.frame(dew_point=c(1,-1,5,4,4.5),temperature=c(2,3,6,8,5),relative_humidity=c(1,6,7,0,10),
#' readout_time=as.POSIXct(c('2019-01-01 02:00','2019-01-01 04:01','2019-01-01 06:02','2019-01-01 08:01','2019-01-01 10:02'),tz='GMT'))
#' ucrtCoef <- list(list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        temp=c(100.187,100.195,100.203))
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=data,VarUcrt='temp',ucrtCoef=ucrtCoef)
#' 
#' def.ucrt.meas.rh.dew.frst.pt(data=data,calSlct=calSlct)
#' 
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}
#' 
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2021-02-12)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.meas.rh.dew.frst.pt.R\n")

# Unit test of def.ucrt.meas.rh.dew.frst.pt.R
test_that("Unit test of def.ucrt.meas.rh.dew.frst.pt.R", {
  log <- NEONprocIS.base::def.log.init()
  
  data <- data.frame(dew_point=c(1,-1,5,4,4.5),temperature=c(2,-3,6,8,5),relative_humidity=c(1,6,7,0,10),
           readout_time=as.POSIXct(c('2019-01-01 02:00','2019-01-01 04:01','2019-01-01 06:02','2019-01-01 08:01','2019-01-01 10:02'),tz='GMT'))
 
  calSlct=list("temperature"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
  timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "testdata/temperature/30000000000080_WO29705_157555.xml",id = 157555, expi= FALSE),
  "relative_humidity"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),
  timeEnd=as.POSIXct("2019-01-02",tz="GMT"),file = "testdata/relHumidity/30000000000080_WO29705_157554.xml",id = 157554, expi= FALSE),
  "dew_point"= data.frame(timeBgn=as.POSIXct("2019-01-01",tz="GMT"),timeEnd=as.POSIXct("2019-01-02",tz="GMT"),
  file = "testdata/dewPoint/30000000000080_WO29705_157556.xml",id = 157556, expi= FALSE))
  
  ucrt <- NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data=data,calSlct=calSlct) 
  expect_true(is.data.frame(ucrt) && is.numeric(ucrt$ucrtMeas))
  
  #  Sad Path 1, if 
  data_outOfRange <- data.frame(dew_point=c(1,-1,5,4,4.5),temperature=c(2,-3,6,8,5),relative_humidity=c(1,6,7,0,10),
                     readout_time=as.POSIXct(c('2021-01-01 02:00','2021-01-01 04:01','2021-01-01 06:02','2021-01-01 08:01','2021-01-01 10:02'),tz='GMT'))
  
  ucrt <- NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data=data_outOfRange,calSlct=calSlct)
  expect_true(is.data.frame(ucrt) && is.na(ucrt$ucrtMeas))
   })
