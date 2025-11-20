##############################################################################################
#' @title Unit test of def.cal.conv.poly.a0.as.a1
#' Convert CVALA0 to CVALA1 and to a 1 degree polynomial 

#' @description
#' Definition function. Apply polynomial to data. If CVAL uses A0 when it should be A1, it needs conversion to
#' level 1 polynomial before calculating. A0 is traditionally an offset, but in the calibration it 
#' is treated as the multiplier. (eg #bucket tips* A0 = mm precip)

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALA[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
#' @param varConv A character string of the target variable (column) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame. Defaults to the first
#' column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv.dp0p for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal_noF0 <- list(cal=data.frame(Name=c('CVALA0'),Value=c(.4985),stringsAsFactors=FALSE))
#' def.cal.conv.poly.tip(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv.dp0p}
#' 
# changelog and author contributions / copyrights
#   Mija Choi (2025-09-19)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.cal.conv.poly.a0.as.a1.R\n")

# Unit test of .conv-poly-a0.as.a1.R
test_that("Unit test of def.cal.conv.polya0.as.a1.R", {
   # The input json has Name, Value, and .attrs
   
   testDir = "testdata/"
   testFileCal = c("calibrationA0.xml","calibrationA0_validBefore.xml")
   testFileCalPath <- fs::path(testDir, testFileCal)
   metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
   TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
   TimeEnd <- base::as.POSIXct('2020-01-01',tz='GMT')
   calSlct <- list(resistance=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
   
   testData = "L0_data.csv"
   testDataPath <- fs::path(testDir, testData)
   
   data <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   # Sad path, readout_time is not POSIXt
   calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, 
                                                             varConv='resistance', 
                                                             calSlct=calSlct),
                     silent=TRUE)
   testthat::expect_true ("try-error" %in% class(calibrated))
   
   
   # Happy path, infoCal has CVALA0 
   data$readout_time <- base::as.POSIXct(data$readout_time,tz='GMT')
   calibrated <- NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, 
                                                             varConv='resistance', 
                                                             calSlct=calSlct)

   testthat::expect_true (is.data.frame(calibrated))
   testthat::expect_equal (calibrated$resistance[2],21.96915,tolerance=1E-5)
   
   # data is before the valid date range of the cal. Return NA
   testthat::expect_true (is.na(calibrated$resistance[4]))
   
   # cal valid start date is inclusive, while end date is exclusive
   testthat::expect_equal (calibrated$resistance[5],10.98490,tolerance=1E-5)
   testthat::expect_equal (calibrated$resistance[1],21.96918,tolerance=1E-5)
   
   # infoCal does not have CVALA0, returns error
   testFileCal = "calibration.xml"
   testFileCalPath <- fs::path(testDir, testFileCal)
   metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=testFileCalPath)
   calSlct <- list(resistance=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
   
   calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, 
                                                             varConv='resistance', 
                                                             calSlct=calSlct),
                     silent=TRUE)
   testthat::expect_true ("try-error" %in% class(calibrated))
   
   # No cals specified for "resistance". Returns NA
   calSlct <- list(voltage=NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd))
   calibrated <- NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, 
                                                             varConv='resistance', 
                                                             calSlct=calSlct)
   testthat::expect_true (all(is.na(calibrated$resistance)))
   
   
   # input is not numeric, returns error
   calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, 
                                                                 varConv='site_id', 
                                                                 calSlct=calSlct),
                     silent=TRUE)
   testthat::expect_true ("try-error" %in% class(calibrated))
   
   # readout_time not present, returns error
   calibrated <- try(NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data[,setdiff(names(data),"readout_time")], 
                                                                 varConv='resistance', 
                                                                 calSlct=calSlct),
                     silent=TRUE)
   testthat::expect_true((class(calibrated)[1] == "try-error"))
   
   
   })
