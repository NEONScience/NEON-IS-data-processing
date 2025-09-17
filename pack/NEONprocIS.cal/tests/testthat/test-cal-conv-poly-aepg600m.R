##############################################################################################
#' @title Unit test of def.cal.conv.poly.aepg600m.R,
#' Convert raw to calibrated data using NEON CVALA polynomial calibration coefficients with new origin F0 for 
#' Belfort primary precip sensor strain gauges
#' @description
#' Definition function. Apply NEON calibration polynomial function with calibration coefficients and a new origin F0 for 
#' Belfort primary precip sensor strain gauges

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
#' NEONprocIS.cal::wrap.cal.conv for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal <- list(Name=c('CVALA1','CVALA2','CVALF0'),Value=c(10,1,5),stringsAsFactors=FALSE)
#' def.cal.conv.poly.aepg600m(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}
#' 
# changelog and author contributions / copyrights
#   Mija Choi (2025-09-17)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def-cal-conv-poly-aepg600m.R\n")

# Unit test of def-cal-conv-poly-aepg600m.R
test_that("Unit test of def-cal-conv-poly-aepg600m.R", {
   # The input json has Name, Value, and .attrs
   
   testDir = "testdata/"
   testFileCal = "calibration_CVALM.xml"
   testFileCalPath <- paste0(testDir, testFileCal)
   
   testData = "L0_data.csv"
   testDataPath <- paste0(testDir, testData)
   
   data0 <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   data <- data.frame(data0$resistance)
   
   # Happy path 1
   
   # infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   infoCal <- list(cal=data.frame(Name=c('CVALA1','CVALA2','CVALF0'),Value=c(10,1,5),stringsAsFactors=FALSE))
   
   
   vector_cval_aepg600m <- NEONprocIS.cal::def.cal.conv.poly.aepg600m (data = data,
                                                        infoCal = infoCal,
                                                        varConv = base::names(data)[1],
                                                        log = NULL)
   
   expect_true (is.vector(vector_cval_aepg600m))
   expect_true (all(!is.na(vector_cval_aepg600m))) 
   
   # Happy path 2 infoCal is not passed in, defaulted to NULL. Returns NA
   
   vector_cval_aepg600m <- NEONprocIS.cal::def.cal.conv.poly.aepg600m (data = data, log = NULL)
   
   expect_true (is.vector(vector_cval_aepg600m))
   expect_true (all(is.na(vector_cval_aepg600m))) 
   
   
   # Sad path 1 - data is not an array.  Error out due to "Input is not a data frame."
   data <- list (data)
   vector_cvalB <- try(NEONprocIS.cal::def.cal.conv.poly.aepg600m (data = data,
                                                            infoCal = infoCal,
                                                            log = NULL), silent = TRUE)
   
   testthat::expect_true((class(vector_cvalB)[1] == "try-error"))
 
   })
