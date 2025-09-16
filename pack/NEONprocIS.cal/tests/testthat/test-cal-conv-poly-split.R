##############################################################################################
#' @title Unit test of def.cal.conv.poly.split,
#' Convert raw to calibrated data using NEON polynomial calibration coefficients that are split based on input range

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALM0, CVALM1, CVALM2, CVALH0, CVALH1, CVALH2 etc. to convert raw data to calibrated data.

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALM[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
#' @param varConv A character string of the target variable (column) in the data frame \code{data} for 
#' which the calibration will be applied (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame. Defaults to the first
#' column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal <- list(cal=data.frame(Name=c('CVALH0','CVALH1','CVALH2','CVALM0','CVALM1','CVALM2'),Value=c(-0.48,0.97,-0.000001,0.11,1,-0.00024),stringsAsFactors=FALSE))
#' def.cal.conv.poly.split(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}
#' 
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2025-09-05)
#     Original Creation 
##############################################################################################
# Define test context
context("\n                       Unit test of def-cal-conv-poly-split.R\n")

# Unit test of def-cal-conv-poly-split.R
test_that("Unit test of def-cal-conv-poly-split.R", {
   # The input json has Name, Value, and .attrs
   
   # testDir = "testdata/"
   # testFileCal = "calibration_CVALM.xml"
   # testFileCalPath <- paste0(testDir, testFileCal)
   # 
   # infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   # Happy path 1
   
   # Create calibration coefficients
   Name = c("CVALH2", "CVALH1", "CVALH0", "CVALM2", "CVALM1", "CVALM0")
   Value = c("0.000007067061488", "1.021338038908240", "3.831569019253189", "0.000080828104414", "1.008251095617125", "-0.061550466016713")
   cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
   infoCal <- list(cal = cal)
   
   # Create data
   data=c(101.0,102.0,93.0,94.0,95.0,106.0)
   data = data.frame(data=data)
 
   vector_cval_M_H <- NEONprocIS.cal::def.cal.conv.poly.split (data = data,
                                                        infoCal = infoCal,
                                                        varConv = base::names(data)[1],
                                                        calSlct=NULL,
                                                        log = NULL)
   
   expect_true (is.vector(vector_cval_M_H))
   expect_true (vector_cval_M_H[c(1, 2, 6)] > 100.0 && vector_cval_M_H[3:5] < 100.0) 

   # Sad path 1 infoCal is not passed in, defaulted to NULL. Returns NA
   
   vector_cval_M_H <- NEONprocIS.cal::def.cal.conv.poly.split (data = data, log = NULL)
   
   expect_true (all(is.na(vector_cval_M_H))) 

   # Sad path 2 - data is not an array
   data <- list (data)
   vector_cval_M_H <- try(NEONprocIS.cal::def.cal.conv.poly.b (data = data,
                                                            infoCal = infoCal,
                                                            log = NULL), silent = TRUE)
   
   testthat::expect_true((class(vector_cval_M_H)[1] == "try-error"))
})
