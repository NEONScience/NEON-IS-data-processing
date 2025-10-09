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
   # testFileCal = "calibration_CVALM.xml"
   # testFileCalPath <- paste0(testDir, testFileCal)
   # infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   testData = "L0_data.csv"
   testDataPath <- paste0(testDir, testData)
   
   data0 <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   data <- data.frame(data0$resistance)
   
   # Case #1 - Happy path, infoCal has CVALA0 only
   
   infoCal <- list(cal=data.frame(Name=c('CVALA0'),Value=c(.9485),stringsAsFactors=FALSE))

   vector_cval_a0_as_a1 <- NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data,
                                                        infoCal = infoCal,
                                                        varConv = base::names(data)[1],
                                                        log = NULL)

   expect_true (is.vector(vector_cval_a0_as_a1))
   expect_true (all(!is.na(vector_cval_a0_as_a1)))
   
   # Case #2, infoCal does not have CVALA0 
   
   infoCal_noCVALA0 <- list(cal=data.frame(Name=c('CVALA1','CVALA2'),Value=c(10,1),stringsAsFactors=FALSE))
   
   vector_cval_noCVALA0 <- NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data,
                                                                       infoCal = infoCal_noCVALA0,
                                                                       varConv = base::names(data)[1],
                                                                       log = NULL)
   expect_true (all(vector_cval_noCVALA0 == 0)) 
 
   # Case #3, infoCal is not passed in, defaulted to NULL. Returns NA
   
   vector_cval_a0_as_a1 <- NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data, log = NULL)
   
   expect_true (is.vector(vector_cval_a0_as_a1))
   expect_true (all(is.na(vector_cval_a0_as_a1))) 
   
   
   # Case #4, data is not an array.  Error out due to "Input is not a data frame."
   data <- list (data)
   vector_cval_a0_as_a1 <- try(NEONprocIS.cal::def.cal.conv.poly.a0.as.a1 (data = data,
                                                            infoCal = infoCal,
                                                            log = NULL), silent = TRUE)
   
   testthat::expect_true((class(vector_cval_a0_as_a1)[1] == "try-error"))
 
   })
