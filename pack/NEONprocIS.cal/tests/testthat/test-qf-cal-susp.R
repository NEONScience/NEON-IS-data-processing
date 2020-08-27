##############################################################################################
#' @title Quality flag for suspect calibration

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Definition function. Check the CVALR1 coefficient in the calibration file. If it is present,
#' set the suspect calibration flag for all data values to the value of the coefficient 
#' (1=bad,0=good). If the CVALR1 coefficient is not present, set the suspect calibration flag 
#' to 0. If no calibration information is available, set the flag to -1.

#' @param data Numeric vector of raw measurements
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml. 
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients. 
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient.\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case flag value will be -1. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A numeric vector the same length as input argument "data" of suspect calibration flag 
#' values (1=bad,0=good,-1=could not evaluate).

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' data <- c(1,2,3,4,5)
#' infoCal <- list(cal=data.frame(Name=c('CVALR1','CVALA0'),Value=c(1,0),stringsAsFactors=FALSE))
#' qfSusp <- NEONprocIS.cal::def.qf.cal.susp(data,infoCal)

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2020-08-27)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def-qf-cal-susp.R\n")

# Unit test of def-qf-cal-susp.R
test_that("Unit test of def-qf-cal-susp.R", {
   testDir = "testdata/"
   
   testData = "L0_data.csv"
   testDataPath <- paste0(testDir, testData)
   
   data0 <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   data <- data0$resistance
   
   # Happy path 1 - Check the CVALR1 coefficient in the calibration file. If it is present,
   # set the suspect calibration flag for all data values to the value of the coefficient
   # (1=bad,0=good).
   
   testFileCal = "calibration2.xml"
   testFileCalPath <- paste0(testDir, testFileCal)
   
   infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   qfSusp <- NEONprocIS.cal::def.qf.cal.susp(data, infoCal)
   
   expect_true (all(qfSusp) == 0)
   
   # Happy path 2 -  If the CVALR1 coefficient is not present, set the suspect calibration flag
   # to 0.
   
   testFileCal = "calibration4.xml"
   testFileCalPath <- paste0(testDir, testFileCal)
   
   infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   qfSusp <- NEONprocIS.cal::def.qf.cal.susp(data, infoCal)
   
   expect_true (all(qfSusp) == 0)
   
   # Happy path 3 - If no calibration information is available, set the flag to -1.
   
   qfSusp <- NEONprocIS.cal::def.qf.cal.susp(data)
   
   expect_true (all(qfSusp == -1))
})
