##############################################################################################
#' @title Unit test of def.ucrt.meas.cnst.R

#' @description
#' Run unit tests for def.ucrt.meas.cnst.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.meas.cnst.R for the details of the function.
#' @param data Numeric vector of raw measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and ii) uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2020-06-18)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.meas.cnst.R\n")

# Unit test of def.ucrt.meas.cnst.R
test_that("Unit test of def.ucrt.meas.cnst.R", {
 
  # The input is a json with elements of Name, Value, and .attrs
  # fileCal has the correct value for "resistance" calibration

  fileCal = "calibration.xml"
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
  data = c(0.9)
 
  # Happy Path 1 - All params passed
  umeas_cnstDf_returned <- NEONprocIS.cal::def.ucrt.meas.cnst (data = data, infoCal = infoCal)

  expect_true ((is.data.frame(umeas_cnstDf_returned)) &&
                 !(is.null(umeas_cnstDf_returned)))
  # The output is a data frame having Name, Value, and .attrs
  # Happy path 2 - no parameters passed
  
  umeas_cnstDf_returned <- NEONprocIS.cal::def.ucrt.meas.cnst ()
  
  expect_true ((is.data.frame(umeas_cnstDf_returned)) &&
                 (nrow(umeas_cnstDf_returned) == 0))
  
  # Happy path 3 - More than one matching uncertainty coefficient was found for U_CVALA1. 
  #            - Will use the first if more than one matching uncertainty coefficient was found
  # calibration44.xml below has 2 entries for U_CVALA1
 
  fileCal = "calibration44.xml"
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile = fileCal, Vrbs = TRUE)
  data = c(0.7)
  
  umeas_cnstDf_returned <- NEONprocIS.cal::def.ucrt.meas.cnst (data = data, infoCal = infoCal)
  
  # Check to see if only the first was returned from calibration44.xml
  # Return combined measurement uncertainty for an individual reading, not multiplied by data
  
  expect_true (umeas_cnstDf_returned$ucrtMeas == (infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',][1,]$Value))
  #
  # Sad path - Check format of infoCal 
  # the calibration222.xml does not have 'U_CVALA1' in tha names of Uncertainty
  
  fileCal = "calibration222.xml"
  infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
  data = c(0.9)
  
  umeas_cnstDf_returned <- try (NEONprocIS.cal::def.ucrt.meas.cnst (data = data, infoCal = infoCal), silent = TRUE)
  
  
  })
