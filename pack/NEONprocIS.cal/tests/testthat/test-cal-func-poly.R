##############################################################################################
#' @title Unit test of calibration polynomial function from NEON CVAL coefficients

#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Unit tests for def.cal.func.poly.R, function of creating a polynomical object of the calibration function from NEON 
#' calibration coefficients (from e.g. NEONprocIS.cal::def.read.cal.xml).
#' The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A polynomial (model) object of the polynomial calibration function

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' infoCal <- list(cal=data.frame(Name=c('CVALA1','CVALA0'),Value=c(10,1),stringsAsFactors=FALSE))
#' def.cal.func.poly(infoCal=infoCal)
#' 
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-03-16)
#     original creation
##############################################################################################
# Define test context
context("\n                       calibration polynomial function from NEON CVAL coefficients\n")
# Test calibration polynomial function
test_that("testing calibration polynomial function", {
  Sys.setenv(LOG_LEVEL='debug')
  # Create calibration data 
  data <- c(1, 2, 3, 4, 5, 6)
  
  # Create calibration coefficients
  Name = c("CVALA1", "CVALA2", "CVALA3", "CVALA4", "CVALA5", "CVALA6")
  Value = c("1", "1", "1", "1", "1", "1")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  infoCal <- list(cal = cal)
  
  # Construct the polynomial calibration function
  ##########  Happy path:::: infoCal is a valid list and have valid values
  
  func <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, log = log)
  
  # Check the polynomial object returned
  testthat::expect_true(polynom::is.polynomial(func))
  
  cat("\n       |====== Positive test 1::                         =====================|\n")
  cat("\n       |------ infoCal is a valid list and have valid values                  |\n")
  cat("\n       |------ polynomial calibration function ran successfully!              |\n")
  cat("\n       |======================================================================|\n")
  ##########
  ########## Sad path #1 - when infoCal (data frame) is empty
  ##########
  data <- as.numeric(c("1", "0.1", "1", "1"))
  
  Name = c()
  Value <- vector(mode = "numeric", length = 0)
  
  cal1 <- data.frame(Name, Value, stringsAsFactors = FALSE)
  infoCal <- list(cal = cal1)
 
   func <-
    try(NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, log = log, Vrbs = TRUE),
        silent = TRUE)
  #
  testthat::expect_true((class(func)[1] == "try-error"))
  #
  cat("\n       |======= Negative test 1::                      =======================|\n")
  cat("\n       |------- infoCal is a list but has a wrong name, cal1, instead of cal  |\n\n")
  #
  cat("\n       |------ polynomial calibration function will not run!                  |\n")
  cat("\n       |======================================================================|\n")

})
