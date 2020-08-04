##############################################################################################
#' @title Unit test of def.ucrt.wq.do.conc.R

#' @description
#' Run unit tests for def.ucrt.wq.do.conc.R, 
#'            "Uncertainty for dissolved oxygen (DO) concentration (mg/L) as part of the water"
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have valid values

#' Refer to def.ucrt.wq.do.conc.R for the details of the function.

#' @param data Dissolved oxygen (DO) concentration data [vector]
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]
#' 
#' Written to potentially plug in to def.cal.conv.R
#' ucrt <- def.ucrt.wq.do.conc(data = data, cal = NULL)

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' 

# changelog and author contributions / copyrights
#   Mija Choi (2020-08-03)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.ucrt.wq.do.conc.R\n")

# Unit test of def.ucrt.wq.do.conc.R
test_that("Unit test of def.ucrt.wq.do.conc.R", {
  # Happy Path 
  
  ### output = 0.01 if DO is > 0 & <= 20 mg/l according to the manual
  ### output = 0.05 if DO is >20 mg/l & < 50 mg/l according to the manual

  data = c(-5, 25, 20, 30, 10, 35)
  out_Data = c(0.01, 0.05, 0.01, 0.05, 0.01, 0.05)
  
  col_List = c("ucrtPercent", "ucrtMeas")
 
  outputDF_returned <-
    NEONprocIS.cal::def.ucrt.wq.do.conc (data = data, log = NULL)
  
  expect_true ((is.data.frame(outputDF_returned)) &&
                 !(is.null(outputDF_returned)))
  # columns returned are ucrtPercent and ucrtMeas
  expect_true (all (names(outputDF_returned) == col_List) &&
                  all(outputDF_returned$ucrtMeas == out_Data*data))
  
  # Sad path - data is NULL
  
  # The error will be returned since empty data is not a valid input to def.ucrt.wq.do.conc.R
 
  data = c()
  outputDF_returned <- try (NEONprocIS.cal::def.ucrt.wq.do.conc (data = data), silent = TRUE) 
  testthat::expect_true((class(outputDF_returned)[1] == "try-error")) 
})
