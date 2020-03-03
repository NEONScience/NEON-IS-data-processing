##############################################################################################
#' @title Unit test of Read NEON calibration XML file


#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Run unit tests for Read in a NEON calibration XML file. The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param NameFile String. Name (including relative or absolute path) of calibration file.
#' @param Vrbs (Optional) Logical. If TRUE, returns the full contents of the calibration file as an additional output.

#' @return TRUE when a test passes. Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-03-02)
#     Added unit testing
##############################################################################################
# Define test context
context("\n                      Read NEON calibration XML file\n")

# Test calibration conversion
test_that("testing Read NEON calibration XML file", {
  ##########
  ########## Happy path #1 - input xml is valid and conforms to the xml schmema
  ##########
  
  file1 = "calibration.xml"
  xsd1 = "calibration.xsd"
  #
  cat("\n       |======= Positive test 1:: input xml is valid and conforms to the xml schmema  ================|\n\n")
  
  #
  rpt1 <- NEONprocIS.cal::def.read.cal.xml (file1, Vrbs = TRUE)
  
  listXml <- XML::xmlToList(file1)
  listIdentical <- identical(listXml, rpt1$file)
  
  testthat::expect_true(listIdentical)
  
  cat("\n       |------ The list of input xml and the output of def.read.cal.xml are identical? --- ",listIdentical, " ====|\n")
  cat("\n       |------ Read NEON calibration XML ran successfully!                                            |\n")
  cat("\n       |==============================================================================================|\n\n"  )
  
  ##########
  ########## Sad path #1 - input xml does not exist
  ##########
  
  file1 = "calibration-doesNotExist.xml"
  xsd1 = "calibration.xsd"
  #
  cat("\n       |======= Negative test 1:: input xml does not exist  ==========================================|\n\n" )
  #
  rpt1 <-
    try(NEONprocIS.cal::def.read.cal.xml (file1, Vrbs = TRUE),
        silent = TRUE)
  #
  testthat::expect_true((class(rpt1)[1] == "try-error"))
  
  cat("\n       |------ Read NEON calibration XML will not run!                                                |\n")
  cat("\n       |==============================================================================================|\n\n")
  
  ##########
  ########## Sad path #2 - input xml does not conform to the xml schema
  ########## Warning issued and calibrated values NA.
  ##########
  
  file1 = "calibration-timeMissing.xml"
  xsd1 = "calibration.xsd"
  #
  cat("\n       |======= Negative test 2:: input xml does not conform to the xml schema  ======================|\n\n")
  #
  rpt1 <-
    try(NEONprocIS.cal::def.read.cal.xml (file1, Vrbs = TRUE),
        silent = TRUE)
  
  testthat::expect_true((class(rpt1)[1] == "try-error"))
  cat("\n       |------ Read NEON calibration XML will not run!                                                |\n")
  cat("\n       |==============================================================================================|\n")
  }
)
