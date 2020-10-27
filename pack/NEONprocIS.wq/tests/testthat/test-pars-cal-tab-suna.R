##############################################################################################
#' @title Unit test of def.pars.cal.tab.R, parse SUNA cal table using calibration filename
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for def.pars.cal.tab.R
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values
#'
#' In def.pars.cal.tab.R, definition function. Given a calibration filename, the code parses the calibration table
#' if it exists. Some older SUNA files do not have a calibration table. As of the development
#' of this function no other sensors, besides the SUNA, have a calibraiton table.

#' @param calFilename The filename assiciated with the desired calibration table [character]
#' @param calTableName The calibration table name, defaults to "CVALTABLEA1" [character]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of \cr
#'
#' @references Currently none
#'
#' @keywords Currently none
#'
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.wq\")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.wq")
#'
# changelog and author contributions / copyrights
#   Mija Choi (2020-10-13)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.pars.cal.tab.R\n")

# Unit test of def.pars.cal.tab.R
test_that("Unit test of def.pars.cal.tab.R", {

# Validate sunav2 calibration against the xsd  
  
  xsd1 <- system.file("extdata", "sunav2_calibration.xsd", package = "NEONprocIS.wq")
  
  calDir = "calibrations/"
  #
  # Test 1: sunav2 calibration has Calibrationtable
  #
  testFileCal = "sunav2_calibration1.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent = TRUE)
  
  if (xmlchk != TRUE) {
    log$error(base::paste0(" ====== def.read.cal.xml will not run due to the error in xml,  ",NameFile))
    base::stop()
  }
  
  df_SunaCalTab <- NEONprocIS.wq::def.pars.cal.tab.suna(calFilename = testFileCalPath, calTableName = "CVALTABLEA1")
 
  expect_true ((is.data.frame(df_SunaCalTab)) && !(is.null(df_SunaCalTab))  && nrow(df_SunaCalTab) == 256)
  
  col_List = c("wavelength","transmittance")
  
  # columns returned are  wavelength transmittance 
  expect_true (all (names(df_SunaCalTab) ==  col_List))
  #
  # Test 2: sunav2 calibration does not have Calibrationtable
  #
  testFileCal = "sunav2_noCalTbl_calibration.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent=TRUE)
  
  if (xmlchk != TRUE) {
    log$error(base::paste0(" ====== def.read.cal.xml will not run due to the error in xml,  ",NameFile))
    base::stop()
  }
  
  df_SunaCalTab <- try(NEONprocIS.wq::def.pars.cal.tab.suna(calFilename=testFileCalPath, calTableName="CVALTABLEA1"),silent=TRUE)
  

  })