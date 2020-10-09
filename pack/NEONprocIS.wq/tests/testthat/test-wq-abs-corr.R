##############################################################################################
#' @title Unit test of def.wq.abs.corr.R, determine excitation (Abs_ex) and emission (Abs_em) correction factors for using
#' SUNA data to correct sonde fDOM data. Also populates spectrumCount.
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' Run unit tests for def.wq.abs.corr.R
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values
#'
#' In def.wq.abs.corr.R, alternative calibration uncertainty function, create file (dataframe) with
#' uncertainty information based off of the L0 dissolved oxygen (DO) concentration data values
#' according to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' @param sunav2Filenames SUNA data filenames used to determine absorbance [character]
#' @param sunav2CalFilenames Calibration filenames for the SUNA data used to determine absorbance [character]
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]
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
#   Mija Choi (2020-09-10)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of def.wq.abs.corr.R\n")

# Unit test of def.wq.abs.corr.R
test_that("Unit test of def.wq.abs.corr.R", {

# Validate sunav2 calibration against the xsd  
  
  xsd1 <- system.file("extdata", "sunav2_calibration.xsd", package = "NEONprocIS.wq")
  
  calDir = "calibrations/"
  testFileCal = "sunav2_calibration1.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <-
    try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),
        silent = TRUE)
  
  if (xmlchk != TRUE) {
    log$error(
      base::paste0(
        " ====== def.read.cal.xml will not run due to the error in xml,  ",
        NameFile
      )
    )
    
    base::stop()
  }
  
  testDir = "testdata/"
  testFile = "sunav2_File4.parquet"
  testFilesPath <- paste0(testDir, testFile)
  # Get the filenames without path information
 # nameFileCal <- base::unlist(base::lapply(strsplit(fileCal,'/'),utils::tail,n=1))
  NEONprocIS.wq::def.wq.abs.corr(sunav2Filenames=testFilesPath,sunav2CalFilenames=testFileCalPath,log = NULL)
  
  })