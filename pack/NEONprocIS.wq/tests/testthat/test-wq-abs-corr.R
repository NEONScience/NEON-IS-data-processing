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
  
  # Get calibration xsd
  xsd1 <- system.file("extdata", "sunav2_calibration.xsd", package = "NEONprocIS.wq")
  calDir = "calibrations/"
  testDir = "testdata/"
  
  # calibration has all the required elements including CalibrationTable 
  # Validate sunav2 calibration against the xsd
  
  testFileCal = "sunav2_calibration3.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent = TRUE)
  
  if (xmlchk != TRUE) {log$error(
      base::paste0(" ====== def.read.cal.xml will not run due to the error in xml,  ",NameFile))
    base::stop()
  }
  # Happy path 1: maxBurstIdx < 20
  testFile = "sunav2_File5_01-02.parquet"
  testFilesPath <- paste0(testDir, testFile)
  # Get the filenames without path information
  # nameFileCal <- base::unlist(base::lapply(strsplit(fileCal,'/'),utils::tail,n=1))
 
  wq_abs_corr_returned <- NEONprocIS.wq::def.wq.abs.corr(
      sunav2Filenames = testFilesPath,
      sunav2CalFilenames = testFileCalPath,
      log = NULL
    )
  
  col_List = c("readout_time","Abs_ex","Abs_em","ucrt_A_ex","ucrt_A_em","spectrumCount","fDOMAbsQF")
  expect_true ((is.data.frame(wq_abs_corr_returned)) && !(is.null(wq_abs_corr_returned)))

  # columns returned are readout_time, abs_ex, abs_em, ucrt_A_ex, ucrt_A_em, spectrumCount, fDOMAbsQF 
  expect_true (all (names(wq_abs_corr_returned) ==  col_List))
  
  # Happy path 2: maxBurstIdx > 20
  
  testFile = "sunav2_File4.parquet"
  testFilesPath <- paste0(testDir, testFile)
  # Get the filenames without path information
  # nameFileCal <- base::unlist(base::lapply(strsplit(fileCal,'/'),utils::tail,n=1))
  
  wq_abs_corr_returned <- NEONprocIS.wq::def.wq.abs.corr(
    sunav2Filenames = testFilesPath,
    sunav2CalFilenames = testFileCalPath,
    log = NULL
  )
  
  col_List = c("readout_time","Abs_ex","Abs_em","ucrt_A_ex","ucrt_A_em","spectrumCount","fDOMAbsQF")
  expect_true ((is.data.frame(wq_abs_corr_returned)) && !(is.null(wq_abs_corr_returned)))
 
  # columns returned are readout_time, abs_ex, abs_em, ucrt_A_ex, ucrt_A_em, spectrumCount, fDOMAbsQF 
  expect_true (all (names(wq_abs_corr_returned) ==  col_List))
  
  ############################################################################################
  # Change the calibration 
  # CalibrationTable does not have 256 rows
  #
  # Sad path 1:  CalibrationTable has 255 rows
  
  testFileCal = "sunav2_calibration_255row.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent = TRUE)
  
  if (xmlchk != TRUE) {
    log$error(
      base::paste0(" ====== def.read.cal.xml will not run due to the error in xml,  ",NameFile))
    base::stop()
  }
  
  wq_abs_corr_returned <- try(NEONprocIS.wq::def.wq.abs.corr(
    sunav2Filenames = testFilesPath,
    sunav2CalFilenames = testFileCalPath,
    log = NULL),silent = FALSE)
  
  expect_true(class(wq_abs_corr_returned)[1] == "try-error")
  #
  # Sad path 2:  calibration does not have CalibrationTable
  #  
  testFileCal = "sunav2_noCalTbl_calibration.xml"
  testFileCalPath <- paste0(calDir, testFileCal)
  NameFile = testFileCalPath
  
  xmlchk <- try(NEONprocIS.base::def.validate.xml.schema(NameFile, xsd1),silent = TRUE)
  
  if (xmlchk != TRUE) {
    log$error(
      base::paste0(" ====== def.read.cal.xml will not run due to the error in xml,  ",NameFile))
    base::stop()
  }
  
  wq_abs_corr_returned <- try(NEONprocIS.wq::def.wq.abs.corr(
      sunav2Filenames = testFilesPath,
      sunav2CalFilenames = testFileCalPath,
      log = NULL),silent = FALSE)
  
  expect_true(class(wq_abs_corr_returned)[1] == "try-error")
})