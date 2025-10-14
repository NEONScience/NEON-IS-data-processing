##############################################################################################
#' @title Unit test of def.cal.meta.R, Compile metadata for calibrations

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description

#' Run unit tests for def.cal.meta.R, Read in any number of calibration files and compile their metadata, including
#' file name, certificate number, and valid date range.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or have invalid values

#' Refer to def.cal.meta for the details of the function.

#' @param fileCal Character vector of the full or relative paths to each calibration file
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of calibration metadata

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' # Not run
#' # fileCal <- c('/path/to/file1.xml','/path/to/file2.xml')
#' # metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=fileCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export


#' @return TRUE when a test passes. The test checks to ensure that a valid data frame is returned.
#' Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' # Not run
#' # fileCal <- c('/path/to/file1.xml','/path/to/file2.xml')
#' # metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=fileCal)

#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

#' @seealso \link[NEONprocIS.cal]{def.cal.meta}
#'
#' @export
# changelog and author contributions / copyrights
#   Mija Choi (2020-04-08)
#     original creation
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
##############################################################################################
# Define test context
#context("\n       |testing def.cal.meta.R, Compile metadata for calibrations\n")

source('../../../../neon-package-loader.R')
load_neon_base()

# Test def.cal.meta.R, Compile metadata for calibrations
test_that("   Test of def.cal.meta.R, Compile metadata for calibrations",
          {
            ##########
            ##########  Happy path:::: All the input parameters are valid
            ##########
            ########## fileCal: multiple calibration xmls
            ########## metaCal: a data frame returned.
            
            testDir = "testdata/"
            
            testFileCal <- c('calibration.xml','calibration2.xml','calibration3.xml','calibration4.xml')
            testFileCalPath <- paste0(testDir, testFileCal)
            
            cat("\n       |=====================================   Test Summary   ====================================|\n")
            
            cat("\n       |------ Positive test 1:: All the input parameters are valid                                |\n")
            
            metaCal <-
              NEONprocIS.cal::def.cal.meta(fileCal = testFileCalPath)
            
            NameList = c('path', 'file', 'timeValiBgn', 'timeValiEnd', 'id','err')
            
            expect_true (is.data.frame(metaCal))
            expect_true (all(NameList %in% colnames(metaCal)))
            expect_true (all(!metaCal$err))
            
            cat("\n       |------                   The test ran successfully, a correct Data frame is returned       |\n")
            
            fileCal_noCertNum <- c('calibration.xml','calibration5_NoCertNum.xml','calibration6_BadFile.xml')
            testFileCalPath <- paste0(testDir, fileCal_noCertNum)
            
            cat("\n       |=====================================   Test Summary   ====================================|\n")
            
            cat("\n       |------ Negative test 1:: second cal file is missing the cert number, 3rd file bad          |\n")
            
            metaCal <- try(NEONprocIS.cal::def.cal.meta(fileCal = testFileCalPath), silent = TRUE)
              
            testthat::expect_true(!("try-error" %in% class(metaCal)) && metaCal$err[1] == FALSE)
            testthat::expect_true(!("try-error" %in% class(metaCal)) && metaCal$err[2] == TRUE && is.na(metaCal$id[2]))
            testthat::expect_true(!("try-error" %in% class(metaCal)) && metaCal$err[3] == TRUE && is.na(metaCal$timeValiEnd[3]))
            
            
          })
