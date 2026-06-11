##############################################################################################
#' @title Unit test for Wrapper for SUNA sensor-specific quality flagging
#' 
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.sunav2.quality.flags.R. Tests basic file/directory input/output,
#' schema argument passing, and successful output of SUNA quality flagged files.
#' 
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility flags and quality flag thresholds.
#'  
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param WndwMinPt Numeric value. The time window in minutes for which to keep at least one row if all other points are dropped during 
#' the lamp stabilization check.
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the data file.
#' This should be the same for the input as the output.  Only the number of rows of measurements should change. 
#' 
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for the output flags. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#'
#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' SchmDataOut <- base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_logfilled.avsc'),collapse='')
#' SchmFlagsOut <- base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_all_flags.avsc'),collapse='')
#' 
#' changelog and author contributions 
#   Nora Catolico (2026-02-09)
#     Original Creation 
#
##############################################################################################

context("\n                       Unit test of wrap.sunav2.quality.flags.R\n")

test_that("Unit test of wrap.sunav2.quality.flags.R", {
  
  source('../../flow.sunav2.quality.flags/wrap.sunav2.quality.flags.R')
  library(stringr)
  library(dplyr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Set up working directories and schemas
  workingDirPath <- getwd()
  testDirIn  <- file.path(workingDirPath, 'pfs/nitrate_analyze_pad_and_qaqc_plau/2025/06/25/nitrate_HOPB112100/sunav2/CFGLOC113620')
  testDirOut <- file.path(workingDirPath, 'pfs/nitrate_out')
  testSchmFlagsOutDir <- file.path(workingDirPath, 'pfs/sunav2_avro_schemas/nitrate/nitrate_all_flags.avsc')
  testSchmFlagsOut <- base::paste0(base::readLines(testSchmFlagsOutDir), collapse='')
  
  # Get repo info and output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- InfoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  ## Test 1: Input and output directories passed in with required WndwMinPt
  wrap.sunav2.quality.flags (DirIn=testDirIn,
                             WndwMinPt=15,
                             DirOutBase=testDirOut)
  testthat::expect_true(file.exists(testDirOutPath))
  
  ## Test 2: Not NULL Schema is passed in
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  wrap.sunav2.quality.flags (DirIn=testDirIn,
                             DirOutBase=testDirOut,
                             WndwMinPt=15,
                             SchmFlagsOut=testSchmFlagsOut,
                             log=log)
  testthat::expect_true(file.exists(testDirOutPath))
  
  
  ## Additional checks:
  # Check that the output files (data & flags) exist in their expected locations
  DirOutData  <- file.path(testDirOutPath, 'data')
  DirOutFlags <- file.path(testDirOutPath, 'flags')
  testthat::expect_true(dir.exists(DirOutData))
  testthat::expect_true(dir.exists(DirOutFlags))
  
  # Read the output data and flags files to verify lamp stabilization filtering behavior
  outDataFiles <- base::list.files(DirOutData, full.names = TRUE)
  outFlagsFiles <- base::list.files(DirOutFlags, full.names = TRUE)
  
  testthat::expect_true(length(outDataFiles) > 0, info = "Output data file should exist")
  testthat::expect_true(length(outFlagsFiles) > 0, info = "Output flags file should exist")
  
  if (length(outDataFiles) > 0 && length(outFlagsFiles) > 0) {
    outData <- NEONprocIS.base::def.read.parq(NameFile = outDataFiles[1], log = log)
    outFlags <- NEONprocIS.base::def.read.parq(NameFile = outFlagsFiles[1], log = log)
    
    # Verify output data and flags have same number of rows
    testthat::expect_equal(nrow(outData), nrow(outFlags), 
                           info = "Output data and flags should have equal row counts")
    
    # Verify that no non-placeholder measurements remain with nitrateLampStabilizeQF==1.
    # Lamp-stabilization flags are written to the flags parquet, while placeholder rows
    # with nitrate == NA may remain in the output data.
    testthat::expect_false(base::any(!is.na(outData$nitrate) &
                                       !is.na(outFlags$nitrateLampStabilizeQF) &
                                       outFlags$nitrateLampStabilizeQF == 1),
                           info = "No non-NA nitrate values should have nitrateLampStabilizeQF==1")
    
    # Verify that any remaining nitrateLampStabilizeQF==1 rows are placeholder rows
    testthat::expect_true(base::all(is.na(outData$nitrate[!is.na(outFlags$nitrateLampStabilizeQF) &
                                                          outFlags$nitrateLampStabilizeQF == 1])),
                          info = "Rows with nitrateLampStabilizeQF==1 should only remain as placeholder rows with nitrate == NA")
    
    # Verify that filtering does not increase the number of retained measurements.
    # Placeholder rows with nitrate == NA may be added, so total output rows can
    # legitimately exceed input rows.
    inputDataFiles <- base::list.files(file.path(testDirIn, 'data'), full.names = TRUE)
    if (length(inputDataFiles) > 0) {
      inputData <- NEONprocIS.base::def.read.parq(NameFile = inputDataFiles[1], log = log)
      outMeasuredRows <- base::sum(!is.na(outData$nitrate))
      testthat::expect_true(outMeasuredRows <= nrow(inputData),
                            info = "Non-placeholder output measurements should be <= input data rows after filtering")
    }
  }
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
})