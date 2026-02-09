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
#' @param DirOut Character value. The base file path for the output data. 
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
  
  ## Test 1: Only input and output directories passed in
  wrap.sunav2.quality.flags (DirIn=testDirIn,
                             DirOutBase=testDirOut)
  testthat::expect_true(file.exists(testDirOutPath))
  
  ## Test 2: Not NULL Schema is passed in
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  wrap.sunav2.quality.flags (DirIn=testDirIn,
                             DirOutBase=testDirOut,
                             SchmFlagsOut=testSchmFlagsOut,
                             log=log)
  testthat::expect_true(file.exists(testDirOutPath))
  
  
  ## Additional checks:
  # Check that the output files (data & flags) exist in their expected locations
  DirOutData  <- file.path(testDirOutPath, 'data')
  DirOutFlags <- file.path(testDirOutPath, 'flags')
  testthat::expect_true(dir.exists(DirOutData))
  testthat::expect_true(dir.exists(DirOutFlags))
  # You may add further checks for file output, schema format, etc.
  
})