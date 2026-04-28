##############################################################################################
#' @title Unit test for Wrapper for SUNA configuration quality flag
#' 
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.sunav2.config.qf.R. Tests basic file/directory input/output,
#' schema argument passing, and successful output of SUNA config flagged files.
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
#   Nora Catolico (2026-04-14)
#     Original Creation 
#
##############################################################################################

context("\n                       Unit test of wrap.sunav2.config.qf.R\n")

test_that("Unit test of wrap.sunav2.config.qf.R", {
  
  source('../../flow.sunav2.config.qf/wrap.sunav2.config.qf.R')
  library(stringr)
  library(dplyr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Set up working directories and schemas
  workingDirPath <- getwd()
  #testDirIn  <- file.path(workingDirPath, 'flow/tests/testthat/pfs/nitrate_null_gap_ucrt_config/CFGLOC113620')
  #testDirOut <- file.path(workingDirPath, 'flow/tests/testthat/pfs/nitrate_out')
  #testSchmOutDir <- file.path(workingDirPath, 'flow/tests/testthat/pfs/sunav2_avro_schemas/nitrate/nitrate_config_qf.avsc')
  testDirIn  <- file.path(workingDirPath, 'pfs/nitrate_null_gap_ucrt_config/CFGLOC113620')
  testDirOut <- file.path(workingDirPath, 'pfs/nitrate_out')
  testSchmOutDir <- file.path(workingDirPath, 'pfs/sunav2_avro_schemas/nitrate/nitrate_config_qf.avsc')
  testSchmOut <- base::paste0(base::readLines(testSchmOutDir), collapse='')
  
  # Get repo info and output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- InfoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  ## Test 1: Not NULL Schema is passed in
  wrap.sunav2.config.qf (DirIn=testDirIn,
                         DirOutBase=testDirOut,
                         SchmQMs=testSchmOut,
                         log=log)
  testthat::expect_true(file.exists(testDirOutPath))
  
  
  # Check that the output files (QM & stats) exist in their expected locations
  DirOutStats  <- file.path(testDirOutPath, 'stats')
  DirOutQM <- file.path(testDirOutPath, 'quality_metrics')
  testthat::expect_true(dir.exists(DirOutStats))
  testthat::expect_true(dir.exists(DirOutQM))
  
  # check that finalQF is 1 where nitrateConfigQF is 1
  qmFileName<-base::list.files(DirOutQM,full.names=FALSE)
  sunaQMs<-try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirOutQM, '/', qmFileName),
                                              log = log),silent=FALSE)
  testthat::expect_true(length(sunaQMs) != 0)
  testthat::expect_true('nitrateConfigQF' %in% names(sunaQMs))
  testthat::expect_true(any(sunaQMs$nitrateConfigQF == 1, na.rm = TRUE))
  testthat::expect_true(all(sunaQMs$finalQF[sunaQMs$nitrateConfigQF == 1] == 1))
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
})
