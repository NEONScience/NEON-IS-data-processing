##############################################################################################
#' @title Unit test for Wrapper for SUNA Log File Comparison and Gap Filling
#' 
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#' 
#' @description Unit test for the wrapper function. Compares logged SUNA data to streamed data and fills gaps.
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/sensor/yyyy/mm/dd/source-id
#' 
#' @param DirInStream (optional) Character value. This input is used for testing purposes only prior to joining repos.
#' The input path to the streamed L0 data from a single source ID.
#' 
#' @param DirInLogs (optional) Character value. This input is used for testing purposes only prior to joining repos.
#' The input path to the log data from a single source ID.
#' 
#' @param DirOutBase Character value. The output base directory.
#' 
#' @param SchmDataOut (optional) Schema for output data file.
#' 
#' @param SchmFlagsOut (optional) Schema for output flags file.
#' 
#' @param log Logger object as produced by NEONprocIS.base::def.log.init
#' 
#' @return Combined logged and streamed L0 data in daily parquets.
#' 
#' @examples
#' # Not run
# DirInLogs <- "~/pfs/sunav2_logjam_assign_clean_files/sunav2/2024/09/11/20349"
# DirInStream <- "~/pfs/sunav2_trino_data_parser/sunav2/2025/06/22/20345"
# DirIn <- NULL
# DirOutBase <- "~/pfs/out"
# SchmDataOut <- base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2/sunav2_logfilled.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# SchmFlagsOut <- base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'),collapse='')
# wrap.sunav2.logfiles.fill(DirInLogs, DirInStream, DirIn, DirOutBase, SchmDataOut, SchmFlagsOut, log)
#'
# changelog and author contributions 
#   Nora Catolico (2026-02-09)
#     Original Creation 
##############################################################################################
context("\n                       Unit test of wrap.sunav2.logfiles.fill.R\n")

test_that("Unit test of wrap.sunav2.logfiles.fill.R", {
  
  source('../../flow.sunav2.logfiles.fill/wrap.sunav2.logfiles.fill.R')
  library(stringr)
  
  workingDirPath <- getwd()
  testDirOut = file.path(workingDirPath, 'pfs/out')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  # Test 1: SUNAV2, only directories and output passed in, no schema
  DirInLogs <- file.path(workingDirPath, 'pfs/sunav2_logjam_assign_clean_files/sunav2/2024/09/10/20349')
  DirInStream <- file.path(workingDirPath, 'pfs/sunav2_trino_data_parser/sunav2/2024/09/10/20349')
  DirIn <- NULL
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInLogs)
  subDirParts <- InfoDirIn$dirRepo
  subDir <- paste0(subDirParts, collapse = '', sep = '/')
  subDirPath <- file.path(subDir)
  testOutputDirPath <- base::paste0(testDirOut, "/", subDirPath)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.sunav2.logfiles.fill(
    DirInLogs = DirInLogs,
    DirInStream = DirInStream,
    DirIn = DirIn,
    DirOutBase = testDirOut,
    SchmDataOut = NULL,
    SchmFlagsOut = NULL,
    log = log
  )
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  # Test 2: SUNAV2, schema files provided
  SchmDataOut <- base::paste0(base::readLines('pfs/sunav2_avro_schemas/sunav2_logfilled.avsc'), collapse = '')
  SchmFlagsOut <- base::paste0(base::readLines('pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'), collapse = '')
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.sunav2.logfiles.fill(
    DirInLogs = DirInLogs,
    DirInStream = DirInStream,
    DirIn = NULL,
    DirOutBase = testDirOut,
    SchmDataOut = SchmDataOut,
    SchmFlagsOut = SchmFlagsOut,
    log = log
  )
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  # Test 3: SUNAV2, DirInStream does not have files, only logs available
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  DirInStreamEmpty <- file.path(workingDirPath, 'pfs/sunav2_trino_data_parser/sunav2/2024/09/11/20349_empty')
  InfoDirInEmpty <- NEONprocIS.base::def.dir.splt.pach.time(DirInStreamEmpty)
  subDirPartsEmpty <- InfoDirInEmpty$dirRepo
  subDirEmpty <- paste0(subDirPartsEmpty, collapse = '', sep = '/')
  subDirPathEmpty <- file.path(subDirEmpty)
  testOutputDirPathEmpty <- base::paste0(testDirOut, "/", subDirPathEmpty)
  
  wrap.sunav2.logfiles.fill(
    DirInLogs = DirInLogs,
    DirInStream = DirInStreamEmpty,
    DirIn = DirIn,
    DirOutBase = testDirOut,
    SchmDataOut = SchmDataOut,
    SchmFlagsOut = SchmFlagsOut,
    log = log
  )
  
  testthat::expect_true(file.exists(file.path(testOutputDirPathEmpty, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPathEmpty, "flags")))
  
  # Test 4: SUNAV2, DirInLogs is empty, only stream available
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  DirInLogsEmpty <- file.path(workingDirPath, 'pfs/sunav2_logjam_assign_clean_files/sunav2/2024/09/11/20349_empty')
  
  wrap.sunav2.logfiles.fill(
    DirInLogs = DirInLogsEmpty,
    DirInStream = DirInStream,
    DirIn = DirIn,
    DirOutBase = testDirOut,
    SchmDataOut = SchmDataOut,
    SchmFlagsOut = SchmFlagsOut,
    log = log
  )
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))

  
})