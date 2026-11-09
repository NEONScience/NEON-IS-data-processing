##############################################################################################
#' @title Unit test for Wrapper for fDOM temperature and absorbance corrections
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.wq.fdom.corr.R. Tests basic file/directory input/output,
#' schema argument passing, and successful output of fDOM corrected data and quality flags.
#'
#' @param DirIn Character value. The base file path to the exo2 and sunav2 data.
#'
#' @param DirOutBase Character value. The base file path for the output data.
#'
#' @param SchmData (optional) A json-formatted character string containing the schema for the output data parquet.
#'
#' @param SchmQf (optional) A json-formatted character string containing the schema for the output flags parquet.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#'
#' changelog and author contributions
#   Nora Catolico (2026-11-09)
#'     Original Creation
#
##############################################################################################

context("\n                       Unit test of wrap.wq.fdom.corr.R\n")

test_that("Unit test of wrap.wq.fdom.corr.R - Basic output structure", {

  source('../../flow.wq.fdom.corr/wrap.wq.fdom.corr.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  # Set up working directories and schemas
  workingDirPath <- getwd()

  testDirIn <- file.path(workingDirPath,
    'pfs/water_quality_exo2fdom_correct_group',
    '2025/09/04/water-quality_POSE102100'
  )
  testDirOut <- file.path(workingDirPath, 'pfs/water_quality_out')

  fdomFilePath <- file.path(testDirIn, 'exo2fdom/CFGLOC123456/data/exo2fdom_CFGLOC123456_2025-09-04_data_001.parquet')
  conductivityFilePath <- file.path(testDirIn, 'exo2conductivity/CFGLOC123456/data/exo2conductivity_CFGLOC123456_2025-09-04_data_001.parquet')
  ucrtDataDir <- file.path(testDirIn,'exo2conductivity/CFGLOC123456/uncertainty_data/exo2conductivity_CFGLOC123456_2025-09-04_uncertainty_001.parquet')

  # --------------------------------------------------------------------------------------------
  # Test 1: Basic wrapper execution without schemas
  # --------------------------------------------------------------------------------------------
  wrap.wq.fdom.corr(
    DirIn = file.path(testDirIn),
    DirOutBase = testDirOut,
    SchmData = NULL,
    SchmQf = NULL,
    log = log
  )

  # --------------------------------------------------------------------------------------------
  # Test 2: Verify output directory structure is created
  # --------------------------------------------------------------------------------------------
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  dirOutBase <- base::paste0(testDirOut, testDirRepo)

  dirOutData <- file.path(dirOutBase, 'exo2fdom/CFGLOC123456/data')
  dirOutFlags <- file.path(dirOutBase, 'exo2fdom/CFGLOC123456/flags')

  testthat::expect_true(dir.exists(dirOutData))
  testthat::expect_true(dir.exists(dirOutFlags))

  # --------------------------------------------------------------------------------------------
  # Test 3: Verify data output file exists and contains expected columns
  # --------------------------------------------------------------------------------------------
  dataFiles <- list.files(dirOutData, pattern = '_basicStats_', full.names = TRUE)
  testthat::expect_equal(length(dataFiles), 1)

  fdomOut <- try(
    NEONprocIS.base::def.read.parq(NameFile = dataFiles[1], log = log),
    silent = FALSE
  )
  testthat::expect_false(inherits(fdomOut, 'try-error'))
  testthat::expect_true(length(fdomOut) != 0)
  testthat::expect_true(nrow(fdomOut) > 0)

  # Verify required columns exist
  requiredDataCols <- c('startDateTime', 'endDateTime', 'fDOM', 'rawCalibratedfDOM', 'fDOMExpUncert', 'spectrumCount')
  testthat::expect_true(all(requiredDataCols %in% names(fdomOut)))

  # --------------------------------------------------------------------------------------------
  # Test 4: Verify flags output file exists and contains expected columns
  # --------------------------------------------------------------------------------------------
  flagFiles <- list.files(dirOutFlags, pattern = '_flagsCorrection', full.names = TRUE)
  testthat::expect_equal(length(flagFiles), 1)

  fdomFlags <- try(
    NEONprocIS.base::def.read.parq(NameFile = flagFiles[1], log = log),
    silent = FALSE
  )
  testthat::expect_false(inherits(fdomFlags, 'try-error'))
  testthat::expect_true(length(fdomFlags) != 0)
  testthat::expect_true(nrow(fdomFlags) > 0)

  # Verify required flag columns exist
  requiredFlagCols <- c('readout_time', 'fDOMTempQF', 'fDOMAbsQF')
  testthat::expect_true(all(requiredFlagCols %in% names(fdomFlags)))

  # --------------------------------------------------------------------------------------------
  # Test 5: Verify data types of output columns
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(inherits(fdomOut$startDateTime, c('POSIXct', 'POSIXt')))
  testthat::expect_true(is.numeric(fdomOut$fDOM))
  testthat::expect_true(is.numeric(fdomOut$spectrumCount))

  # --------------------------------------------------------------------------------------------
  # Test 6: Verify flag values are valid (should be -1, 0, 1, or 2)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(fdomFlags$fDOMTempQF %in% c(-1, 0, 1, 2)))
  testthat::expect_true(all(fdomFlags$fDOMAbsQF %in% c(-1, 0, 1, 2)))

  # --------------------------------------------------------------------------------------------
  # Test 7: Verify temperature flag is 0 when temperature data is available
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(any(fdomFlags$fDOMTempQF == 0, na.rm = TRUE))

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
})
