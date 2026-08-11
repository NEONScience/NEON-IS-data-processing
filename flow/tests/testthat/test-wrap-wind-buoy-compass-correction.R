##############################################################################################
#' @title Unit test for Wrapper for buoy wind-specific quality flagging and compass correction
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.wind.buoy.compass.correction.R. Tests basic file/directory
#' input/output, schema argument passing, and successful output of corrected wind data and
#' dead band/calm wind flags.
#'
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility
#' flags and quality flag thresholds.
#'
#' @param DirOutBase Character value. The base file path for the output data.
#'
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for
#' the output data file.
#'
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for
#' the output flags file.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce
#' structured log output.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#'
#' changelog and author contributions
#   Nora Catolico (2026-08-11)
#     Original Creation
#
##############################################################################################

context("\n                       Unit test of wrap.wind.buoy.compass.correction.R\n")

test_that("Unit test of wrap.wind.buoy.compass.correction.R", {

  source('../../flow.wind.buoy.compass.correction/wrap.wind.buoy.compass.correction.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  # Set up working directories and schemas.
  workingDirPath <- getwd()
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_threshold_select/2025/12/17/wind-buoy_BARC103100')
  testDirOut <- file.path(workingDirPath, 'pfs/out')
  testDataSchmFile <- file.path(workingDirPath, 'pfs/windBuoy_avro_schemas/windBuoy/windBuoy_compass_corrected.avsc')
  testFlagSchmFile <- file.path(workingDirPath, 'pfs/windBuoy_avro_schemas/windBuoy/windBuoy_flags_deadcalm.avsc')

  testDataSchmOut <- base::paste0(base::readLines(testDataSchmFile), collapse = '')
  testFlagSchmOut <- base::paste0(base::readLines(testFlagSchmFile), collapse = '')

  # Get expected output repo/day path.
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test 1: NULL output schemas are accepted and expected outputs are written.
  wrap.wind.buoy.compass.correction(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      log = log
    )
  testthat::expect_true (file.exists(testDirOutPath, recursive = TRUE))
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test 2: Non-NULL output schemas are accepted and expected outputs are written.
  wrap.wind.buoy.compass.correction(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      SchmDataOut = testDataSchmOut,
      SchmFlagsOut = testFlagSchmOut,
      log = log
    )

  testthat::expect_true (file.exists(testDirOutPath, recursive = TRUE))

  # Discover CFGLOC directory dynamically from output so test is robust to fixture updates.
  cfgDirs <- base::list.dirs(file.path(testDirOutPath, 'rmyoung'), recursive = FALSE, full.names = TRUE)
  cfgDirs <- cfgDirs[grepl('CFGLOC', basename(cfgDirs))]
  testthat::expect_true(length(cfgDirs) == 1)

  dirOutData <- file.path(cfgDirs[1], 'data')
  dirOutFlags <- file.path(cfgDirs[1], 'flags')
  testthat::expect_true(dir.exists(dirOutData))
  testthat::expect_true(dir.exists(dirOutFlags))

  dataFiles <- base::list.files(dirOutData, full.names = TRUE)
  flagFiles <- base::list.files(dirOutFlags, full.names = TRUE)
  testthat::expect_true(length(dataFiles) == 1)
  testthat::expect_true(length(flagFiles) == 1)

  windData <- try(NEONprocIS.base::def.read.parq(NameFile = dataFiles[1], log = log), silent = FALSE)
  windFlags <- try(NEONprocIS.base::def.read.parq(NameFile = flagFiles[1], log = log), silent = FALSE)

  testthat::expect_true(length(windData) != 0)
  testthat::expect_true(length(windFlags) != 0)

  # Validate core output fields from correction and flagging.
  testthat::expect_true('direction' %in% names(windData))
  testthat::expect_true('direction_rad' %in% names(windData))
  testthat::expect_true('vectorAverageHeading' %in% names(windData))
  testthat::expect_true('buoyWindDirDeadZone' %in% names(windFlags))
  testthat::expect_true('buoyWindDirCalmWind' %in% names(windFlags))

  validDir <- windData$direction[!is.na(windData$direction)]
  testthat::expect_true(all(validDir >= 0 & validDir < 360))

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})