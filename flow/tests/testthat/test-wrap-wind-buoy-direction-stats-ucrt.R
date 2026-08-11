##############################################################################################
#' @title Unit test for Wrapper for buoy wind-specific direction statistics and uncertainty calculations
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.wind.buoy.direction.stats.ucrt.R. Tests basic
#' file/directory input/output, schema argument passing, and successful output of
#' wind direction statistics for configured averaging windows.
#'
#' @param DirIn Character value. The base file path to the input data, uncertainty
#' coefficients, and thresholds.
#'
#' @param DirOutBase Character value. The base file path for the output data.
#'
#' @param WndwAgr Character vector. Averaging windows in minutes.
#'
#' @param SchmStatsOut (optional), A json-formatted character string containing the
#' schema for the output stats file.
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

context("\n                       Unit test of wrap.wind.buoy.direction.stats.ucrt.R\n")

test_that("Unit test of wrap.wind.buoy.direction.stats.ucrt.R", {

  source('../../flow.wind.buoy.direction.stats.ucrt/wrap.wind.buoy.direction.stats.ucrt.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  library(dplyr)

  # Set up working directories and schemas.
  workingDirPath <- getwd()
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_direction/2025/12/17/wind-buoy_BARC103100/rmyoung/CFGLOC110692')
  testDirOut <- file.path(workingDirPath, 'pfs/out')
  testStatsSchmFile <- file.path(workingDirPath, 'pfs/windBuoy_avro_schemas/windBuoy/windBuoy_direction_stats.avsc')
  testStatsSchmOut <- base::paste0(base::readLines(testStatsSchmFile), collapse = '')
  wndwAgr <- c('002', '030')

  # Get expected output repo/day path.
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test 1: NULL output schema is accepted and expected outputs are written.
  wrap.wind.buoy.direction.stats.ucrt(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    WndwAgr = wndwAgr,
    log = log
  )
  testthat::expect_true(file.exists(testDirOutPath, recursive = TRUE))

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test 2: Non-NULL output schema is accepted and expected outputs are written.
  wrap.wind.buoy.direction.stats.ucrt(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    WndwAgr = wndwAgr,
    SchmStatsOut = testStatsSchmOut,
    log = log
  )

  testthat::expect_true(file.exists(testDirOutPath, recursive = TRUE))

  dirOutStats <- file.path(testDirOutPath, 'stats')
  testthat::expect_true(dir.exists(dirOutStats))

  statsFiles <- base::list.files(dirOutStats, full.names = TRUE)
  testthat::expect_true(length(statsFiles) == length(wndwAgr))
  testthat::expect_true(any(grepl('_sciStats_002\\.parquet$', basename(statsFiles))))
  testthat::expect_true(any(grepl('_sciStats_030\\.parquet$', basename(statsFiles))))

  stats002 <- try(
    NEONprocIS.base::def.read.parq(
      NameFile = statsFiles[grepl('_sciStats_002\\.parquet$', basename(statsFiles))][1],
      log = log
    ),
    silent = FALSE
  )
  stats030 <- try(
    NEONprocIS.base::def.read.parq(
      NameFile = statsFiles[grepl('_sciStats_030\\.parquet$', basename(statsFiles))][1],
      log = log
    ),
    silent = FALSE
  )

  testthat::expect_true(length(stats002) != 0)
  testthat::expect_true(length(stats030) != 0)
  testthat::expect_true(nrow(stats002) > 0)
  testthat::expect_true(nrow(stats030) > 0)

  # Validate core output fields from stats and uncertainty calculations.
  reqCols <- c(
    'startDateTime',
    'endDateTime',
    'source_id',
    'site_id',
    'buoyWindDirMean',
    'buoyWindDirVariance',
    'buoyWindDirNumPts',
    'buoyWindDirStdErMean',
    'buoyWindDirExpUncert'
  )
  testthat::expect_true(all(reqCols %in% names(stats002)))
  testthat::expect_true(all(reqCols %in% names(stats030)))

  validDir002 <- stats002$buoyWindDirMean[!is.na(stats002$buoyWindDirMean)]
  validDir030 <- stats030$buoyWindDirMean[!is.na(stats030$buoyWindDirMean)]
  testthat::expect_true(all(validDir002 >= 0 & validDir002 < 360))
  testthat::expect_true(all(validDir030 >= 0 & validDir030 < 360))

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})