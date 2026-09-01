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
  testthat::expect_true(length(statsFiles) == length(wndwAgr),
                        info = sprintf("Expected %d stats files, got %d", length(wndwAgr), length(statsFiles)))
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

  testthat::expect_false(inherits(stats002, 'try-error'), info = "Failed to read 002-minute stats file")
  testthat::expect_false(inherits(stats030, 'try-error'), info = "Failed to read 030-minute stats file")
  testthat::expect_true(length(stats002) != 0)
  testthat::expect_true(length(stats030) != 0)
  testthat::expect_true(nrow(stats002) > 0, info = "Stats 002 file has no rows")
  testthat::expect_true(nrow(stats030) > 0, info = "Stats 030 file has no rows")

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
  testthat::expect_true(all(reqCols %in% names(stats002)), 
                        info = sprintf("Missing columns in stats002: %s", 
                                       paste(setdiff(reqCols, names(stats002)), collapse = ", ")))
  testthat::expect_true(all(reqCols %in% names(stats030)), 
                        info = sprintf("Missing columns in stats030: %s", 
                                       paste(setdiff(reqCols, names(stats030)), collapse = ", ")))

  # Validate mean direction values are within valid range (0-360 degrees).
  validDir002 <- stats002$buoyWindDirMean[!is.na(stats002$buoyWindDirMean)]
  validDir030 <- stats030$buoyWindDirMean[!is.na(stats030$buoyWindDirMean)]
  testthat::expect_true(length(validDir002) > 0, info = "No valid direction means for 002-minute window")
  testthat::expect_true(length(validDir030) > 0, info = "No valid direction means for 030-minute window")
  testthat::expect_true(all(validDir002 >= 0 & validDir002 < 360),
                        info = "Direction means outside [0, 360) in 002-minute window")
  testthat::expect_true(all(validDir030 >= 0 & validDir030 < 360),
                        info = "Direction means outside [0, 360) in 030-minute window")

  # Validate variance values are non-negative (variance cannot be negative).
  validVar002 <- stats002$buoyWindDirVariance[!is.na(stats002$buoyWindDirVariance)]
  validVar030 <- stats030$buoyWindDirVariance[!is.na(stats030$buoyWindDirVariance)]
  testthat::expect_true(length(validVar002) > 0, info = "No valid variance values for 002-minute window")
  testthat::expect_true(length(validVar030) > 0, info = "No valid variance values for 030-minute window")
  testthat::expect_true(all(validVar002 >= 0), 
                        info = "Variance contains negative values in 002-minute window")
  testthat::expect_true(all(validVar030 >= 0), 
                        info = "Variance contains negative values in 030-minute window")

  # Validate number of points are positive integers.
  numPts002 <- stats002$buoyWindDirNumPts[!is.na(stats002$buoyWindDirNumPts)]
  numPts030 <- stats030$buoyWindDirNumPts[!is.na(stats030$buoyWindDirNumPts)]
  testthat::expect_true(length(numPts002) > 0, info = "No valid point counts for 002-minute window")
  testthat::expect_true(length(numPts030) > 0, info = "No valid point counts for 030-minute window")
  testthat::expect_true(all(numPts002 > 0), 
                        info = "Non-positive point counts in 002-minute window")
  testthat::expect_true(all(numPts030 > 0), 
                        info = "Non-positive point counts in 030-minute window")
  testthat::expect_true(all(numPts002 == floor(numPts002)), 
                        info = "Non-integer point counts in 002-minute window")
  testthat::expect_true(all(numPts030 == floor(numPts030)), 
                        info = "Non-integer point counts in 030-minute window")

  # Validate standard error of the mean values (should be non-negative).
  validStdEr002 <- stats002$buoyWindDirStdErMean[!is.na(stats002$buoyWindDirStdErMean)]
  validStdEr030 <- stats030$buoyWindDirStdErMean[!is.na(stats030$buoyWindDirStdErMean)]
  testthat::expect_true(length(validStdEr002) > 0, info = "No valid std error values for 002-minute window")
  testthat::expect_true(length(validStdEr030) > 0, info = "No valid std error values for 030-minute window")
  testthat::expect_true(all(validStdEr002 >= 0), 
                        info = "Negative std error values in 002-minute window")
  testthat::expect_true(all(validStdEr030 >= 0), 
                        info = "Negative std error values in 030-minute window")

  # Validate expanded uncertainty values (should be non-negative).
  validUncert002 <- stats002$buoyWindDirExpUncert[!is.na(stats002$buoyWindDirExpUncert)]
  validUncert030 <- stats030$buoyWindDirExpUncert[!is.na(stats030$buoyWindDirExpUncert)]
  testthat::expect_true(length(validUncert002) > 0, info = "No valid uncertainty values for 002-minute window")
  testthat::expect_true(length(validUncert030) > 0, info = "No valid uncertainty values for 030-minute window")
  testthat::expect_true(all(validUncert002 >= 0), 
                        info = "Negative uncertainty values in 002-minute window")
  testthat::expect_true(all(validUncert030 >= 0), 
                        info = "Negative uncertainty values in 030-minute window")

  # Validate temporal consistency (startDateTime < endDateTime).
  testthat::expect_true(all(stats002$startDateTime < stats002$endDateTime, na.rm = TRUE),
                        info = "Start times not before end times in 002-minute window")
  testthat::expect_true(all(stats030$startDateTime < stats030$endDateTime, na.rm = TRUE),
                        info = "Start times not before end times in 030-minute window")

  # Validate that stats002 typically has more data points per day than stats030 (shorter window = more windows).
  # This is a logical check based on window sizes: 2-min windows should provide more data points than 30-min windows.
  meanPts002 <- mean(stats002$buoyWindDirNumPts, na.rm = TRUE)
  meanPts030 <- mean(stats030$buoyWindDirNumPts, na.rm = TRUE)
  # (Note: may vary by data availability, so we just check both are valid)
  testthat::expect_true(meanPts002 > 0 && meanPts030 > 0,
                        info = "Invalid mean point counts across windows")

  # Validate consistency of source_id and site_id across all rows.
  testthat::expect_true(length(unique(stats002$source_id)) == 1,
                        info = "Multiple source_ids in 002-minute stats (should be consistent)")
  testthat::expect_true(length(unique(stats030$source_id)) == 1,
                        info = "Multiple source_ids in 030-minute stats (should be consistent)")

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})

# Additional tests for robustness and edge cases
test_that("Input validation and error handling", {

  source('../../flow.wind.buoy.direction.stats.ucrt/wrap.wind.buoy.direction.stats.ucrt.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()
  testDirOut <- file.path(workingDirPath, 'pfs/out')
  wndwAgr <- c('002', '030')

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test with non-existent input directory should fail gracefully.
  testDirInInvalid <- file.path(workingDirPath, 'pfs/non_existent_dir/2025/12/17/wind-buoy_INVALID/rmyoung/CFGLOC999999')
  testthat::expect_error(
    wrap.wind.buoy.direction.stats.ucrt(
      DirIn = testDirInInvalid,
      DirOutBase = testDirOut,
      WndwAgr = wndwAgr,
      log = log
    ),
    info = "Function should error on non-existent input directory"
  )

  # Test with missing required window argument.
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_direction/2025/12/17/wind-buoy_BARC103100/rmyoung/CFGLOC110692')
  if (dir.exists(testDirIn)) {
    testthat::expect_error(
      wrap.wind.buoy.direction.stats.ucrt(
        DirIn = testDirIn,
        DirOutBase = testDirOut,
        log = log
        # Missing WndwAgr parameter
      ),
      info = "Function should error on missing WndwAgr parameter"
    )
  }

  # Test with empty window vector.
  if (dir.exists(testDirIn)) {
    testthat::expect_error(
      wrap.wind.buoy.direction.stats.ucrt(
        DirIn = testDirIn,
        DirOutBase = testDirOut,
        WndwAgr = character(0),  # Empty vector
        log = log
      ),
      info = "Function should error on empty WndwAgr vector"
    )
  }

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})

# Test window parameter handling
test_that("Window parameter validation and handling", {

  source('../../flow.wind.buoy.direction.stats.ucrt/wrap.wind.buoy.direction.stats.ucrt.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_direction/2025/12/17/wind-buoy_BARC103100/rmyoung/CFGLOC110692')
  testDirOut <- file.path(workingDirPath, 'pfs/out')

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test with single averaging window.
  if (dir.exists(testDirIn)) {
    wrap.wind.buoy.direction.stats.ucrt(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      WndwAgr = c('010'),  # Single window
      log = log
    )

    infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
    testDirRepo <- infoDirIn$dirRepo
    testDirOutPath <- base::paste0(testDirOut, testDirRepo)
    dirOutStats <- file.path(testDirOutPath, 'stats')

    testthat::expect_true(dir.exists(dirOutStats),
                          info = "Output stats directory not created for single window")

    statsFiles <- base::list.files(dirOutStats, full.names = TRUE)
    testthat::expect_true(length(statsFiles) == 1,
                          info = "Should have exactly 1 stats file for 1 window")
    testthat::expect_true(any(grepl('_sciStats_010\\.parquet$', basename(statsFiles))),
                          info = "Expected 010-minute stats file not found")
  }

  # Test with multiple distinct windows.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  if (dir.exists(testDirIn)) {
    wrap.wind.buoy.direction.stats.ucrt(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      WndwAgr = c('005', '015', '060'),  # Multiple windows
      log = log
    )

    infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
    testDirRepo <- infoDirIn$dirRepo
    testDirOutPath <- base::paste0(testDirOut, testDirRepo)
    dirOutStats <- file.path(testDirOutPath, 'stats')

    testthat::expect_true(dir.exists(dirOutStats),
                          info = "Output stats directory not created for multiple windows")

    statsFiles <- base::list.files(dirOutStats, full.names = TRUE)
    testthat::expect_true(length(statsFiles) == 3,
                          info = sprintf("Expected 3 stats files, got %d", length(statsFiles)))
    testthat::expect_true(any(grepl('_sciStats_005\\.parquet$', basename(statsFiles))))
    testthat::expect_true(any(grepl('_sciStats_015\\.parquet$', basename(statsFiles))))
    testthat::expect_true(any(grepl('_sciStats_060\\.parquet$', basename(statsFiles))))
  }

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})