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
#   Nora Catolico (2026-09-08)
#     Added test for handling of duplicated readout times in wind and compass data
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
      SensWind = "rmyoung",
      SensCompass = "hmr3300",
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
      SensWind = "rmyoung",
      SensCompass = "hmr3300",
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

  testthat::expect_false(inherits(windData, 'try-error'), info = "Failed to read wind data file")
  testthat::expect_false(inherits(windFlags, 'try-error'), info = "Failed to read wind flags file")
  testthat::expect_true(length(windData) != 0)
  testthat::expect_true(length(windFlags) != 0)

  # Validate core output fields from correction and flagging.
  testthat::expect_true('direction' %in% names(windData))
  testthat::expect_true('direction_rad' %in% names(windData))
  testthat::expect_true('vectorAverageHeading' %in% names(windData))
  testthat::expect_true('buoyWindDirDeadZoneQF' %in% names(windFlags))
  testthat::expect_true('buoyWindDirCalmWindQF' %in% names(windFlags))

  # Validate direction values are within valid range (0-360 degrees).
  validDir <- windData$direction[!is.na(windData$direction)]
  testthat::expect_true(length(validDir) > 0, info = "No valid direction values found")
  testthat::expect_true(all(validDir >= 0 & validDir < 360), 
                        info = "Direction values outside valid range [0, 360)")

  # Validate direction_rad is within valid range (0-2π radians).
  validDirRad <- windData$direction_rad[!is.na(windData$direction_rad)]
  testthat::expect_true(length(validDirRad) > 0, info = "No valid direction_rad values found")
  testthat::expect_true(all(validDirRad >= 0 & validDirRad <= 2 * pi), 
                        info = "Direction radians outside valid range [0, 2π]")

  # Validate consistency between direction and direction_rad (should be proportional).
  if (length(validDir) > 0 && length(validDirRad) > 0) {
    # For the same index, direction (degrees) should convert to direction_rad via π/180
    minCommonIdx <- min(length(validDir), length(validDirRad))
    dirDegToRad <- windData$direction[1:minCommonIdx] * pi / 180
    testthat::expect_true(all(abs(dirDegToRad - windData$direction_rad[1:minCommonIdx]) < 1e-5, na.rm = TRUE),
                          info = "Inconsistency between direction and direction_rad conversions")
  }

  # Validate vectorAverageHeading values and relationship to direction.
  validVecHeading <- windData$vectorAverageHeading[!is.na(windData$vectorAverageHeading)]
  testthat::expect_true(length(validVecHeading) > 0, info = "No valid vectorAverageHeading values found")
  testthat::expect_true(all(validVecHeading >= 0 & validVecHeading < 360),
                        info = "Vector average heading outside valid range [0, 360)")

  # Validate that data and flags have matching dimensions.
  testthat::expect_equal(nrow(windData), nrow(windFlags), 
                         info = "Data and flags have different number of rows")

  # Validate timestamp consistency if present in both datasets.
  if ('time' %in% names(windData) && 'time' %in% names(windFlags)) {
    testthat::expect_true(all(windData$time == windFlags$time, na.rm = TRUE),
                          info = "Timestamps do not match between data and flags")
  }

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})

# Additional tests for robustness and edge cases
test_that("Input validation and error handling", {

  source('../../flow.wind.buoy.compass.correction/wrap.wind.buoy.compass.correction.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()
  testDirOut <- file.path(workingDirPath, 'pfs/out')

  # Clean up output directory before starting.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test with non-existent input directory should fail gracefully.
  testDirInInvalid <- file.path(workingDirPath, 'pfs/non_existent_dir/2025/12/17/wind-buoy_INVALID')
  testthat::expect_error(
    wrap.wind.buoy.compass.correction(
      DirIn = testDirInInvalid,
      DirOutBase = testDirOut,
      SensWind = "rmyoung",
      SensCompass = "hmr3300",
      log = log
    ),
    info = "Function should error on non-existent input directory"
  )

  # Test with missing required sensor parameters.
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_threshold_select/2025/12/17/wind-buoy_BARC103100')
  if (dir.exists(testDirIn)) {
    testthat::expect_error(
      wrap.wind.buoy.compass.correction(
        DirIn = testDirIn,
        DirOutBase = testDirOut,
        log = log
        # Missing SensWind and SensCompass parameters
      ),
      info = "Function should error on missing sensor parameters"
    )
  }

  # Clean up.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})

# Test for duplicate readout time handling
test_that("Duplicated readout times in wind and compass data are handled", {

  source('../../flow.wind.buoy.compass.correction/wrap.wind.buoy.compass.correction.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()
  testDirInOrig <- file.path(workingDirPath, 'pfs/windBuoy_threshold_select/2025/12/17/wind-buoy_BARC103100')
  testDirInBase <- file.path(workingDirPath, 'pfs/windBuoy_threshold_select_dup')
  testDirIn <- file.path(testDirInBase, '2025/12/17/wind-buoy_BARC103100')
  testDirOut <- file.path(workingDirPath, 'pfs/out')

  # Clean up any leftovers from previous runs.
  if (dir.exists(testDirInBase)) {
    unlink(testDirInBase, recursive = TRUE)
  }
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Copy the base fixture, then inject a duplicated readout time into both the wind
  # and compass data files with distinguishing bogus values on the duplicate row.
  dir.create(file.path(testDirInBase, '2025/12/17'), recursive = TRUE)
  file.copy(testDirInOrig, file.path(testDirInBase, '2025/12/17'), recursive = TRUE)

  windDataFile <- file.path(testDirIn, 'rmyoung/CFGLOC110692/data/rmyoung_CFGLOC110692_2025-12-17.parquet')
  compassDataFile <- file.path(testDirIn, 'hmr3300/CFGLOC110692/data/hmr3300_CFGLOC110692_2025-12-17.parquet')

  windData <- NEONprocIS.base::def.read.parq(NameFile = windDataFile, log = log)
  compassData <- NEONprocIS.base::def.read.parq(NameFile = compassDataFile, log = log)

  numRowsWindOrig <- nrow(windData)

  # Bogus duplicate of the second row's readout_time, inserted after the original so the
  # original (first occurrence) is expected to be retained and the bogus row dropped.
  windDup <- windData[2, ]
  windDup$direction_calibrated <- 999
  windDataDup <- rbind(windData[1, ], windData[2, ], windDup, windData[3:nrow(windData), ])

  compassDup <- compassData[2, ]
  compassDup$direction <- 999
  compassDataDup <- rbind(compassData[1, ], compassData[2, ], compassDup, compassData[3:nrow(compassData), ])

  arrow::write_parquet(windDataDup, windDataFile)
  arrow::write_parquet(compassDataDup, compassDataFile)

  wrap.wind.buoy.compass.correction(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      SensWind = "rmyoung",
      SensCompass = "hmr3300",
      log = log
    )

  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)

  cfgDirs <- base::list.dirs(file.path(testDirOutPath, 'rmyoung'), recursive = FALSE, full.names = TRUE)
  cfgDirs <- cfgDirs[grepl('CFGLOC', basename(cfgDirs))]
  testthat::expect_true(length(cfgDirs) == 1)

  dirOutData <- file.path(cfgDirs[1], 'data')
  dataFiles <- base::list.files(dirOutData, full.names = TRUE)
  testthat::expect_true(length(dataFiles) == 1)

  outData <- try(NEONprocIS.base::def.read.parq(NameFile = dataFiles[1], log = log), silent = FALSE)
  testthat::expect_false(inherits(outData, 'try-error'), info = "Failed to read output data file")

  # No duplicated readout times should remain in the output.
  testthat::expect_false(any(duplicated(outData$readout_time)),
                        info = "Duplicate readout times were not removed from output")

  # Row count should match the number of unique readout times in the original (pre-duplicate) input.
  testthat::expect_equal(nrow(outData), numRowsWindOrig,
                         info = "Unexpected number of rows after deduplication of wind data")

  # The duplicated readout_time (floored to the 4-second interval) should retain the first
  # occurrence's value, not the bogus duplicate.
  dupTimeFloor <- as.POSIXct(floor(as.numeric(windData$readout_time[2]) / 4) * 4, origin = "1970-01-01", tz = "GMT")
  keptRow <- outData[outData$readout_time == dupTimeFloor, ]
  testthat::expect_equal(nrow(keptRow), 1, info = "Expected exactly one row for the deduplicated readout_time")
  testthat::expect_true(keptRow$direction_calibrated != 999,
                        info = "Bogus duplicate wind value was retained instead of the first occurrence")

  # Compass data duplicates should also be deduplicated, so the bogus value should not surface
  # in the merged compass_direction_raw output field.
  testthat::expect_true(all(outData$compass_direction_raw != 999, na.rm = TRUE),
                        info = "Bogus duplicate compass value was retained instead of the first occurrence")

  # Clean up.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  if (dir.exists(testDirInBase)) {
    unlink(testDirInBase, recursive = TRUE)
  }
})

# Test for optional DirSubCopy parameter
test_that("DirSubCopy parameter handling", {

  source('../../flow.wind.buoy.compass.correction/wrap.wind.buoy.compass.correction.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()
  testDirIn <- file.path(workingDirPath, 'pfs/windBuoy_threshold_select/2025/12/17/wind-buoy_BARC103100')
  testDirOut <- file.path(workingDirPath, 'pfs/out')

  # Clean up output directory.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  # Test with DirSubCopy parameter (optional subdirectories to copy).
  if (dir.exists(testDirIn)) {
    wrap.wind.buoy.compass.correction(
      DirIn = testDirIn,
      DirOutBase = testDirOut,
      SensWind = "rmyoung",
      SensCompass = "hmr3300",
      DirSubCopy = character(0),  # Empty vector for optional copy dirs
      log = log
    )

    infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
    testDirRepo <- infoDirIn$dirRepo
    testDirOutPath <- base::paste0(testDirOut, testDirRepo)

    testthat::expect_true(file.exists(testDirOutPath, recursive = TRUE),
                          info = "Output directory not created with empty DirSubCopy")
  }

  # Clean up.
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})