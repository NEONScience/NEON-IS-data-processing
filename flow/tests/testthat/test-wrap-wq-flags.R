##############################################################################################
#' @title Unit test for Wrapper for water quality low conductivity and buoy profiling flags
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.wq.flags.R. Tests basic file/directory input/output,
#' schema argument passing, and successful output of quality flags for low conductivity
#' and buoy profiling conditions.
#'
#' @param DirIn Character value. The input path with sensor data, flags, and conductivity reference data.
#'
#' @param DirOutBase Character value. The base file path for the output data.
#'
#' @param BuoySite Logical value. Indicates whether the site is a buoy site.
#'
#' @param SchmPath (optional) The full path to the avro schemas for the output flags file.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders to copy.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#'
#' changelog and author contributions
#   Nora Catolico (2026-11-13)
#'     Original Creation
#
##############################################################################################

context("\n                       Unit test of wrap.wq.flags.R\n")

test_that("Unit test of wrap.wq.flags.R - Basic output structure (Non-buoy site)", {

  source('../../flow.wq.flags/wrap.wq.flags.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  # Set up working directories and schemas
  workingDirPath <- getwd()

  testDirIn <- file.path(workingDirPath,
    'pfs/waterQuality_flags_specific_group',
    '2025/09/04/water-quality_BARC103100'
  )
  testDirOut <- file.path(workingDirPath, 'pfs/water_quality_flags_out')
  SchmPath <- file.path(workingDirPath,
    'pfs/waterQuality_avro_schemas'
  )

  # --------------------------------------------------------------------------------------------
  # Test 1: Basic wrapper execution for non-buoy site
  # --------------------------------------------------------------------------------------------
  wrap.wq.flags(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    BuoySite = TRUE,
    DirSubCopy = NULL,
    SchmPath = SchmPath,
    log = log
  )

  # --------------------------------------------------------------------------------------------
  # Test 2: Verify output directory structure is created
  # --------------------------------------------------------------------------------------------
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  dirOutBase <- base::paste0(testDirOut, testDirRepo)

  dirOutExo2Flags <- file.path(dirOutBase, 'exo2/CFGLOC123456/flags')
  dirOutTurbFlags <- file.path(dirOutBase, 'exo2turbidity/CFGLOC123456/flags')

  testthat::expect_true(dir.exists(dirOutExo2Flags))
  testthat::expect_true(dir.exists(dirOutTurbFlags))

  # --------------------------------------------------------------------------------------------
  # Test 3: Verify exo2 flags output file exists and contains expected columns
  # --------------------------------------------------------------------------------------------
  exo2FlagFiles <- list.files(dirOutExo2Flags, pattern = '_flagsPlausibility', full.names = TRUE)
  testthat::expect_equal(length(exo2FlagFiles), 1)

  exo2FlagsOut <- try(
    NEONprocIS.base::def.read.parq(NameFile = exo2FlagFiles[1], log = log),
    silent = FALSE
  )
  testthat::expect_false(inherits(exo2FlagsOut, 'try-error'))
  testthat::expect_true(length(exo2FlagsOut) != 0)
  testthat::expect_true(nrow(exo2FlagsOut) > 0)

  # Verify required columns exist
  requiredFlagCols <- c('readout_time', 'exo2ZeroConductivityQF')
  testthat::expect_true(all(requiredFlagCols %in% names(exo2FlagsOut)))

  # --------------------------------------------------------------------------------------------
  # Test 4: Verify turbidity flags output file exists and contains expected columns
  # --------------------------------------------------------------------------------------------
  turbFlagFiles <- list.files(dirOutTurbFlags, pattern = '_flagsPlausibility', full.names = TRUE)
  testthat::expect_equal(length(turbFlagFiles), 1)

  turbFlagsOut <- try(
    NEONprocIS.base::def.read.parq(NameFile = turbFlagFiles[1], log = log),
    silent = FALSE
  )
  testthat::expect_false(inherits(turbFlagsOut, 'try-error'))
  testthat::expect_true(length(turbFlagsOut) != 0)
  testthat::expect_true(nrow(turbFlagsOut) > 0)

  # Verify required columns exist
  requiredFlagCols <- c('readout_time', 'turbidityZeroConductivityQF')
  testthat::expect_true(all(requiredFlagCols %in% names(turbFlagsOut)))

  # --------------------------------------------------------------------------------------------
  # Test 5: Verify conductivity flag values are valid (should be -1, 0, or 1)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(exo2FlagsOut$exo2ZeroConductivityQF %in% c(-1, 0, 1), na.rm = TRUE))
  testthat::expect_true(all(turbFlagsOut$turbidityZeroConductivityQF %in% c(-1, 0, 1), na.rm = TRUE))

  # --------------------------------------------------------------------------------------------
  # Test 6: Verify conductivity flag logic (conductance <= 1 should get flag 1, > 1 should get 0)
  # --------------------------------------------------------------------------------------------
  # From our test data: specificConductance = c(2.5, 0.8, 1.2, 3.0)
  # Expected flags:     c(0, 1, 0, 0)
  testthat::expect_equal(exo2FlagsOut$exo2ZeroConductivityQF[1], 0)  # 2.5 > 1
  testthat::expect_equal(exo2FlagsOut$exo2ZeroConductivityQF[2], 1)  # 0.8 <= 1
  testthat::expect_equal(exo2FlagsOut$exo2ZeroConductivityQF[3], 0)  # 1.2 > 1
  testthat::expect_equal(exo2FlagsOut$exo2ZeroConductivityQF[4], 0)  # 3.0 > 1

  # Cleanup
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
})

# --------------------------------------------------------------------------------------------
# Test for buoy site with buoy profiling flags
# --------------------------------------------------------------------------------------------
test_that("Unit test of wrap.wq.flags.R - Buoy site with profiling flags", {

  source('../../flow.wq.flags/wrap.wq.flags.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  workingDirPath <- getwd()

  testDirIn <- file.path(workingDirPath,
    'pfs/waterQuality_flags_specific_group_buoy',
    '2025/09/04/water-quality_POSE102100'
  )
  testDirOut <- file.path(workingDirPath, 'pfs/water_quality_flags_buoy_out')

  # Create test input directory structure
  dir.create(file.path(testDirIn, 'exo2conductivity/CFGLOC123456/data'), recursive = TRUE)
  dir.create(file.path(testDirIn, 'exo2/CFGLOC123456/data'), recursive = TRUE)
  dir.create(file.path(testDirIn, 'exo2/CFGLOC123456/flags'), recursive = TRUE)

  # Create sample conductivity data
  condData <- data.frame(
    readout_time = as.POSIXct(c('2025-09-04 12:00:00', '2025-09-04 12:00:30', 
                                  '2025-09-04 12:01:00', '2025-09-04 12:01:30'), tz = 'UTC'),
    specificConductance = c(2.5, 0.8, 1.2, 3.0),
    zeroConductivityQF = as.integer(NA)
  )
  condFile <- file.path(testDirIn, 'exo2conductivity/CFGLOC123456/data/exo2conductivity_CFGLOC123456_2025-09-04_data_001.parquet')
  NEONprocIS.base::def.wrte.parq(data = condData, NameFile = condFile)

  # Create sample exo2 data with varying depth (to trigger profiling flags)
  exo2Data <- data.frame(
    readout_time = as.POSIXct(c('2025-09-04 12:00:00', '2025-09-04 12:00:30', 
                                 '2025-09-04 12:01:00', '2025-09-04 12:01:30'), tz = 'UTC'),
    sensorDepth = c(0.1, 0.7, 0.2, 0.25),  # Depth change > 0.5 at index 2
    temp = c(15.2, 15.3, 15.4, 15.3)
  )
  exo2File <- file.path(testDirIn, 'exo2/CFGLOC123456/data/exo2_CFGLOC123456_2025-09-04_data_001.parquet')
  NEONprocIS.base::def.wrte.parq(data = exo2Data, NameFile = exo2File)

  # Create sample plausibility flags file for exo2
  exo2Flags <- data.frame(
    readout_time = as.POSIXct(c('2025-09-04 12:00:00', '2025-09-04 12:00:30', 
                                 '2025-09-04 12:01:00', '2025-09-04 12:01:30'), tz = 'UTC'),
    tempPlausibilityQF = c(0, 0, 0, 0),
    Step = c(0, 1, 1, 0)  # Step = 1 at indices 2 and 3
  )
  exo2FlagsFile <- file.path(testDirIn, 'exo2/CFGLOC123456/flags/exo2_CFGLOC123456_2025-09-04_flagsPlausibility.parquet')
  NEONprocIS.base::def.wrte.parq(data = exo2Flags, NameFile = exo2FlagsFile)

  # --------------------------------------------------------------------------------------------
  # Test 1: Basic wrapper execution for buoy site
  # --------------------------------------------------------------------------------------------
  wrap.wq.flags(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    BuoySite = TRUE,
    DirSubCopy = NULL,
    SchmPath = NULL,
    log = log
  )

  # --------------------------------------------------------------------------------------------
  # Test 2: Verify buoyProfilingQF column is created
  # --------------------------------------------------------------------------------------------
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  dirOutBase <- base::paste0(testDirOut, testDirRepo)
  dirOutExo2Flags <- file.path(dirOutBase, 'exo2/CFGLOC123456/flags')

  exo2FlagFiles <- list.files(dirOutExo2Flags, pattern = '_flagsPlausibility_', full.names = TRUE)
  testthat::expect_equal(length(exo2FlagFiles), 1)

  exo2FlagsOut <- try(
    NEONprocIS.base::def.read.parq(NameFile = exo2FlagFiles[1], log = log),
    silent = FALSE
  )
  testthat::expect_false(inherits(exo2FlagsOut, 'try-error'))

  # Verify buoyProfilingQF column exists
  testthat::expect_true('buoyProfilingQF' %in% names(exo2FlagsOut))

  # --------------------------------------------------------------------------------------------
  # Test 3: Verify buoyProfilingQF flag values are valid (should be -1, 0, or 1)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(exo2FlagsOut$buoyProfilingQF %in% c(-1, 0, 1)))

  # --------------------------------------------------------------------------------------------
  # Test 4: Verify buoy profiling logic (depth change >= 0.5 should get flag 1)
  # --------------------------------------------------------------------------------------------
  # Depth changes: c(NA, 0.6, -0.5, 0.05)
  # Expected buoyProfilingQF: c(-1, 1, 1, 0)
  testthat::expect_equal(exo2FlagsOut$buoyProfilingQF[1], -1)  # NA depth change
  testthat::expect_equal(exo2FlagsOut$buoyProfilingQF[2], 1)   # |0.6| >= 0.5
  testthat::expect_equal(exo2FlagsOut$buoyProfilingQF[3], 1)   # |-0.5| >= 0.5
  testthat::expect_equal(exo2FlagsOut$buoyProfilingQF[4], 0)   # |0.05| < 0.5

  # --------------------------------------------------------------------------------------------
  # Test 5: Verify Step flag is set to 0 when buoyProfilingQF = 1
  # --------------------------------------------------------------------------------------------
  # Original Step values: c(0, 1, 1, 0)
  # buoyProfilingQF values: c(-1, 1, 1, 0)
  # Expected Step after processing: c(0, 0, 0, 0)
  testthat::expect_equal(exo2FlagsOut$Step[1], 0)  # No change (buoyProfilingQF != 1)
  testthat::expect_equal(exo2FlagsOut$Step[2], 0)  # Set to 0 (buoyProfilingQF == 1 & Step == 1)
  testthat::expect_equal(exo2FlagsOut$Step[3], 0)  # Set to 0 (buoyProfilingQF == 1 & Step == 1)
  testthat::expect_equal(exo2FlagsOut$Step[4], 0)  # No change (buoyProfilingQF != 1)

  # Cleanup
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  if (dir.exists(testDirIn)) {
    unlink(testDirIn, recursive = TRUE)
  }
  
})
