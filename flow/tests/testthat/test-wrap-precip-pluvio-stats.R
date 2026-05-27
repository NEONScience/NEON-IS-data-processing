##############################################################################################
#' @title Unit tests for wrap.precip.pluvio.stats

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Run unit tests for wrap.precip.pluvio.stats.R.
#' The tests include positive and negative scenarios.
#' Positive tests verify that 1-minute and 30-minute output files are created with the
#' expected columns, that uncertainty is applied correctly, that 30-minute aggregation
#' sums precipitation across 1-minute intervals, and that finalQF reflects the component flags.
#' The negative test verifies that an invalid input directory produces an error.
#'
#' Fixture data expected at:
#'   pfs/precipPluvioStats/2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245/
#'     data/            - pluvio_CFGLOC105245_2025-03-31.parquet (60 rows, 1 hr)
#'     flags/           - pluvio_CFGLOC105245_2025-03-31_flagsCal.parquet
#'                          (validCalQF, suspectCalQF)
#'                        pluvio_CFGLOC105245_2025-03-31_flagsPlausibility.parquet
#'                          (nullQF, rangeQF, gapQF, sensorErrorQF, heaterErrorQF)
#'     uncertainty_coef/ - pluvio_CFGLOC105245_2025-03-31_uncertaintyCoef.json
#'                         (U_CVALA1 = 0.02)

# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-05-08)
#     Initial creation
##############################################################################################
library(testthat)

context("\n       | Unit test of wrap.precip.pluvio.stats for NEON IS data processing \n")

test_that("Unit test of wrap.precip.pluvio.stats.R", {
  source('../../flow.precip.pluvio.stats/wrap.precip.pluvio.stats.R')

  DirFixture <- file.path(getwd(), 'pfs/precipPluvioStats')
  DirOutBase <- tempfile(pattern = 'precip_pluvio_stats_test_out')

  # Clean up temp output after test completes
  on.exit(if (dir.exists(DirOutBase)) unlink(DirOutBase, recursive = TRUE), add = TRUE)

  DirIn <- file.path(DirFixture, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')

  # --------------------------------------------------------------------------------------------
  # Test 1: Happy path - output stats directory is created
  # --------------------------------------------------------------------------------------------
  wrap.precip.pluvio.stats(
    DirIn      = DirIn,
    DirOutBase = DirOutBase,
    DirSubCopy = 'location'
  )

  DirOut <- file.path(DirOutBase, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')
  testthat::expect_true(dir.exists(file.path(DirOut, 'stats')))

  # --------------------------------------------------------------------------------------------
  # Test 2: Both 1-min and 30-min output files are written
  # --------------------------------------------------------------------------------------------
  stats_files <- list.files(
    file.path(DirOut, 'stats'),
    pattern    = '_stats_(001|030)\\.parquet$',
    full.names = TRUE
  )

  testthat::expect_equal(length(stats_files), 2)

  file_001 <- stats_files[grepl('_stats_001\\.parquet$', stats_files)]
  file_030 <- stats_files[grepl('_stats_030\\.parquet$', stats_files)]
  testthat::expect_equal(length(file_001), 1)
  testthat::expect_equal(length(file_030), 1)

  # --------------------------------------------------------------------------------------------
  # Test 3: 1-min output schema contains all expected columns
  # --------------------------------------------------------------------------------------------
  stats_001 <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = file_001,
    VarTime = 'startDateTime',
    RmvDupl = TRUE,
    Df      = TRUE
  )


  # Update expected columns: remove gapQF, add insuffDataQF for 30-min
  expected_cols <- c(
    'startDateTime', 'endDateTime',
    'precipBulk', 'precipBulkExpUncert', 'precipNumPts',
    'nullQF', 'extremePrecipQF',
    'heaterErrorQF', 'sensorErrorQF',
    'validCalQF', 'suspectCalQF', 'finalQF'
  )

  testthat::expect_true(all(expected_cols %in% names(stats_001)))

  # --------------------------------------------------------------------------------------------
  # Test 4: 30-min output schema contains all expected columns including insuffDataQF
  # --------------------------------------------------------------------------------------------
  stats_030 <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = file_030,
    VarTime = 'startDateTime',
    RmvDupl = TRUE,
    Df      = TRUE
  )


  expected_cols_030 <- c(expected_cols[1:5], 'insuffDataQF', expected_cols[6:length(expected_cols)])
  testthat::expect_true(all(expected_cols_030 %in% names(stats_030)))

  # insuffDataQF must not appear in 1-min output
  testthat::expect_false('insuffDataQF' %in% names(stats_001))

  # --------------------------------------------------------------------------------------------
  # Test 5: 30-min output has fewer rows than 1-min output
  # --------------------------------------------------------------------------------------------
  testthat::expect_lt(nrow(stats_030), nrow(stats_001))

  # --------------------------------------------------------------------------------------------
  # Test 6: precipBulk values are non-negative
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_001$precipBulk >= 0, na.rm = TRUE))
  testthat::expect_true(all(stats_030$precipBulk >= 0, na.rm = TRUE))

  # --------------------------------------------------------------------------------------------
  # Test 7: precipBulkExpUncert meets the minimum 0.1 mm manufacturer accuracy spec
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_001$precipBulkExpUncert >= 0.1, na.rm = TRUE))

  # --------------------------------------------------------------------------------------------
  # Test 8: 30-min precipBulk is the sum of the corresponding 1-min precipBulk values
  # --------------------------------------------------------------------------------------------
  # First 30-min interval covers rows 1-30 of 1-min stats
  first_30_sum <- sum(stats_001$precipBulk[1:30], na.rm = TRUE)
  testthat::expect_equal(stats_030$precipBulk[1], first_30_sum, tolerance = 1e-8)

  # --------------------------------------------------------------------------------------------
  # Test 9: finalQF is 0 when all component flags are 0 (fixture has no errors)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_001$finalQF == 0L, na.rm = TRUE))
  testthat::expect_true(all(stats_030$finalQF == 0L, na.rm = TRUE))

  # --------------------------------------------------------------------------------------------
  # Test 10: Quality flag values are within the expected set {-1, 0, 1}
  # --------------------------------------------------------------------------------------------

  qf_cols_001 <- c('nullQF', 'extremePrecipQF', 'heaterErrorQF',
                   'sensorErrorQF', 'finalQF')
  qf_cols_030 <- c(qf_cols_001, 'insuffDataQF')

  for (col in qf_cols_001) {
    testthat::expect_true(
      all(stats_001[[col]] %in% c(-1L, 0L, 1L), na.rm = TRUE),
      info = paste0('Column ', col, ' in 1-min output has unexpected values')
    )
  }
  for (col in qf_cols_030) {
    testthat::expect_true(
      all(stats_030[[col]] %in% c(-1L, 0L, 1L), na.rm = TRUE),
      info = paste0('Column ', col, ' in 30-min output has unexpected values')
    )
  }

  # --------------------------------------------------------------------------------------------
  # Test 10b: insuffDataQF is 0 when precipNumPts == 30 (fixture has complete data)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_030$insuffDataQF == 0L, na.rm = TRUE))

  # --------------------------------------------------------------------------------------------
  # Test 11: precipNumPts equals 1 for each non-NA 1-min row and sums to 30 per 30-min interval
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_001$precipNumPts %in% c(0L, 1L)))
  testthat::expect_equal(stats_030$precipNumPts[1], 30L)

  # --------------------------------------------------------------------------------------------
  # Test 12: DirSubCopy carries additional directories through to output
  # --------------------------------------------------------------------------------------------
  DirOutBase2 <- tempfile(pattern = 'precip_pluvio_stats_test_out2')
  on.exit(if (dir.exists(DirOutBase2)) unlink(DirOutBase2, recursive = TRUE), add = TRUE)

  wrap.precip.pluvio.stats(
    DirIn      = DirIn,
    DirOutBase = DirOutBase2,
    DirSubCopy = c('flags', 'uncertainty_coef')
  )

  DirOut2 <- file.path(DirOutBase2, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')
  testthat::expect_true(dir.exists(file.path(DirOut2, 'stats')))
  testthat::expect_true(dir.exists(file.path(DirOut2, 'flags')))
  testthat::expect_true(dir.exists(file.path(DirOut2, 'uncertainty_coef')))

  # --------------------------------------------------------------------------------------------
  # Test 13: Invalid input directory produces an error
  # --------------------------------------------------------------------------------------------
  DirIn_bad <- file.path(DirFixture, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC_DOES_NOT_EXIST')

  rpt <- try(
    wrap.precip.pluvio.stats(
      DirIn      = DirIn_bad,
      DirOutBase = DirOutBase
    ),
    silent = TRUE
  )

  testthat::expect_true('try-error' %in% class(rpt))
})
