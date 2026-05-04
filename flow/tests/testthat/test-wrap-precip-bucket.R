##############################################################################################
#' @title Unit test for precip bucket aggregation module for NEON IS data processing.
#'
#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#'
#' @description
#' Run unit tests for wrap.precip.bucket.R.
#' The tests include positive and negative scenarios.
#' The positive tests verify that 1-minute and 30-minute output files are created with the
#' expected columns and quality-flag behavior for both tipping and throughfall sensor types.
#' The negative test verifies that an invalid input directory produces an error.
#'
#' Fixture data expected at:
#'   pfs/precipBucket_thresh_select/2025/08/30/
#'     precip-tipping_OSBS000060/metone370380/CFGLOC102736/
#'     precip-throughfall_OSBS001000/metone370380/CFGLOC102741/
#'     precip-throughfall_OSBS002000/metone370380/CFGLOC102768/
#'     ... (all containing data/, flags/, location/, threshold/, uncertainty_coef/)
#'
# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-03-04)
#     Initial creation
##############################################################################################
library(testthat)

context("\n       | Unit test of precip bucket aggregation module for NEON IS data processing \n")

test_that("Unit test of wrap.precip.bucket.R", {
  source('../../flow.precip.bucket/wrap.precip.bucket.R')

  DirFixture <- file.path(getwd(), 'pfs/precipBucket_thresh_select')
  DirOutBase <- tempfile(pattern = 'precip_bucket_test_out')
  
  # Clean up temp output after test completes
  on.exit(if (dir.exists(DirOutBase)) unlink(DirOutBase, recursive = TRUE), add = TRUE)
  
  # --------------------------------------------------------------------------------------------
  # Test 1: Happy path for tipping bucket - output directory structure is created
  # --------------------------------------------------------------------------------------------
  DirIn_tipping <- file.path(DirFixture, '2025/08/30/precip-tipping_OSBS000060/metone370380/CFGLOC102736')
  
  wrap.precip.bucket(
    DirIn       = DirIn_tipping,
    DirOutBase  = DirOutBase,
    DirSubCopy  = 'location'
  )
  
  DirOut_tipping <- file.path(DirOutBase, '2025/08/30/precip-tipping_OSBS000060/metone370380/CFGLOC102736')
  
  testthat::expect_true(dir.exists(file.path(DirOut_tipping, 'stats')))
  testthat::expect_true(dir.exists(file.path(DirOut_tipping, 'location')))
  
  # --------------------------------------------------------------------------------------------
  # Test 2: Both 1-min and 30-min output files are written
  # --------------------------------------------------------------------------------------------
  stats_files_tipping <- list.files(
    file.path(DirOut_tipping, 'stats'),
    pattern  = '_stats_(001|030)\\.parquet$',
    full.names = TRUE
  )
  
  testthat::expect_equal(length(stats_files_tipping), 2)
  
  file_001 <- stats_files_tipping[grepl('_stats_001\\.parquet$', stats_files_tipping)]
  file_030 <- stats_files_tipping[grepl('_stats_030\\.parquet$', stats_files_tipping)]
  testthat::expect_equal(length(file_001), 1)
  testthat::expect_equal(length(file_030), 1)
  
  # --------------------------------------------------------------------------------------------
  # Test 3: Output schema contains all expected columns
  # --------------------------------------------------------------------------------------------
  stats_001 <- NEONprocIS.base::def.read.parq.ds(
    fileIn   = file_001,
    VarTime  = 'startDateTime',
    RmvDupl  = TRUE,
    Df       = TRUE
  )
  
  expected_cols <- c(
    'startDateTime', 'endDateTime',
    'precipBulk', 'precipBulkExpUncert',
    'precipHeater0QM', 'precipHeater1QM', 'precipHeater2QM', 'precipHeater3QM',
    'validCalQF', 'suspectCalQF',
    'extremePrecipQF', 'finalQF'
  )
  
  testthat::expect_true(all(expected_cols %in% names(stats_001)))
  
  # --------------------------------------------------------------------------------------------
  # Test 4: Quality flag values are binary (0 or 1)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(stats_001$finalQF %in% c(0L, 1L)))
  testthat::expect_true(all(stats_001$extremePrecipQF %in% c(0L, 1L, -1L)))
  
  # --------------------------------------------------------------------------------------------
  # Test 5: Happy path for throughfall sensor - output files are created
  # --------------------------------------------------------------------------------------------
  DirIn_throughfall <- file.path(DirFixture, '2025/08/30/precip-throughfall_OSBS001000/metone370380/CFGLOC102741')
  
  wrap.precip.bucket(
    DirIn      = DirIn_throughfall,
    DirOutBase = DirOutBase,
    DirSubCopy = 'location'
  )
  
  DirOut_throughfall <- file.path(DirOutBase, '2025/08/30/precip-throughfall_OSBS001000/metone370380/CFGLOC102741')
  
  stats_files_tf <- list.files(
    file.path(DirOut_throughfall, 'stats'),
    pattern    = '_stats_(001|030)\\.parquet$',
    full.names = TRUE
  )
  
  testthat::expect_equal(length(stats_files_tf), 2)
  
  # --------------------------------------------------------------------------------------------
  # Test 6: Throughfall area conversion reduces per-tip precipitation vs raw tipping volume
  # --------------------------------------------------------------------------------------------
  raw_file <- list.files(file.path(DirIn_throughfall, 'data'), pattern = '\\.parquet$', full.names = TRUE)
  raw_data <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = raw_file[1],
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df      = TRUE
  )
  
  file_001_tf <- stats_files_tf[grepl('_stats_001\\.parquet$', stats_files_tf)]
  stats_001_tf <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = file_001_tf,
    VarTime = 'startDateTime',
    RmvDupl = TRUE,
    Df      = TRUE
  )
  
  raw_sum       <- sum(raw_data$precipitation, na.rm = TRUE)
  converted_sum <- sum(stats_001_tf$precipBulk, na.rm = TRUE)
  
  # The area conversion factor is A_b/A_t = 32429/251400 ≈ 0.129, so output < input when > 0
  if (!is.na(raw_sum) && raw_sum > 0) {
    testthat::expect_lt(converted_sum, raw_sum)
  } else {
    testthat::expect_gte(converted_sum, 0)
  }
  
  # --------------------------------------------------------------------------------------------
  # Test 7: DirSubCopy carries additional directories through to the output
  # --------------------------------------------------------------------------------------------
  DirIn_tf2 <- file.path(DirFixture, '2025/08/30/precip-throughfall_OSBS002000/metone370380/CFGLOC102768')
  
  wrap.precip.bucket(
    DirIn      = DirIn_tf2,
    DirOutBase = DirOutBase,
    DirSubCopy = c('location', 'threshold')
  )
  
  DirOut_tf2 <- file.path(DirOutBase, '2025/08/30/precip-throughfall_OSBS002000/metone370380/CFGLOC102768')
  
  testthat::expect_true(dir.exists(file.path(DirOut_tf2, 'stats')))
  testthat::expect_true(dir.exists(file.path(DirOut_tf2, 'location')))
  testthat::expect_true(dir.exists(file.path(DirOut_tf2, 'threshold')))
  
  # --------------------------------------------------------------------------------------------
  # Test 8: Invalid input directory (missing required sub-folders) produces an error
  # --------------------------------------------------------------------------------------------
  DirIn_bad <- file.path(DirFixture, '2025/08/30/precip-tipping_OSBS000060/metone370380/CFGLOC_DOES_NOT_EXIST')
  
  rpt <- try(
    wrap.precip.bucket(
      DirIn      = DirIn_bad,
      DirOutBase = DirOutBase
    ),
    silent = TRUE
  )
  
  testthat::expect_true('try-error' %in% class(rpt))
  # --------------------------------------------------------------------------------------------
  # Test 9: Modified data to have heater info and precip to count 
  # --------------------------------------------------------------------------------------------
  DirIn_tf3 <- file.path(DirFixture, '2025/08/30/modHeatPrecip_precip-throughfall_OSBS003000/metone370380/CFGLOC102795/')
  
  wrap.precip.bucket(
    DirIn      = DirIn_tf3,
    DirOutBase = DirOutBase,
    DirSubCopy = c('location', 'threshold')
  )
  
  DirOut_tf3 <- file.path(DirOutBase, '2025/08/30/modHeatPrecip_precip-throughfall_OSBS003000/metone370380/CFGLOC102795/')
  
  testthat::expect_true(dir.exists(file.path(DirOut_tf3, 'stats')))
  testthat::expect_true(dir.exists(file.path(DirOut_tf3, 'location')))
  testthat::expect_true(dir.exists(file.path(DirOut_tf3, 'threshold')))
  
  stats_files_tf <- list.files(
    file.path(DirOut_tf3, 'stats'),
    pattern    = '_stats_(001|030)\\.parquet$',
    full.names = TRUE
  )
  
  testthat::expect_equal(length(stats_files_tf), 2)
  
  #test that modified data passes similar tests as unmodified data
  
  raw_file <- list.files(file.path(DirIn_tf3, 'data'), pattern = '\\.parquet$', full.names = TRUE)
  raw_data <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = raw_file[1],
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df      = TRUE
  )
  
  file_001_tf <- stats_files_tf[grepl('_stats_001\\.parquet$', stats_files_tf)]
  stats_001_tf <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = file_001_tf,
    VarTime = 'startDateTime',
    RmvDupl = TRUE,
    Df      = TRUE
  )
  
  raw_sum       <- sum(raw_data$precipitation, na.rm = TRUE)
  converted_sum <- sum(stats_001_tf$precipBulk, na.rm = TRUE)
  
  # The area conversion factor is A_b/A_t = 32429/251400 ≈ 0.129, so output < input when > 0
  if (!is.na(raw_sum) && raw_sum > 0) {
    testthat::expect_lt(converted_sum, raw_sum)
  } else {
    testthat::expect_gte(converted_sum, 0)
  }
  
  #testing that it's approximately 100 (rounding can make it slightly off)
  qm_sums <- stats_001_tf$precipHeater0QM + stats_001_tf$precipHeater1QM +
    stats_001_tf$precipHeater2QM + stats_001_tf$precipHeater3QM
  testthat::expect_true(all(qm_sums >= 99 & qm_sums <= 101))
  
})


##############################################################################################
# Error-handling tests using bad fixtures created by:
#   testdata/create_precip_bucket_test_fixtures.R
#
# Each bad fixture is a copy of the tipping bucket datum with exactly one thing broken.
##############################################################################################
test_that("Integration test with missing thresholds", {
  source('../../flow.precip.bucket/wrap.precip.bucket.R')
  # Use actual test data paths
  DirIn <- file.path(getwd(), "pfs/precipBucket_thresh_select/2025/08/30/missingThresh_precip-throughfall_OSBS005000/metone370380/CFGLOC102849/")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Run the wrap function
  
  expect_error(wrap.precip.bucket(
    DirIn      = DirIn,
    DirOutBase = DirOutBase
  ))
  # Cleanup
  unlink(DirOutBase, recursive = TRUE)
  
})

test_that("Integration test with locations", {
  source('../../flow.precip.bucket/wrap.precip.bucket.R')
  # Use actual test data paths
  DirIn <- file.path(getwd(), "pfs/precipBucket_thresh_select/2025/08/30/missingLoc_precip-throughfall_OSBS004000/metone370380/CFGLOC102822/")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Run the wrap function
  expect_error(wrap.precip.bucket(
    DirIn      = DirIn,
    DirOutBase = DirOutBase
  ))
  # Cleanup
  unlink(DirOutBase, recursive = TRUE)
})

