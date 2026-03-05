##############################################################################################
#' @title Unit test for time shift module for NEON IS data processing.
#'
#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#'
#' @description
#' Run unit tests for wrap.time.shft.R.
#' Tests include:
#'   - Negative time shift: output file created, all output times are on the center date
#'   - Positive time shift: output file created
#'   - DirSubCopy carries additional directories to the output
#'   - Input validation errors for invalid TimeShft, TimeUnit, and TimeShftDir
#'   - Missing manifest causes early return (no output parquet written)
#'   - Incomplete pad (missing adjacent day) causes early return (no output parquet written)
#'
#' Fixture data expected at (relative to test working directory):
#'   pfs/timeShft/pluvio/2025/04/02/55221/       - 3-day pad + manifest + location/ subdir
#'   pfs/timeShft/nopad/pluvio/2025/04/02/55222/ - center day only + manifest (pad incomplete)
#'   pfs/timeShft/nomanifest/pluvio/2025/04/02/55223/ - 3-day data, no manifest
#'
#' Create fixtures by running (from flow/tests/testthat/):
#'   Rscript testdata/create_time_shft_fixtures.R
#'
# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-03-04)
#     Initial creation
##############################################################################################
library(testthat)

context("\n       | Unit test of time shift module for NEON IS data processing \n")
source('../../flow.time.shft/wrap.time.shft.R')

test_that("Unit test of wrap.time.shft.R", {
  
  DirFixture <- 'pfs/timeShft'
  DirOutBase <- tempfile(pattern = 'time_shft_test_out')
  
  # Clean up temp output after test completes
  on.exit(if (dir.exists(DirOutBase)) unlink(DirOutBase, recursive = TRUE), add = TRUE)
  
  # --------------------------------------------------------------------------------------------
  # Test 1: Negative shift - output data directory and parquet file are created
  # --------------------------------------------------------------------------------------------
  DirIn_main     <- file.path(getwd(), DirFixture, 'pluvio/2025/04/02/55221')
  DirOutBase_neg <- file.path(DirOutBase, 'neg')
  
  wrap.time.shft(
    DirIn       = DirIn_main,
    DirOutBase  = DirOutBase_neg,
    TimeShft    = 5,
    TimeUnit    = 'mins',
    TimeShftDir = 'Neg'
  )
  
  DirOut_main <- file.path(DirOutBase_neg, 'pluvio/2025/04/02/55221')
  testthat::expect_true(dir.exists(file.path(DirOut_main, 'data')))
  
  file_out_neg <- list.files(
    file.path(DirOut_main, 'data'),
    pattern    = '\\.parquet$',
    full.names = TRUE
  )
  testthat::expect_equal(length(file_out_neg), 1)
  
  # --------------------------------------------------------------------------------------------
  # Test 2: Negative shift - output file name corresponds to the center date
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(grepl('2025-04-02', basename(file_out_neg)))
  
  # --------------------------------------------------------------------------------------------
  # Test 3: Negative shift - all output readout_time values fall on the center date
  # --------------------------------------------------------------------------------------------
  out_data_neg <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = file_out_neg,
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df      = TRUE
  )
  
  testthat::expect_true(
    all(as.Date(out_data_neg$readout_time, tz = 'UTC') == as.Date('2025-04-02'))
  )
  
  # --------------------------------------------------------------------------------------------
  # Test 4: Positive shift - output file exists and contains the center date
  # --------------------------------------------------------------------------------------------
  DirOutBase_pos <- file.path(DirOutBase, 'pos')
  
  wrap.time.shft(
    DirIn       = DirIn_main,
    DirOutBase  = DirOutBase_pos,
    TimeShft    = 5,
    TimeUnit    = 'mins',
    TimeShftDir = 'Pos'
  )
  
  file_out_pos <- list.files(
    file.path(DirOutBase_pos, 'pluvio/2025/04/02/55221/data'),
    pattern    = '2025-04-02\\.parquet$',
    full.names = TRUE
  )
  testthat::expect_equal(length(file_out_pos), 1)
  
  # --------------------------------------------------------------------------------------------
  # Test 5: DirSubCopy carries additional directories to the output
  # --------------------------------------------------------------------------------------------
  DirOutBase_copy <- file.path(DirOutBase, 'copy')
  
  wrap.time.shft(
    DirIn       = DirIn_main,
    DirOutBase  = DirOutBase_copy,
    TimeShft    = 5,
    TimeUnit    = 'mins',
    TimeShftDir = 'Neg',
    DirSubCopy  = 'location'
  )
  
  testthat::expect_true(dir.exists(file.path(DirOutBase_copy, 'pluvio/2025/04/02/55221/location')))
  
  # --------------------------------------------------------------------------------------------
  # Test 6: Invalid TimeShft (non-numeric) produces an error
  # --------------------------------------------------------------------------------------------
  rpt <- try(
    wrap.time.shft(
      DirIn       = DirIn_main,
      DirOutBase  = DirOutBase,
      TimeShft    = 'five',
      TimeUnit    = 'mins',
      TimeShftDir = 'Neg'
    ),
    silent = TRUE
  )
  testthat::expect_true('try-error' %in% class(rpt))

  # --------------------------------------------------------------------------------------------
  # Test 7: Invalid TimeUnit produces an error
  # --------------------------------------------------------------------------------------------
  rpt <- try(
    wrap.time.shft(
      DirIn       = DirIn_main,
      DirOutBase  = DirOutBase,
      TimeShft    = 5,
      TimeUnit    = 'minutes',   # must be "mins"
      TimeShftDir = 'Neg'
    ),
    silent = TRUE
  )
  testthat::expect_true('try-error' %in% class(rpt))
  
  # --------------------------------------------------------------------------------------------
  # Test 8: Invalid TimeShftDir produces an error
  # --------------------------------------------------------------------------------------------
  rpt <- try(
    wrap.time.shft(
      DirIn       = DirIn_main,
      DirOutBase  = DirOutBase,
      TimeShft    = 5,
      TimeUnit    = 'mins',
      TimeShftDir = 'backward'   # must be "Pos" or "Neg"
    ),
    silent = TRUE
  )
  testthat::expect_true('try-error' %in% class(rpt))
  
  # --------------------------------------------------------------------------------------------
  # Test 9: Missing manifest file - function returns early, no output parquet written
  # --------------------------------------------------------------------------------------------
  DirIn_nomanifest  <- file.path(getwd(), DirFixture, 'nomanifest/pluvio/2025/04/02/55223')
  DirOut_nomanifest <- file.path(DirOutBase, 'nomanifest/pluvio/2025/04/02/55223')
  
  wrap.time.shft(
    DirIn       = DirIn_nomanifest,
    DirOutBase  = DirOutBase,
    TimeShft    = 5,
    TimeUnit    = 'mins',
    TimeShftDir = 'Neg'
  )
  
  testthat::expect_equal(
    length(list.files(file.path(DirOut_nomanifest, 'data'), pattern = '\\.parquet$')),
    0
  )
  
  # --------------------------------------------------------------------------------------------
  # Test 10: Incomplete pad (missing day+1 for Neg shift) - returns early, no output written
  # --------------------------------------------------------------------------------------------
  DirIn_nopad  <- file.path(getwd(), DirFixture, 'nopad/pluvio/2025/04/02/55222')
  DirOut_nopad <- file.path(DirOutBase, 'nopad/pluvio/2025/04/02/55222')
  
  wrap.time.shft(
    DirIn       = DirIn_nopad,
    DirOutBase  = DirOutBase,
    TimeShft    = 5,
    TimeUnit    = 'mins',
    TimeShftDir = 'Neg'
  )
  
  testthat::expect_equal(
    length(list.files(file.path(DirOut_nopad, 'data'), pattern = '\\.parquet$')),
    0
  )
  
})
