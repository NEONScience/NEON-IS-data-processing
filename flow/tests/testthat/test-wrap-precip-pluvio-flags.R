##############################################################################################
#' @title Unit tests for wrap.precip.pluvio.flags

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Run unit tests for wrap.precip.pluvio.flags.R.
#' The tests include positive and negative scenarios.
#' Positive tests verify that the output flags directory is created, that the
#' flagsPlausibility output file contains heaterErrorQF and sensorErrorQF columns,
#' and that the bitwise sensor-status and inlet-temperature logic produces the
#' expected flag values.
#' The negative test verifies that an invalid input directory produces an error.
#'
#' Fixture data expected at:
#'   pfs/precipPluvioFlags/2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245/
#'     data/  - pluvio_CFGLOC105245_2025-03-31.parquet
#'     flags/ - pluvio_CFGLOC105245_2025-03-31_flagsPlausibility.parquet
#'
#' Row key for fixture data:
#'   Row 1:  sensorStatus=0,    inletTemp=5.0  -> sensorErrorQF=0,  heaterErrorQF=0
#'   Row 2:  sensorStatus=NA,   inletTemp=NA   -> sensorErrorQF=-1, heaterErrorQF=-1
#'   Row 3:  sensorStatus=64,   inletTemp=-2.0 -> sensorErrorQF=1,  heaterErrorQF=1
#'   Row 4:  sensorStatus=128,  inletTemp=5.0  -> sensorErrorQF=1,  heaterErrorQF=0
#'   Row 5:  sensorStatus=256,  inletTemp=5.0  -> sensorErrorQF=1,  heaterErrorQF=0
#'   Row 6:  sensorStatus=512,  inletTemp=5.0  -> sensorErrorQF=1,  heaterErrorQF=0
#'   Row 7:  sensorStatus=1024, inletTemp=5.0  -> sensorErrorQF=1,  heaterErrorQF=0
#'   Row 8:  sensorStatus=0,    inletTemp=-2.0 -> sensorErrorQF=0,  heaterErrorQF=1
#'   Row 9:  sensorStatus=0,    inletTemp=5.0  -> sensorErrorQF=0,  heaterErrorQF=0
#'   Row 10: sensorStatus=0,    inletTemp=5.0  -> sensorErrorQF=0,  heaterErrorQF=0

# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-05-08)
#     Initial creation
##############################################################################################
library(testthat)

context("\n       | Unit test of wrap.precip.pluvio.flags for NEON IS data processing \n")

test_that("Unit test of wrap.precip.pluvio.flags.R", {
  source('../../flow.precip.pluvio.flags/wrap.precip.pluvio.flags.R')

  DirFixture <- file.path(getwd(), 'pfs/precipPluvioFlags')
  DirOutBase <- tempfile(pattern = 'precip_pluvio_flags_test_out')

  # Clean up temp output after test completes
  on.exit(if (dir.exists(DirOutBase)) unlink(DirOutBase, recursive = TRUE), add = TRUE)

  DirIn <- file.path(DirFixture, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')

  # --------------------------------------------------------------------------------------------
  # Test 1: Happy path - output flags directory is created
  # --------------------------------------------------------------------------------------------
  wrap.precip.pluvio.flags(
    DirIn      = DirIn,
    DirOutBase = DirOutBase
  )

  DirOut <- file.path(DirOutBase, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')
  testthat::expect_true(dir.exists(file.path(DirOut, 'flags')))

  # --------------------------------------------------------------------------------------------
  # Test 2: Output flagsPlausibility file is written
  # --------------------------------------------------------------------------------------------
  out_flags <- list.files(file.path(DirOut, 'flags'), pattern = 'flagsPlausibility\\.parquet$',
                          full.names = TRUE)
  testthat::expect_equal(length(out_flags), 1)

  # --------------------------------------------------------------------------------------------
  # Test 3: Output file contains heaterErrorQF and sensorErrorQF columns
  # --------------------------------------------------------------------------------------------
  qfOut <- NEONprocIS.base::def.read.parq.ds(
    fileIn  = out_flags,
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df      = TRUE
  )

  testthat::expect_true('heaterErrorQF' %in% names(qfOut))
  testthat::expect_true('sensorErrorQF' %in% names(qfOut))

  # --------------------------------------------------------------------------------------------
  # Test 4: sensorErrorQF is -1 when sensorStatus is NA (row 2)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[2], -1L)

  # --------------------------------------------------------------------------------------------
  # Test 5: sensorErrorQF is 0 when sensorStatus is 0 (row 1)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[1], 0L)

  # --------------------------------------------------------------------------------------------
  # Test 6: sensorErrorQF is 1 when bit 6 (unstable) is set (row 3, sensorStatus=64)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[3], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 7: sensorErrorQF is 1 when bit 7 (defective) is set (row 4, sensorStatus=128)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[4], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 8: sensorErrorQF is 1 when bit 8 (weight < min) is set (row 5, sensorStatus=256)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[5], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 9: sensorErrorQF is 1 when bit 9 (weight > max) is set (row 6, sensorStatus=512)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[6], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 10: sensorErrorQF is 1 when bit 10 (no calibration) is set (row 7, sensorStatus=1024)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$sensorErrorQF[7], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 11: heaterErrorQF is -1 when inletTemp is NA (row 2)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$heaterErrorQF[2], -1L)

  # --------------------------------------------------------------------------------------------
  # Test 12: heaterErrorQF is 0 when inletTemp > 0 (row 1, inletTemp=5.0)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$heaterErrorQF[1], 0L)

  # --------------------------------------------------------------------------------------------
  # Test 13: heaterErrorQF is 1 when inletTemp < 0 and cell is near freezing (row 3, inletTemp=-2)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$heaterErrorQF[3], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 14: heaterErrorQF is 1 when sensorStatus=0 but inletTemp<0 (row 8, inletTemp=-2)
  # --------------------------------------------------------------------------------------------
  testthat::expect_equal(qfOut$heaterErrorQF[8], 1L)

  # --------------------------------------------------------------------------------------------
  # Test 15: Original flag columns are preserved in output (nullQF, rangeQF, gapQF)
  # --------------------------------------------------------------------------------------------
  testthat::expect_true('nullQF' %in% names(qfOut))
  testthat::expect_true('rangeQF' %in% names(qfOut))
  testthat::expect_true('gapQF' %in% names(qfOut))

  # --------------------------------------------------------------------------------------------
  # Test 16: All flag values are in the expected set {-1, 0, 1}
  # --------------------------------------------------------------------------------------------
  testthat::expect_true(all(qfOut$sensorErrorQF %in% c(-1L, 0L, 1L)))
  testthat::expect_true(all(qfOut$heaterErrorQF %in% c(-1L, 0L, 1L)))

  # --------------------------------------------------------------------------------------------
  # Test 17: DirSubCopy carries additional directories through to output
  # --------------------------------------------------------------------------------------------
  DirOutBase2 <- tempfile(pattern = 'precip_pluvio_flags_test_out2')
  on.exit(if (dir.exists(DirOutBase2)) unlink(DirOutBase2, recursive = TRUE), add = TRUE)

  wrap.precip.pluvio.flags(
    DirIn      = DirIn,
    DirOutBase = DirOutBase2,
    DirSubCopy = 'data'
  )

  DirOut2 <- file.path(DirOutBase2, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC105245')
  testthat::expect_true(dir.exists(file.path(DirOut2, 'flags')))
  testthat::expect_true(dir.exists(file.path(DirOut2, 'data')))

  # --------------------------------------------------------------------------------------------
  # Test 18: Invalid input directory produces an error
  # --------------------------------------------------------------------------------------------
  DirIn_bad <- file.path(DirFixture, '2025/03/31/precip-weighing_SITE001/pluvio/CFGLOC_DOES_NOT_EXIST')

  rpt <- try(
    wrap.precip.pluvio.flags(
      DirIn      = DirIn_bad,
      DirOutBase = DirOutBase
    ),
    silent = TRUE
  )

  testthat::expect_true('try-error' %in% class(rpt))
})
