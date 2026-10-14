##############################################################################################
#' @title Unit test for Wrapper for Continuous Discharge Prediction
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.discharge.predict.R. Verifies pass-through
#' output when a minimal CSD_15_min input parquet is present.
#'
#' @param DirIn Character value. The input path to the discharge datum.
#'
#' @param DirOutBase Character value. The base file path for the output data.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to
#' produce structured log output.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#'
#' changelog and author contributions
#   Nora Catolico (2026-08-11)
#'     Original Creation
#
##############################################################################################

context("\n                       Unit test of wrap.discharge.predict.R\n")

test_that("Unit test of wrap.discharge.predict.R", {

  source('../../flow.discharge.predict/wrap.discharge.predict.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  # Create a minimal Pachyderm-shaped input containing only the CSD parquet.
  testRoot <- tempfile('wrap-discharge-predict-')
  testDirOut <- file.path(testRoot, 'out')
  withr::defer(unlink(testRoot, recursive = TRUE))
  testDirIn <- file.path(
    testRoot,
    'pfs/l4discharge_predictgroup_tmp',
    '2024/09/01/l4discharge_BLUE110100'
  )
  base::dir.create(file.path(testDirIn, 'data'), recursive = TRUE)

  # Create a deterministic corrected discharge parquet so wrapper uses
  # pass-through branch instead of full BaM modeling.
  correctedFileName <- 'l4discharge_BLUE110100_2024-09-01_CSD_15_min_015.parquet'
  correctedFilePath <- file.path(testDirIn, 'data', correctedFileName)
  csdInput <- data.frame(
    startDateTime = as.POSIXct('2024-09-01 00:00:00', tz = 'UTC'),
    endDateTime = as.POSIXct('2024-09-01 00:15:00', tz = 'UTC'),
    stationHorizontalID = '100',
    dischargeContinuous = 12.34,
    dischargeFinalQF = 0,
    dischargeCorrectionApplied = 0,
    waterColumnHeightCorrectionApplied = 0,
    dischargeCorrectedShiftPost = 0,
    dischargeCorrectedShiftPre = 0,
    dischargeGapFilledUSGS = 0,
    dischargeGapFilledConstant = 0,
    dischargeGapFilledInterpolation = 0
  )
  writeInput <- try(
    NEONprocIS.base::def.wrte.parq(
      data = csdInput,
      NameFile = correctedFilePath,
      Schm = NULL
    ),
    silent = TRUE
  )
  testthat::expect_false(inherits(writeInput, 'try-error'))
  testthat::expect_true(file.exists(correctedFilePath))

  # Run wrapper.
  wrap.discharge.predict(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    SchmDataOut = NULL,
    log = log
  )

  # Check output file exists in expected output structure.
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  dirOutData <- file.path(base::paste0(testDirOut, testDirRepo), 'data')
  testthat::expect_true(dir.exists(dirOutData))

  outFilePath <- file.path(dirOutData, correctedFileName)
  testthat::expect_true(file.exists(outFilePath))

  # Validate pass-through content.
  csdOut <- try(NEONprocIS.base::def.read.parq(NameFile = outFilePath, log = log), silent = FALSE)
  testthat::expect_true(length(csdOut) != 0)
  testthat::expect_true(nrow(csdOut) == 1)
  testthat::expect_true('dischargeContinuous' %in% names(csdOut))
  testthat::expect_equal(round(csdOut$dischargeContinuous[1], 2), 12.34)

})