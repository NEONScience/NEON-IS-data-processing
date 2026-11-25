##############################################################################################
#' @title Unit test for Wrapper for L4 Discharge OS input table processing
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit test for wrap.discharge.parse.os.inputs.R. Tests basic
#' file/directory input/output and successful output of filtered OS tables for a
#' discharge datum.
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
#     Original Creation
#
##############################################################################################

context("\n                       Unit test of wrap.discharge.parse.os.inputs.R\n")

test_that("Unit test of wrap.discharge.parse.os.inputs.R", {

  source('../../flow.discharge.parse.os.inputs/wrap.discharge.parse.os.inputs.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  library(lubridate)
  library(stringr)

  # Set up working directories and fixture paths.
  workingDirPath <- getwd()
  testDirIn <- file.path(
    workingDirPath,
    'pfs/l4discharge_csd_swe_group/2024/09/01/l4discharge_BLUE110100/surfacewater-physical_BLUE110100'
  )
  testDirOut <- tempfile('wrap-discharge-parse-os-inputs-')
  withr::defer(unlink(testDirOut, recursive = TRUE))
  testDirOS <- file.path(workingDirPath, 'pfs/l4discharge_os_table_loader')

  # Read OS input tables used by the wrapper.
  csd_constantBiasShift_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.csv'))
  csd_dataGapToFillMethodMapping_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv'))
  csd_gapFillingRegression_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.csv'))
  csd_gaugeWaterColumnRegression_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv'))
  sdrc_controlInfo_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv'))
  sdrc_curveIdentification_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv'))
  sdrc_priorParameters_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv'))
  sdrc_gaugeDischargeMeas_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv'))
  sdrc_sampledParameters_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv'))
  sdrc_gaugePressureRelationship_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv'))
  sdrc_stageDischargeCurveInfo_pub <- read.csv(file.path(testDirOS, 'NEON.DOM.SITE.DP4.00133.001.sdrc_stageDischargeCurveInfo_pub.csv'))

  # Get expected output repo/day path.
  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- infoDirIn$dirRepo
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  dirOutData <- file.path(testDirOutPath, 'data')

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  wrap.discharge.parse.os.inputs(
    DirIn = testDirIn,
    csd_constantBiasShift_pub = csd_constantBiasShift_pub,
    csd_dataGapToFillMethodMapping_pub = csd_dataGapToFillMethodMapping_pub,
    csd_gapFillingRegression_pub = csd_gapFillingRegression_pub,
    csd_gaugeWaterColumnRegression_pub = csd_gaugeWaterColumnRegression_pub,
    csd_dischargeRegressionUSGS_pub = data.frame(
      usgsDischargeRegStartDate = character(),
      usgsDischargeRegEndDate = character(),
      usgsDischargeRegID = character()
    ),
    sdrc_controlInfo_pub = sdrc_controlInfo_pub,
    sdrc_curveIdentification_pub = sdrc_curveIdentification_pub,
    sdrc_priorParameters_pub = sdrc_priorParameters_pub,
    sdrc_gaugeDischargeMeas_pub = sdrc_gaugeDischargeMeas_pub,
    sdrc_sampledParameters_pub = sdrc_sampledParameters_pub,
    sdrc_gaugePressureRelationship_pub = sdrc_gaugePressureRelationship_pub,
    sdrc_stageDischargeCurveInfo_pub = sdrc_stageDischargeCurveInfo_pub,
    DirOutBase = testDirOut,
    log = log
  )

  testthat::expect_true(dir.exists(testDirOutPath))
  testthat::expect_true(dir.exists(dirOutData))

  outFiles <- base::list.files(dirOutData, full.names = FALSE)
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_EOS_30_min_030\\.parquet$', outFiles)))

  expectedOsParquets <- c(
    'NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.parquet',
    'NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.parquet',
    'NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.parquet',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.parquet',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.parquet',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.parquet',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.parquet',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.parquet',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.parquet',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_stageDischargeCurveInfo_pub.parquet'
  )
  testthat::expect_true(all(expectedOsParquets %in% outFiles))
  testthat::expect_false('NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.parquet' %in% outFiles)

  curveData <- arrow::read_parquet(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.parquet'))
  controlData <- arrow::read_parquet(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.parquet'))
  gapData <- arrow::read_parquet(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.parquet'))

  testthat::expect_true(nrow(curveData) == 1)
  testthat::expect_true(all(grepl('^BLUE\\.', curveData$curveID)))
  testthat::expect_true(nrow(controlData) > 0)
  testthat::expect_true(all(controlData$namedLocation == 'BLUE.AOS.discharge'))
  testthat::expect_true(nrow(gapData) > 0)
  testthat::expect_true(all(gapData$namedLocation == 'BLUE.AOS.discharge'))

})

test_that("TOMB writes only the active USGS regression table", {

  source('../../flow.discharge.parse.os.inputs/wrap.discharge.parse.os.inputs.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  testRoot <- tempfile('wrap-discharge-parse-os-inputs-tomb-')
  withr::defer(unlink(testRoot, recursive = TRUE))
  testDirIn <- file.path(
    testRoot,
    'pfs/l4discharge_csd_swe_group/2021/09/05/l4discharge_TOMB101100',
    'surfacewater-physical_TOMB101100'
  )
  base::dir.create(file.path(testDirIn, 'data'), recursive = TRUE)
  testDirOut <- file.path(testRoot, 'out')

  emptyDates <- data.frame(startDate = character(), endDate = character())
  emptyRegressionDates <- data.frame(
    regressionStartDate = character(),
    regressionEndDate = character()
  )
  emptyCurveDates <- data.frame(
    curveStartDate = character(),
    curveEndDate = character()
  )

  usgsRegressions <- data.frame(
    usgsDischargeRegStartDate = c('2021-09-01 00:00:00', '2022-01-01 00:00:00'),
    usgsDischargeRegEndDate = c('2021-09-30 23:59:59', '2022-12-31 23:59:59'),
    usgsDischargeRegID = c('TOMB.USGS.active', 'TOMB.USGS.inactive'),
    usgsDischargeRegSlope = c(1.2, 2.3)
  )

  wrap.discharge.parse.os.inputs(
    DirIn = testDirIn,
    csd_constantBiasShift_pub = emptyDates,
    csd_dataGapToFillMethodMapping_pub = emptyDates,
    csd_gapFillingRegression_pub = emptyDates,
    csd_gaugeWaterColumnRegression_pub = emptyRegressionDates,
    csd_dischargeRegressionUSGS_pub = usgsRegressions,
    sdrc_controlInfo_pub = data.frame(endDate = character()),
    sdrc_curveIdentification_pub = emptyCurveDates,
    sdrc_priorParameters_pub = data.frame(endDate = character()),
    sdrc_gaugeDischargeMeas_pub = data.frame(),
    sdrc_sampledParameters_pub = data.frame(),
    sdrc_gaugePressureRelationship_pub = data.frame(),
    sdrc_stageDischargeCurveInfo_pub = data.frame(),
    DirOutBase = testDirOut,
    log = log
  )

  infoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  outputPath <- paste0(
    testDirOut,
    infoDirIn$dirRepo,
    '/data/NEON.DOM.SITE.DP1.00133.001.csd_dischargeRegressionUSGS_pub.parquet'
  )
  testthat::expect_true(file.exists(outputPath))

  output <- arrow::read_parquet(outputPath)
  testthat::expect_equal(nrow(output), 1)
  testthat::expect_equal(output$usgsDischargeRegID, 'TOMB.USGS.active')
  testthat::expect_equal(output$usgsDischargeRegSlope, 1.2, tolerance = 1e-6)
})