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
  testDirOut <- file.path(workingDirPath, 'pfs/out')
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
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_EOS_1_min_001\\.parquet$', outFiles)))
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_EOS_30_min_030\\.parquet$', outFiles)))
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_EOS_5_min_005\\.parquet$', outFiles)))
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_TOSW_30_min_030\\.parquet$', outFiles)))
  testthat::expect_true(any(grepl('surfacewater-physical_BLUE110100_2024-09-01_TOSW_5_min_005\\.parquet$', outFiles)))

  expectedCsvs <- c(
    'NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv',
    'NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.csv',
    'NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv',
    'NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv',
    'NEON.DOM.SITE.DP4.00133.001.sdrc_stageDischargeCurveInfo_pub.csv'
  )
  testthat::expect_true(all(expectedCsvs %in% outFiles))
  testthat::expect_false('NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.csv' %in% outFiles)

  curveData <- read.csv(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv'))
  controlData <- read.csv(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv'))
  gapData <- read.csv(file.path(dirOutData, 'NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv'))

  testthat::expect_true(nrow(curveData) == 1)
  testthat::expect_true(all(grepl('^BLUE\\.', curveData$curveID)))
  testthat::expect_true(nrow(controlData) > 0)
  testthat::expect_true(all(controlData$namedLocation == 'BLUE.AOS.discharge'))
  testthat::expect_true(nrow(gapData) > 0)
  testthat::expect_true(all(gapData$namedLocation == 'BLUE.AOS.discharge'))

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
})