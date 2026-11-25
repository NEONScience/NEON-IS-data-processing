##############################################################################################
#' @title Unit test for Wrapper for Continuous Discharge Prediction
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description Unit tests for wrap.discharge.predict.R. Verify corrected CSD
#' pass-through and TOMB USGS publication output.
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
#   Nora Catolico (2026-11-24)
#'     Add TOMB USGS publication test
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
    SchmCSDOut = NULL,
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

test_that("TOMB USGS discharge is published with regression estimates", {

  source('../../flow.discharge.predict/wrap.discharge.predict.R')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  testRoot <- tempfile('wrap-discharge-predict-tomb-')
  withr::defer(unlink(testRoot, recursive = TRUE))
  testDirIn <- file.path(
    testRoot,
    'pfs/l4discharge_predictgroup_tmp',
    '2021/09/05/l4discharge_TOMB101100'
  )
  testDirData <- file.path(testDirIn, 'data')
  testDirSWEData <- file.path(
    testDirIn,
    'surfacewater-physical_TOMB101100',
    'data'
  )
  base::dir.create(testDirData, recursive = TRUE)
  base::dir.create(testDirSWEData, recursive = TRUE)

  inputFileName <- 'surfacewater-physical_TOMB101100_2021-09-05_EOS_1_min_001.parquet'
  inputFilePath <- file.path(testDirSWEData, inputFileName)
  regressionFilePath <- file.path(
    testDirData,
    'NEON.DOM.SITE.DP1.00133.001.csd_dischargeRegressionUSGS_pub.parquet'
  )
  regression <- data.frame(
    usgsDischargeRegStartDate = '2020-01-01 00:00:00',
    usgsDischargeRegEndDate = '2022-01-01 00:00:00',
    usgsDischargeRegID = 'TOMB.USGS.2021.reg.1',
    usgsDischargeRegSlope = 1,
    usgsDischargeRegSlopeSE = 0.1,
    usgsDischargeRegIntercept = 0,
    usgsDischargeRegInterceptSE = 0.05,
    usgsSiteNumber = '1234567'
  )

  testthat::expect_false(inherits(
    try(
      NEONprocIS.base::def.wrte.parq(
        data = data.frame(surfacewaterColumnHeight = 1),
        NameFile = inputFilePath,
        Schm = NULL
      ),
      silent = TRUE
    ),
    'try-error'
  ))
  testthat::expect_false(inherits(
    try(
      NEONprocIS.base::def.wrte.parq(
        data = regression,
        NameFile = regressionFilePath,
        Schm = NULL
      ),
      silent = TRUE
    ),
    'try-error'
  ))

  retrievedArgs <- NULL
  testthat::local_mocked_bindings(
    read_waterdata_continuous = function(monitoring_location_id,
                                         parameter_code,
                                         time) {
      retrievedArgs <<- list(
        monitoring_location_id = monitoring_location_id,
        parameter_code = parameter_code,
        time = time
      )
      data.frame(
        time = as.POSIXct(
          c(
            '2021-09-05 00:00:00',
            '2021-09-05 12:00:00',
            '2021-09-05 23:30:00',
            '2021-09-05 23:45:00'
          ),
          tz = 'UTC'
        ),
        value = c(1, 2, 3, 4),
        qualifier = c('A', 'P', 'A', 'A')
      )
    },
    .package = 'dataRetrieval'
  )

  testDirOut <- file.path(testRoot, 'out')
  wrap.discharge.predict(
    DirIn = testDirIn,
    DirOutBase = testDirOut,
    SchmCSDOut = NULL,
    SchmUSGSOut = NULL,
    log = log
  )

  testthat::expect_equal(retrievedArgs$monitoring_location_id, 'USGS-01234567')
  testthat::expect_equal(retrievedArgs$parameter_code, '00060')

  outputFileName <- 'l4discharge_TOMB101100_2021-09-05_CSD_USGS_100.parquet'
  outputFilePath <- file.path(
    testDirOut,
    '2021/09/05/l4discharge_TOMB101100/data',
    outputFileName
  )
  testthat::expect_true(file.exists(outputFilePath))

  usgsOut <- NEONprocIS.base::def.read.parq(NameFile = outputFilePath, log = log)
  testthat::expect_equal(nrow(usgsOut), 3)
  expectedTimes <- as.POSIXct(
    c(
      '2021-09-05 00:00:00',
      '2021-09-05 12:00:00',
      '2021-09-05 23:30:00'
    ),
    tz = 'UTC'
  )
  testthat::expect_equal(as.numeric(usgsOut$startDate), as.numeric(expectedTimes))
  testthat::expect_equal(as.numeric(usgsOut$endDate), as.numeric(expectedTimes))
  testthat::expect_true(all(c(
    'startDate',
    'endDate',
    'usgsSiteNumber',
    'usgsDischargeRegID',
    'usgsDischarge',
    'regressionDerivedDischarge',
    'withRegressionUncQLower1Std',
    'withRegressionUncQUpper1Std',
    'withRegressionUncQLower2Std',
    'withRegressionUncQUpper2Std',
    'usgsValueQualCode',
    'dischargeFinalQF'
  ) %in% names(usgsOut)))
  testthat::expect_equal(usgsOut$usgsSiteNumber, rep('01234567', 3))
  testthat::expect_equal(usgsOut$usgsDischargeRegID, rep('TOMB.USGS.2021.reg.1', 3))
  testthat::expect_equal(
    usgsOut$usgsDischarge,
    c(1, 2, 3) * 28.316847,
    tolerance = 1e-5
  )
  testthat::expect_equal(usgsOut$usgsValueQualCode, c('A', 'P', 'A'))
  testthat::expect_equal(usgsOut$dischargeFinalQF, c(0, 0, 0))
  testthat::expect_equal(
    usgsOut$regressionDerivedDischarge,
    c(1, 2, 3) * 28.316847,
    tolerance = 1e-5
  )
  logDischarge <- log10(c(1, 2, 3) * 28.316847)
  testthat::expect_equal(
    usgsOut$withRegressionUncQLower1Std,
    10^(logDischarge * (1 - 0.1) - 0.05),
    tolerance = 1e-5
  )
  testthat::expect_equal(
    usgsOut$withRegressionUncQUpper1Std,
    10^(logDischarge * (1 + 0.1) + 0.05),
    tolerance = 1e-5
  )
  testthat::expect_equal(
    usgsOut$withRegressionUncQLower2Std,
    10^(logDischarge * (1 - 1.96 * 0.1) - 1.96 * 0.05),
    tolerance = 1e-5
  )
  testthat::expect_equal(
    usgsOut$withRegressionUncQUpper2Std,
    10^(logDischarge * (1 + 1.96 * 0.1) + 1.96 * 0.05),
    tolerance = 1e-5
  )
})