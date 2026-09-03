##############################################################################################
#' @title Unit test of nominal calibration conversion function def.cal.conv.nmnl

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description
#' Unit tests for def.cal.conv.nmnl.R, a function that applies a NEON nominal calibration
#' (CVAL coefficient) to convert nominally-scaled sensor readings to calibrated output.
#' The unit tests include positive and negative scenarios.
#' Positive tests: valid data, Meta, calSlct, and calibration file produce expected output.
#' Negative tests: missing readout_time, non-numeric data, missing Meta term, empty Meta.
#'
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

# changelog and author contributions / copyrights
#   Nora Catolico (2026-07-10)
#     original creation
##############################################################################################

test_that("testing nominal calibration conversion function def.cal.conv.nmnl", {

  Sys.setenv(LOG_LEVEL = 'debug')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")

  # ---- Shared setup ----
  timeBgn <- as.POSIXct('2025-11-01', tz = 'UTC')
  timeEnd <- as.POSIXct('2027-01-07', tz = 'UTC')
  readout_time <- as.POSIXct(c('2025-12-18 00:00:00', '2025-12-18 00:01:00', '2025-12-18 00:02:00'), tz = 'UTC')

  # Nominal value for speed (1 rotation / 6 counts per rotation = 0.1666667 revolutions per count)
  nomVal  <- 0.1666667
  coefVal <- 0.166635  # CVALB1 from calibration_nominal.xml
  rawSpeed <- c(200, 300, 400)

  # Build calSlct using the nominal calibration XML fixture
  calFile <- 'calibrations/nominal/calibration_nominal.xml'
  calSlct <- list(
    speed = data.frame(
      timeBgn  = timeBgn,
      timeEnd  = timeEnd,
      path     = testthat::test_path('') ,
      file     = calFile,
      stringsAsFactors = FALSE
    )
  )

  # Meta in serialized single-string form (as parsed by flow.cal.conv)
  Meta <- list(speed = '0.1666667,CVALB1')

  data <- data.frame(readout_time = readout_time, speed = rawSpeed, stringsAsFactors = FALSE)

  ##########
  ########## Positive test 1: valid inputs produce correctly calibrated output column
  ##########
  result <- NEONprocIS.cal::def.cal.conv.nmnl(
    data    = data,
    varConv = 'speed',
    calSlct = calSlct,
    Meta    = Meta,
    log     = log
  )

  # Output should be the original data frame with one extra column
  testthat::expect_true(is.data.frame(result))
  testthat::expect_true('speedCalibrated' %in% names(result))
  testthat::expect_equal(ncol(result), ncol(data) + 1)

  # Check calibrated values: raw / nomVal * coefVal
  expected <- rawSpeed / nomVal * coefVal
  testthat::expect_equal(result$speedCalibrated, expected, tolerance = 1e-5)

  cat("\n       |====== Positive test 1::                                          =====|\n")
  cat("\n       |------ Valid data/Meta/calSlct produce correct calibrated column       |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Positive test 2: NULL calSlct returns NA calibrated output without error
  ##########
  result_null_cal <- NEONprocIS.cal::def.cal.conv.nmnl(
    data    = data,
    varConv = 'speed',
    calSlct = list(speed = NULL),
    Meta    = Meta,
    log     = log
  )

  testthat::expect_true('speedCalibrated' %in% names(result_null_cal))
  testthat::expect_true(all(is.na(result_null_cal$speedCalibrated)))

  cat("\n       |====== Positive test 2::                                          =====|\n")
  cat("\n       |------ NULL calSlct returns all-NA calibrated column without error     |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Positive test 3: Meta provided as legacy three-element vector
  ##########
  MetaLegacy <- list(speed = c('speed', '0.1666667', 'CVALB1'))

  result_legacy <- NEONprocIS.cal::def.cal.conv.nmnl(
    data    = data,
    varConv = 'speed',
    calSlct = calSlct,
    Meta    = MetaLegacy,
    log     = log
  )

  testthat::expect_true('speedCalibrated' %in% names(result_legacy))
  testthat::expect_equal(result_legacy$speedCalibrated, expected, tolerance = 1e-5)

  cat("\n       |====== Positive test 3::                                          =====|\n")
  cat("\n       |------ Legacy three-element Meta vector parsed correctly               |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Negative test 1: data frame missing readout_time column
  ##########
  data_no_time <- data.frame(speed = rawSpeed, stringsAsFactors = FALSE)

  result_no_time <- try(
    NEONprocIS.cal::def.cal.conv.nmnl(
      data    = data_no_time,
      varConv = 'speed',
      calSlct = calSlct,
      Meta    = Meta,
      log     = log
    ),
    silent = TRUE
  )

  testthat::expect_true(class(result_no_time)[1] == 'try-error')

  cat("\n       |====== Negative test 1::                                          =====|\n")
  cat("\n       |------ Missing readout_time column causes error                        |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Negative test 2: data column is non-numeric
  ##########
  data_char <- data.frame(
    readout_time = readout_time,
    speed        = c('a', 'b', 'c'),
    stringsAsFactors = FALSE
  )

  result_char <- try(
    NEONprocIS.cal::def.cal.conv.nmnl(
      data    = data_char,
      varConv = 'speed',
      calSlct = calSlct,
      Meta    = Meta,
      log     = log
    ),
    silent = TRUE
  )

  testthat::expect_true(class(result_char)[1] == 'try-error')

  cat("\n       |====== Negative test 2::                                          =====|\n")
  cat("\n       |------ Non-numeric varConv column causes error                         |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Negative test 3: Meta is empty list
  ##########
  result_empty_meta <- try(
    NEONprocIS.cal::def.cal.conv.nmnl(
      data    = data,
      varConv = 'speed',
      calSlct = calSlct,
      Meta    = list(),
      log     = log
    ),
    silent = TRUE
  )

  testthat::expect_true(class(result_empty_meta)[1] == 'try-error')

  cat("\n       |====== Negative test 3::                                          =====|\n")
  cat("\n       |------ Empty Meta list causes error                                    |\n")
  cat("\n       |======================================================================|\n")

  ##########
  ########## Negative test 4: Meta does not contain the requested varConv term
  ##########
  MetaWrongTerm <- list(direction = '355,CVALA1')

  result_wrong_term <- try(
    NEONprocIS.cal::def.cal.conv.nmnl(
      data    = data,
      varConv = 'speed',
      calSlct = calSlct,
      Meta    = MetaWrongTerm,
      log     = log
    ),
    silent = TRUE
  )

  testthat::expect_true(class(result_wrong_term)[1] == 'try-error')

  cat("\n       |====== Negative test 4::                                          =====|\n")
  cat("\n       |------ Meta missing entry for requested term causes error              |\n")
  cat("\n       |======================================================================|\n")

})
