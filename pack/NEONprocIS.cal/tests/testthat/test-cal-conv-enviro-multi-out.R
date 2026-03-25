##############################################################################################
#' @title Unit test of def.cal.conv.enviro.multi.out.R, calibration conversion for EnviroSCAN sensor

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Run unit tests for def.cal.conv.enviro.multi.out.R.
#' Tests cover:
#'   - Happy path: non-permafrost site (BART) produces manufacturer-default and soil-specific
#'     calibrated outputs, correct depth variable, and expected uncertainty values.
#'   - Happy path: permafrost site (BARR) skips soil-specific calibration (Alt output is all NA).
#'   - Sad path: NULL calSlct returns NA calibrated output with a warning.
#'
#' Fixture data:
#'   testdata/enviroscan/calibrations/rawVSWC0/enviroscan_vswc0_test.xml
#'     CVALD1=5, CVALA1=1, CVALA2=1, CVALA3=0
#'     Valid 2024-01-01 to 2025-01-01
#'   testdata/enviroscan/soilSpecCal_test.csv
#'     Three BART depth layers (0-9, 9-22, 22-40 cm)
#'
# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-03-05)
#     original creation
##############################################################################################
library(testthat)

context("\n       | Unit test of def.cal.conv.enviro.multi.out (EnviroSCAN calibration conversion)\n")

test_that("EnviroSCAN calibration - non-permafrost site (BART)", {

  testDir <- "testdata/enviroscan/"

  # ---- Input data --------------------------------------------------------
  data <- data.frame(
    readout_time = base::as.POSIXct(
      c('2024-05-01 01:00:00', '2024-05-01 02:00:00', '2024-05-01 03:00:00'),
      tz = 'GMT'
    ),
    rawVSWC0 = c(0.3, 0.3, 0.3),
    stringsAsFactors = FALSE
  )
  varConv <- 'rawVSWC0'

  # ---- calSlct -----------------------------------------------------------
  DirCal <- paste0(testDir, "calibrations")
  TimeBgn <- base::as.POSIXct('2024-05-01 00:00:00', tz = 'GMT')
  TimeEnd  <- base::as.POSIXct('2024-05-02 00:00:00', tz = 'GMT')
  varCal <- base::dir(DirCal)
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = 999, stringsAsFactors = FALSE)

  calSlct <- NEONprocIS.cal::wrap.cal.slct(
    DirCal       = DirCal,
    NameVarExpc  = character(0),
    TimeBgn      = TimeBgn,
    TimeEnd      = TimeEnd,
    NumDayExpiMax = NumDayExpiMax,
    log          = NULL
  )

  # ---- Meta --------------------------------------------------------------
  Meta <- list(
    pathCalSoilSpec = paste0(testDir, "soilSpecCal_test.csv"),
    Locations = list(
      list(
        site        = "BART",
        geolocations = list(list(z_offset = 0))
      )
    )
  )

  # ---- Run function ------------------------------------------------------
  rpt <- NEONprocIS.cal::def.cal.conv.enviro.multi.out(
    data    = data,
    varConv = varConv,
    calSlct = calSlct,
    Meta    = Meta,
    log     = NULL
  )

  # ---- Test 1: output list has expected elements -------------------------
  testthat::expect_true(base::is.list(rpt))
  testthat::expect_true(all(c('data', 'ucrtData', 'ucrtCoef') %in% base::names(rpt)))

  # ---- Test 2: output data has expected columns --------------------------
  testthat::expect_true('rawVSWC0'    %in% base::names(rpt$data))  # manufacturer cal
  testthat::expect_true('rawVSWC0Alt' %in% base::names(rpt$data))  # soil-specific cal
  testthat::expect_true('L00Depth'    %in% base::names(rpt$data))  # depth

  # ---- Test 3: manufacturer calibrated value is correct ------------------
  # dataSf = 0.3^1 * 1 + 0 = 0.3
  # out = ((0.3 - 0.02852) / 0.1957)^(1/0.404) / 100 = 0.02248273
  testthat::expect_equal(rpt$data$rawVSWC0[1], 0.02248273, tolerance = 1e-5)
  testthat::expect_true(all(!base::is.na(rpt$data$rawVSWC0)))

  # ---- Test 4: soil-specific calibrated value is correct -----------------
  # BART row 1 coefficients applied to dataSf = 0.3 → 0.1085483
  testthat::expect_equal(rpt$data$rawVSWC0Alt[1], 0.1085483, tolerance = 1e-5)
  testthat::expect_true(all(!base::is.na(rpt$data$rawVSWC0Alt)))

  # ---- Test 5: depth variable is correct ---------------------------------
  # CVALD1 = 5 cm, z_offset = 0 → depth = 5/-100 + 0 = -0.05 m
  testthat::expect_equal(rpt$data$L00Depth[1], -0.05)
  testthat::expect_true(all(rpt$data$L00Depth == -0.05))

  # ---- Test 6: manufacturer uncertainty is constant ----------------------
  testthat::expect_true('rawVSWC0' %in% base::names(rpt$ucrtData))
  testthat::expect_true(all(rpt$ucrtData$rawVSWC0$ucrtMeas == 0.1068177))

  # ---- Test 7: soil-specific uncertainty is recorded ---------------------
  testthat::expect_true('rawVSWC0Alt' %in% base::names(rpt$ucrtData))
  # U_CVALA3 from BART row 1 = 0.0510791418708215
  testthat::expect_equal(rpt$ucrtData$rawVSWC0Alt$ucrtMeas[1], 0.0510791418708215, tolerance = 1e-10)

  # ---- Test 8: uncertainty coefficients are present ----------------------
  testthat::expect_true('rawVSWC0'    %in% base::names(rpt$ucrtCoef))
  testthat::expect_true('rawVSWC0Alt' %in% base::names(rpt$ucrtCoef))

})

test_that("EnviroSCAN calibration - permafrost site (BARR) skips soil-specific cal", {

  testDir <- "testdata/enviroscan/"

  data <- data.frame(
    readout_time = base::as.POSIXct(
      c('2024-05-01 01:00:00', '2024-05-01 02:00:00'),
      tz = 'GMT'
    ),
    rawVSWC0 = c(0.3, 0.3),
    stringsAsFactors = FALSE
  )
  varConv <- 'rawVSWC0'

  DirCal <- paste0(testDir, "calibrations")
  TimeBgn <- base::as.POSIXct('2024-05-01 00:00:00', tz = 'GMT')
  TimeEnd  <- base::as.POSIXct('2024-05-02 00:00:00', tz = 'GMT')
  varCal <- base::dir(DirCal)
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = 999, stringsAsFactors = FALSE)

  calSlct <- NEONprocIS.cal::wrap.cal.slct(
    DirCal        = DirCal,
    NameVarExpc   = character(0),
    TimeBgn       = TimeBgn,
    TimeEnd       = TimeEnd,
    NumDayExpiMax = NumDayExpiMax,
    log           = NULL
  )

  Meta <- list(
    pathCalSoilSpec = paste0(testDir, "soilSpecCal_test.csv"),
    Locations = list(
      list(
        site        = "BARR",
        geolocations = list(list(z_offset = 0))
      )
    )
  )

  rpt <- NEONprocIS.cal::def.cal.conv.enviro.multi.out(
    data    = data,
    varConv = varConv,
    calSlct = calSlct,
    Meta    = Meta,
    log     = NULL
  )

  # Manufacturer cal should still be applied
  testthat::expect_equal(rpt$data$rawVSWC0[1], 0.02248273, tolerance = 1e-5)

  # Alt (soil-specific) output should be all NA for permafrost sites
  testthat::expect_true('rawVSWC0Alt' %in% base::names(rpt$data))
  testthat::expect_true(all(base::is.na(rpt$data$rawVSWC0Alt)))

  # Alt uncertainty should also be all NA
  testthat::expect_true(all(base::is.na(rpt$ucrtData$rawVSWC0Alt$ucrtMeas)))

})

test_that("EnviroSCAN calibration - NULL calSlct returns NA output", {

  data <- data.frame(
    readout_time = base::as.POSIXct(c('2024-05-01 01:00:00', '2024-05-01 02:00:00'), tz = 'GMT'),
    rawVSWC0 = c(0.3, 0.3),
    stringsAsFactors = FALSE
  )
  varConv <- 'rawVSWC0'

  calSlct <- list(rawVSWC0 = NULL)

  Meta <- list(
    pathCalSoilSpec = NULL,
    Locations = list(
      list(
        site        = "BART",
        geolocations = list(list(z_offset = 0))
      )
    )
  )

  rpt <- NEONprocIS.cal::def.cal.conv.enviro.multi.out(
    data    = data,
    varConv = varConv,
    calSlct = calSlct,
    Meta    = Meta,
    log     = NULL
  )

  testthat::expect_true(all(base::is.na(rpt$data$rawVSWC0)))

})
