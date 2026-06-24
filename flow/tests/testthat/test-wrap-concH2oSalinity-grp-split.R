##############################################################################################
#' @title Unit tests for wrap.concH2oSalinity.grp.split

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description Unit tests for the wrap.concH2oSalinity.grp.split function, which selects
#' and renames depth-specific columns from EnviroSCAN soil salinity stats and quality_metrics
#' parquet files based on the VER code in the group JSON.

##############################################################################################

library(testthat)
library(arrow)
library(jsonlite)

source('../../flow.concH2oSalinity.grp.split/wrap.concH2oSalinity.grp.split.R')

# ---------------------------------------------------------------------------
# Helper: build a minimal test datum on disk
#
# Creates:
#   <baseDir>/pfs/<repo>/yyyy/mm/dd/<groupName>/group/<cfgloc>.json
#   <baseDir>/pfs/<repo>/yyyy/mm/dd/<groupName>/<cfgloc>/stats/<statsFile>
#   <baseDir>/pfs/<repo>/yyyy/mm/dd/<groupName>/<cfgloc>/quality_metrics/<qmFile>
#
# Returns a list with $DirIn and $DirOutBase ready for wrap.concH2oSalinity.grp.split.
# ---------------------------------------------------------------------------
makeTestDatum <- function(baseDir,
                          cfgloc   = "CFGLOC105332",
                          site     = "GRSM",
                          HOR      = "004",
                          VER      = "503",
                          statsFile  = "enviroscan_parsed_CFGLOC105332_2025-10-17_basicStats_030.parquet",
                          qmFile     = "enviroscan_parsed_CFGLOC105332_2025-10-17_qualityMetrics_030.parquet",
                          statsDf    = NULL,
                          qmDf       = NULL) {

  repo      <- "concH2oSalinity_grp_split"
  groupName <- paste0("conc-h2o-soil-salinity-split_", site, HOR, VER)
  dateDir   <- "2025/10/17"

  groupDir  <- file.path(baseDir, "pfs", repo, dateDir, groupName, "group")
  cfglocDir <- file.path(baseDir, "pfs", repo, dateDir, groupName, cfgloc)
  statsDir  <- file.path(cfglocDir, "stats")
  qmDir     <- file.path(cfglocDir, "quality_metrics")

  dir.create(groupDir,  recursive = TRUE, showWarnings = FALSE)
  dir.create(statsDir,  recursive = TRUE, showWarnings = FALSE)
  dir.create(qmDir,     recursive = TRUE, showWarnings = FALSE)

  # Write group JSON
  grpJson <- list(
    type     = "FeatureCollection",
    features = list(list(
      type       = "Feature",
      geometry   = NULL,
      HOR        = HOR,
      VER        = VER,
      site       = site,
      properties = list(name = cfgloc)
    ))
  )
  writeLines(jsonlite::toJSON(grpJson, auto_unbox = TRUE),
             file.path(groupDir, paste0(cfgloc, ".json")))

  # Default stats data frame: two rows, multiple depths, includes SoilMoisture columns
  if (is.null(statsDf)) {
    statsDf <- data.frame(
      startDateTime         = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:30:00"), tz = "UTC"),
      endDateTime           = as.POSIXct(c("2025-10-17 00:30:00", "2025-10-17 01:00:00"), tz = "UTC"),
      VSICDepth01Mean       = c(1.1, 1.2),
      VSICDepth02Mean       = c(2.1, 2.2),
      VSICDepth03Mean       = c(3.1, 3.2),
      Depth01SoilMoisture   = c(0.01, 0.01),
      Depth02SoilMoisture   = c(0.02, 0.02),
      Depth03SoilMoisture   = c(0.03, 0.03),
      stringsAsFactors      = FALSE
    )
  }

  # Default quality_metrics data frame
  if (is.null(qmDf)) {
    qmDf <- data.frame(
      startDateTime                    = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:30:00"), tz = "UTC"),
      endDateTime                      = as.POSIXct(c("2025-10-17 00:30:00", "2025-10-17 01:00:00"), tz = "UTC"),
      VSICDepth01AlphaQM               = c(0L, 1L),
      VSICDepth02AlphaQM               = c(0L, 0L),
      VSICDepth03AlphaQM               = c(1L, 0L),
      stringsAsFactors                 = FALSE
    )
  }

  if (!is.null(statsFile)) {
    arrow::write_parquet(statsDf, file.path(statsDir, statsFile))
  }
  if (!is.null(qmFile)) {
    arrow::write_parquet(qmDf, file.path(qmDir, qmFile))
  }

  list(
    DirIn      = file.path(baseDir, "pfs", repo, dateDir, groupName, cfgloc),
    DirOutBase = file.path(baseDir, "out")
  )
}

########################################################################################################
# Test 1: correct depth selection and renaming (mid-range depth, VER=503)
########################################################################################################
test_that("VER=503 selects Depth03 columns, renames them, excludes SoilMoisture", {

  d <- makeTestDatum(baseDir = file.path(tempdir(), "t1"), VER = "503")

  wrap.concH2oSalinity.grp.split(
    DirIn      = d$DirIn,
    DirOutBase = d$DirOutBase
  )

  # Locate the output stats file
  outStatsFiles <- list.files(file.path(d$DirOutBase), pattern = "\\.parquet$",
                              recursive = TRUE)
  statsOut <- outStatsFiles[grepl("/stats/", outStatsFiles)]
  qmOut    <- outStatsFiles[grepl("/quality_metrics/", outStatsFiles)]

  expect_equal(length(statsOut), 1L)
  expect_equal(length(qmOut),    1L)

  # Read stats output
  dfStats <- arrow::read_parquet(file.path(d$DirOutBase, statsOut))
  expect_true(all(c("startDateTime", "endDateTime", "VSICMean") %in% names(dfStats)))

  # Depth01 and Depth02 columns should NOT be present
  expect_false(any(grepl("Depth01|Depth02", names(dfStats))))

  # SoilMoisture column should NOT be present
  expect_false(any(grepl("SoilMoisture", names(dfStats))))

  # Data values should match Depth03 source values
  expect_equal(dfStats$VSICMean, c(3.1, 3.2), tolerance = 1e-6)

  # Read quality_metrics output
  dfQm <- arrow::read_parquet(file.path(d$DirOutBase, qmOut))
  expect_true("VSICAlphaQM" %in% names(dfQm))
  expect_false(any(grepl("Depth01|Depth02", names(dfQm))))
  expect_equal(dfQm$VSICAlphaQM, c(1L, 0L))
})

########################################################################################################
# Test 2: first depth (VER=501)
########################################################################################################
test_that("VER=501 selects Depth01 columns", {

  d <- makeTestDatum(baseDir = file.path(tempdir(), "t2"), VER = "501", HOR = "001")

  wrap.concH2oSalinity.grp.split(
    DirIn      = d$DirIn,
    DirOutBase = d$DirOutBase
  )

  outFiles <- list.files(file.path(d$DirOutBase), pattern = "\\.parquet$", recursive = TRUE)
  statsOut <- outFiles[grepl("/stats/", outFiles)]
  dfStats  <- arrow::read_parquet(file.path(d$DirOutBase, statsOut))

  expect_true("VSICMean" %in% names(dfStats))
  expect_equal(dfStats$VSICMean, c(1.1, 1.2), tolerance = 1e-6)
  expect_false(any(grepl("Depth02|Depth03", names(dfStats))))
  expect_false(any(grepl("SoilMoisture", names(dfStats))))
})

########################################################################################################
# Test 3: last depth (VER=508)
########################################################################################################
test_that("VER=508 selects Depth08 columns", {

  # Create a stats data frame with a Depth08 column
  statsDf <- data.frame(
    startDateTime     = as.POSIXct("2025-10-17 00:00:00", tz = "UTC"),
    endDateTime       = as.POSIXct("2025-10-17 00:30:00", tz = "UTC"),
    VSICDepth07Mean   = 7.0,
    VSICDepth08Mean   = 8.0,
    Depth08SoilMoisture = 0.08,
    stringsAsFactors  = FALSE
  )
  qmDf <- data.frame(
    startDateTime     = as.POSIXct("2025-10-17 00:00:00", tz = "UTC"),
    endDateTime       = as.POSIXct("2025-10-17 00:30:00", tz = "UTC"),
    VSICDepth08AlphaQM = 0L,
    stringsAsFactors  = FALSE
  )

  d <- makeTestDatum(baseDir = file.path(tempdir(), "t3"), VER = "508", HOR = "001",
                     statsDf = statsDf, qmDf = qmDf)

  wrap.concH2oSalinity.grp.split(
    DirIn      = d$DirIn,
    DirOutBase = d$DirOutBase
  )

  outFiles <- list.files(file.path(d$DirOutBase), pattern = "\\.parquet$", recursive = TRUE)
  statsOut <- outFiles[grepl("/stats/", outFiles)]
  dfStats  <- arrow::read_parquet(file.path(d$DirOutBase, statsOut))

  expect_true("VSICMean" %in% names(dfStats))
  expect_equal(dfStats$VSICMean, 8.0, tolerance = 1e-6)
  expect_false(any(grepl("Depth07", names(dfStats))))
  expect_false(any(grepl("SoilMoisture", names(dfStats))))
})

########################################################################################################
# Test 4: missing group JSON stops with an error
########################################################################################################
test_that("Missing group JSON causes a fatal stop", {

  baseDir   <- file.path(tempdir(), "t4")
  repo      <- "concH2oSalinity_grp_split"
  groupName <- "conc-h2o-soil-salinity-split_GRSM004503"
  cfgloc    <- "CFGLOC105332"
  dateDir   <- "2025/10/17"

  cfglocDir <- file.path(baseDir, "pfs", repo, dateDir, groupName, cfgloc)
  dir.create(file.path(cfglocDir, "stats"),           recursive = TRUE, showWarnings = FALSE)
  dir.create(file.path(cfglocDir, "quality_metrics"), recursive = TRUE, showWarnings = FALSE)
  # No group/ directory created

  expect_error(
    wrap.concH2oSalinity.grp.split(
      DirIn      = cfglocDir,
      DirOutBase = file.path(baseDir, "out")
    )
  )
})

########################################################################################################
# Test 5: VER out of expected range (e.g., "500") stops with an error
########################################################################################################
test_that("VER=500 (depth index 0, out of range) causes a fatal stop", {

  d <- makeTestDatum(baseDir = file.path(tempdir(), "t5"), VER = "500", HOR = "001")

  expect_error(
    wrap.concH2oSalinity.grp.split(
      DirIn      = d$DirIn,
      DirOutBase = d$DirOutBase
    )
  )
})

########################################################################################################
# Test 6: empty stats directory produces a warning but does not stop
########################################################################################################
test_that("Empty stats directory warns but does not stop", {

  d <- makeTestDatum(baseDir = file.path(tempdir(), "t6"), VER = "503",
                     statsFile = NULL)  # no stats file written

  expect_no_error(
    wrap.concH2oSalinity.grp.split(
      DirIn      = d$DirIn,
      DirOutBase = d$DirOutBase
    )
  )
})

########################################################################################################
# Test 7: output file retains the original input filename unchanged
########################################################################################################
test_that("Output file name matches input file name exactly", {

  fname <- "enviroscan_parsed_CFGLOC105332_2025-10-17_basicStats_030.parquet"
  d <- makeTestDatum(baseDir = file.path(tempdir(), "t7"), VER = "503",
                     statsFile = fname)

  wrap.concH2oSalinity.grp.split(
    DirIn      = d$DirIn,
    DirOutBase = d$DirOutBase
  )

  outFiles  <- list.files(file.path(d$DirOutBase), pattern = "\\.parquet$", recursive = TRUE)
  statsOut  <- outFiles[grepl("/stats/", outFiles)]

  expect_equal(basename(statsOut), fname)
})

