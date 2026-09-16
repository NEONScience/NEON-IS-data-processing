##############################################################################################
#' @title Unit tests for Gap Filling Non-Regularized module for NEON IS data processing.

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Unit tests for wrap.gap.fill.nonrglr.
#'
# changelog and author contributions / copyrights
#   Nora Catolico (2026-02-09)
#     initial creation
#   Nora Catolico (2026-09-14)
#     added unit tests for deduplication (WndwDedup), source_id handling, schema fallbacks,
#     DirSubCopy, and robust error handling
##############################################################################################

context("\n       | Unit test of Gap Filling Non-Regularized module for NEON IS data processing \n")

test_that("Unit test of wrap.gap.fill.nonrglr", {
  source('../../flow.gap.fill.nonrglr/wrap.gap.fill.nonrglr.R')
  library(stringr)
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Define input parameters
  DirIn <- "pfs/sunav2_location_group_and_restructure/sunav2/2025/06/23/CFGLOC110819"
  DirOutBase <- "pfs/nitrate_out"
  DirFill <- c("data", "flags")
  WndwFill <- 15 # 15 minutes window
  DirSubCopy <- c("location", "uncertainty_coef")
  
  # Schemas
  schmData <- 'pfs/sunav2_avro_schemas/sunav2_logfilled.avsc'
  schmCalFlag <- 'pfs/sunav2_avro_schemas/sunav2_calibration_flags.avsc'
  schmLogFlag <- 'pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'
  NameCol <- c('DirFill', 'FileSchmFill')
  SchmDirs <- c("data", "flags", "flags")
  Schmas <- c(schmData, schmCalFlag, schmLogFlag)
  SchmFill <- data.frame(SchmDirs, Schmas)
  names(SchmFill) <- NameCol
  
  # Read in the schema(s)
  SchmFill$SchmFill <- NA
  for (idxSchmFill in 1:base::length(SchmFill$FileSchmFill)) {
    if (SchmFill$FileSchmFill[idxSchmFill] != 'NA') {
      SchmFill$SchmFill[idxSchmFill] <-
        base::paste0(base::readLines(SchmFill$FileSchmFill[idxSchmFill]),
                     collapse = '')
    }
  }
  
  # Clean up before run
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # =========================================================================
  # 1. Base test: Gap filling with schema specification and DirSubCopy
  # =========================================================================
  wrap.gap.fill.nonrglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirFill = DirFill,
    WndwFill = WndwFill,
    WndwDedup = NULL,
    SchmFill = SchmFill,
    DirSubCopy = DirSubCopy,
    log = log
  )
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn, log = log)
  dirOut <- paste0(DirOutBase, InfoDirIn$dirRepo)
  
  dirInData <- file.path(DirIn, 'data')
  dirInFlags <- file.path(DirIn, 'flags')
  dirInLoc <- file.path(DirIn, 'location')
  dirInUncert <- file.path(DirIn, 'uncertainty_coef')
  
  fileData <- base::list.files(dirInData)
  fileFlags <- base::list.files(dirInFlags)
  fileLoc <- base::list.files(dirInLoc)
  fileUncert <- base::list.files(dirInUncert)
  
  dirOutData <- file.path(dirOut, 'data')
  dirOutFlags <- file.path(dirOut, 'flags')
  dirOutLoc <- file.path(dirOut, 'location')
  dirOutUncert <- file.path(dirOut, 'uncertainty_coef')
  
  # Check for filled output file presence in DirOutBase
  testthat::expect_true(file.exists(file.path(dirOutData, fileData)))
  testthat::expect_true(all(file.exists(file.path(dirOutFlags, fileFlags))))
  
  # Check for pass-through output of 'location' and 'uncertainty_coef' via DirSubCopy
  testthat::expect_true(all(file.exists(file.path(dirOutLoc, fileLoc))))
  testthat::expect_true(all(file.exists(file.path(dirOutUncert, fileUncert))))
  
  # Check data integrity in output
  dataChk <- NEONprocIS.base::def.read.parq(NameFile = file.path(dirOutData, fileData))
  timeBgn <- InfoDirIn$time
  timeEnd <- InfoDirIn$time + as.difftime(1, units = 'days')
  all_starts <- seq(timeBgn, timeEnd - WndwFill * 60, by = WndwFill * 60)
  num_starts <- length(all_starts)
  
  # Check minimum expected points across the full day
  testthat::expect_true(length(dataChk$readout_time) >= num_starts)
  # Check all window starts exist
  floor_15m <- as.POSIXct(floor(as.numeric(dataChk$readout_time) / (15 * 60)) * (15 * 60),
                          origin = "1970-01-01", tz = "GMT")
  testthat::expect_true(all(all_starts %in% floor_15m))
  # Check data is sorted chronologically
  testthat::expect_false(is.unsorted(dataChk$readout_time))
  # Check source_id is populated for filled blank rows
  testthat::expect_false(any(is.na(dataChk$source_id)))
  testthat::expect_equal(as.character(unique(dataChk$source_id)), "49259")
  # Check no exact duplicate timestamps exist
  testthat::expect_equal(anyDuplicated(dataChk$readout_time), 0)
  
  # Clean up
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # =========================================================================
  # 2. Test deduplication with WndwDedup parameter
  # =========================================================================
  tempTestDir <- file.path(tempdir(), "test_gap_fill_dedup", "pfs", "test_repo", "sensor", "2025", "06", "23", "CFGLOC123456")
  dir.create(file.path(tempTestDir, "data"), recursive = TRUE)
  
  # Create synthetic test dataset with sub-minute readings within 4-second intervals
  base_time <- as.POSIXct("2025-06-23 00:00:00", tz = "GMT")
  test_times <- c(
    base_time + 2,
    base_time + 3,       # Falls in same 4s floor (0 to 4s) -> should be deduplicated
    base_time + 6,       # Falls in next 4s floor (4 to 8s)
    base_time + 900 + 1  # 15 min later
  )
  test_df <- data.frame(
    source_id = rep("12345", length(test_times)),
    readout_time = test_times,
    value = c(10.0, 10.5, 11.0, 12.0)
  )
  NEONprocIS.base::def.wrte.parq(data = test_df, NameFile = file.path(tempTestDir, "data", "sensor_CFGLOC123456_2025-06-23.parquet"))
  
  tempOutBase <- file.path(tempdir(), "test_gap_fill_dedup_out")
  if (dir.exists(tempOutBase)) unlink(tempOutBase, recursive = TRUE)
  
  # Run with 4-second deduplication window: WndwDedup = 4/60 minutes
  wrap.gap.fill.nonrglr(
    DirIn = tempTestDir,
    DirOutBase = tempOutBase,
    DirFill = "data",
    WndwFill = 15,
    WndwDedup = 4 / 60,
    SchmFill = NA,
    log = log
  )
  
  outDedupFile <- file.path(tempOutBase, "sensor", "2025", "06", "23", "CFGLOC123456", "data", "sensor_CFGLOC123456_2025-06-23.parquet")
  testthat::expect_true(file.exists(outDedupFile))
  df_dedup <- NEONprocIS.base::def.read.parq(NameFile = outDedupFile)
  
  # Expect no duplicated timestamps
  testthat::expect_equal(anyDuplicated(df_dedup$readout_time), 0)
  # Expect the first two readings (2s and 3s) floored/rounded to same timestamp (0s) to keep first record (value = 10.0)
  first_rec <- df_dedup[df_dedup$readout_time == base_time, ]
  testthat::expect_equal(nrow(first_rec), 1)
  testthat::expect_equal(first_rec$value, 10.0)
  
  # Clean up temp directories
  unlink(file.path(tempdir(), "test_gap_fill_dedup"), recursive = TRUE)
  unlink(tempOutBase, recursive = TRUE)
  
  # =========================================================================
  # 3. Test exact timestamp deduplication when WndwDedup is NULL
  # =========================================================================
  tempTestDirExact <- file.path(tempdir(), "test_gap_fill_exact", "pfs", "test_repo", "sensor", "2025", "06", "23", "CFGLOC123456")
  dir.create(file.path(tempTestDirExact, "data"), recursive = TRUE)
  
  exact_times <- c(
    base_time + 10,
    base_time + 10,  # Exact duplicate
    base_time + 60
  )
  exact_df <- data.frame(
    source_id = rep("12345", length(exact_times)),
    readout_time = exact_times,
    value = c(1.0, 2.0, 3.0)
  )
  NEONprocIS.base::def.wrte.parq(data = exact_df, NameFile = file.path(tempTestDirExact, "data", "sensor_CFGLOC123456_2025-06-23.parquet"))
  
  tempOutBaseExact <- file.path(tempdir(), "test_gap_fill_exact_out")
  if (dir.exists(tempOutBaseExact)) unlink(tempOutBaseExact, recursive = TRUE)
  
  wrap.gap.fill.nonrglr(
    DirIn = tempTestDirExact,
    DirOutBase = tempOutBaseExact,
    DirFill = "data",
    WndwFill = 15,
    WndwDedup = NULL,
    SchmFill = NA,
    log = log
  )
  
  outExactFile <- file.path(tempOutBaseExact, "sensor", "2025", "06", "23", "CFGLOC123456", "data", "sensor_CFGLOC123456_2025-06-23.parquet")
  testthat::expect_true(file.exists(outExactFile))
  df_exact <- NEONprocIS.base::def.read.parq(NameFile = outExactFile)
  testthat::expect_equal(anyDuplicated(df_exact$readout_time), 0)
  # Kept first duplicate (value = 1.0)
  dup_rec <- df_exact[df_exact$readout_time == (base_time + 10), ]
  testthat::expect_equal(nrow(dup_rec), 1)
  testthat::expect_equal(dup_rec$value, 1.0)
  
  unlink(file.path(tempdir(), "test_gap_fill_exact"), recursive = TRUE)
  unlink(tempOutBaseExact, recursive = TRUE)
  
  # =========================================================================
  # 4. Test schema fallback when SchmFill is NA and NULL
  # =========================================================================
  if (dir.exists(DirOutBase)) unlink(DirOutBase, recursive = TRUE)
  
  # Test with SchmFill = NA
  wrap.gap.fill.nonrglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirFill = "data",
    WndwFill = WndwFill,
    SchmFill = NA,
    log = log
  )
  testthat::expect_true(file.exists(file.path(dirOutData, fileData)))
  unlink(DirOutBase, recursive = TRUE)
  
  # Test with SchmFill = NULL
  wrap.gap.fill.nonrglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirFill = "data",
    WndwFill = WndwFill,
    SchmFill = NULL,
    log = log
  )
  testthat::expect_true(file.exists(file.path(dirOutData, fileData)))
  unlink(DirOutBase, recursive = TRUE)
  
  # =========================================================================
  # 5. Test source_id fallback to "99999" when all input source_ids are NA
  # =========================================================================
  tempTestDirNA <- file.path(tempdir(), "test_gap_fill_na", "pfs", "test_repo", "sensor", "2025", "06", "23", "CFGLOC123456")
  dir.create(file.path(tempTestDirNA, "data"), recursive = TRUE)
  
  na_df <- data.frame(
    source_id = as.character(c(NA, NA)),
    readout_time = c(base_time + 60, base_time + 120),
    value = c(1.0, 2.0)
  )
  NEONprocIS.base::def.wrte.parq(data = na_df, NameFile = file.path(tempTestDirNA, "data", "sensor_CFGLOC123456_2025-06-23.parquet"))
  
  tempOutBaseNA <- file.path(tempdir(), "test_gap_fill_na_out")
  if (dir.exists(tempOutBaseNA)) unlink(tempOutBaseNA, recursive = TRUE)
  
  wrap.gap.fill.nonrglr(
    DirIn = tempTestDirNA,
    DirOutBase = tempOutBaseNA,
    DirFill = "data",
    WndwFill = 15,
    SchmFill = NA,
    log = log
  )
  
  outNAFile <- file.path(tempOutBaseNA, "sensor", "2025", "06", "23", "CFGLOC123456", "data", "sensor_CFGLOC123456_2025-06-23.parquet")
  testthat::expect_true(file.exists(outNAFile))
  df_na <- NEONprocIS.base::def.read.parq(NameFile = outNAFile)
  testthat::expect_true(all(df_na$source_id == "99999"))
  
  unlink(file.path(tempdir(), "test_gap_fill_na"), recursive = TRUE)
  unlink(tempOutBaseNA, recursive = TRUE)
  
  # =========================================================================
  # 6. Error handling and edge cases
  # =========================================================================
  # Case A: No files in input directory
  DirIn_nofiles <- "pfs/sunav2_location_group_and_restructure/sunav2/2025/06/24/CFGLOCEMPTY"
  returnedOutput <- try(wrap.gap.fill.nonrglr(
    DirIn = DirIn_nofiles,
    DirOutBase = DirOutBase,
    DirFill = DirFill,
    WndwFill = WndwFill,
    SchmFill = SchmFill
  ), silent = TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  # Case B: File missing readout_time column
  tempTestDirNoTime <- file.path(tempdir(), "test_gap_fill_notime", "pfs", "test_repo", "sensor", "2025", "06", "23", "CFGLOC123456")
  dir.create(file.path(tempTestDirNoTime, "data"), recursive = TRUE)
  notime_df <- data.frame(
    source_id = c("12345", "12345"),
    value = c(1.0, 2.0)
  )
  NEONprocIS.base::def.wrte.parq(data = notime_df, NameFile = file.path(tempTestDirNoTime, "data", "sensor_CFGLOC123456_2025-06-23.parquet"))
  
  tempOutBaseNoTime <- file.path(tempdir(), "test_gap_fill_notime_out")
  returnedOutputNoTime <- try(wrap.gap.fill.nonrglr(
    DirIn = tempTestDirNoTime,
    DirOutBase = tempOutBaseNoTime,
    DirFill = "data",
    WndwFill = 15,
    SchmFill = NA
  ), silent = TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutputNoTime))
  
  unlink(file.path(tempdir(), "test_gap_fill_notime"), recursive = TRUE)
  unlink(tempOutBaseNoTime, recursive = TRUE)
  
  # Case C: Unreadable / corrupt file
  tempTestDirCorrupt <- file.path(tempdir(), "test_gap_fill_corrupt", "pfs", "test_repo", "sensor", "2025", "06", "23", "CFGLOC123456")
  dir.create(file.path(tempTestDirCorrupt, "data"), recursive = TRUE)
  writeLines("This is not a parquet file", file.path(tempTestDirCorrupt, "data", "corrupt.parquet"))
  
  tempOutBaseCorrupt <- file.path(tempdir(), "test_gap_fill_corrupt_out")
  returnedOutputCorrupt <- try(wrap.gap.fill.nonrglr(
    DirIn = tempTestDirCorrupt,
    DirOutBase = tempOutBaseCorrupt,
    DirFill = "data",
    WndwFill = 15,
    SchmFill = NA
  ), silent = TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutputCorrupt))
  
  unlink(file.path(tempdir(), "test_gap_fill_corrupt"), recursive = TRUE)
  unlink(tempOutBaseCorrupt, recursive = TRUE)
})
