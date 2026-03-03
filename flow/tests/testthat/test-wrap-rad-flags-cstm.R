##############################################################################################
#' @title Unit test for Wrapper for Custom Radiation Flags Module
#' 
#' @description Wrapper function. Applies custom flags to radiation sensor data including 
#' shadow detection and CMP22 heater flags.
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmQf (optional) A json-formatted character string containing the schema for the flags output
#' by this function. If this input is not provided, the output schema for the flags will be generated automatically.
#' 
#' @param FlagsRad (Optional). A list of flags to run. If not provided it will bypass script without producing any flags. 
#' Options include: "Shadow", "Cmp22Heater"
#' 
#' @param termTest (Optional). terms to run for shading flag. If NULL and Shadow is in FlagsRad will result in errored datum.
#' 
#' @param shadowSource (Optional). Which type of shadow is expected. Options include LR Cimel Misc to distinguish between 
#' different types of shading sources from different directions. If not supplied, but shadow check is run script will fail.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Custom radiation quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' The 'flags' directory is automatically populated in the output directory. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.rad.flags.cstm(DirIn="~/pfs/radShortPrimary_analyze_pad_and_qaqc_plau/2025/03/31/CFGLOC100001",
#'                     DirOutBase="~/pfs/out",
#'                     FlagsRad=c("Cmp22Heater"),
#'                     DirSubCopy=NULL,
#'                     log=log)
#'                               
# changelog and author contributions 
#   Teresa Burlingame (2025-09-15)
#     Original Creation
##############################################################################################

# Load required libraries
library(testthat)
library(data.table)
library(arrow)
library(jsonlite)

# Source the functions under test
# Use relative path from test directory - works in both CI and local
flow_dir <- "../../flow.rad.flags.cstm/"

source(file.path(flow_dir, "wrap.rad.flags.cstm.R"))
source(file.path(flow_dir, "def.rad.shadow.flags.R"))
source(file.path(flow_dir, "def.cmp22.heater.flags.R"))

# Unit test of wrap.rad.flags.cstm.R
test_that("Unit test of wrap.rad.flags.cstm.R", {
  
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Get working directory
  workingDirPath <- getwd()
  
  # Test 1: No custom flags specified - should pass through without creating flag
  
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  testDirOut <- file.path(tempdir(), "test_output")
  
  # Clean up any previous test output
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  # Note: If test data doesn't exist, this test will be skipped
  # You'll need to create test data in: pfs/radShortPrimary_analyze_pad_and_qaqc_plau/2025/03/31/CFGLOC100001
  # with subdirectories: /data and /flags containing appropriate parquet files
  
  if (dir.exists(testDirIn)) {
    wrap.rad.flags.cstm(DirIn=testDirIn,
                        DirOutBase=testDirOut,
                        FlagsRad=NULL,
                        DirSubCopy=NULL,
                        log=log)
    
    # When no flags are specified, output should still be created (pass-through)
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
    testDirRepo <- InfoDirIn$dirRepo
    testDirOutPath <- base::paste0(testDirOut, testDirRepo)
    
    # Since no flags were specified, function returns early, so output may not exist
    # This is expected behavior
    expect_true(TRUE)  # Test passes if function completes without error
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
  
  # Test 2: CMP22 Heater flag specified
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  testDirInCmp22 <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  testDirOut <- file.path(tempdir(), "test_output")
  
  if (dir.exists(testDirInCmp22)) {
    wrap.rad.flags.cstm(DirIn=testDirInCmp22,
                        DirOutBase=testDirOut,
                        FlagsRad=c("Cmp22Heater"),
                        DirSubCopy=NULL,
                        log=log)
    
    InfoDirInCmp22 <- NEONprocIS.base::def.dir.splt.pach.time(testDirInCmp22)
    testDirRepoCmp22 <- InfoDirInCmp22$dirRepo
    testDirOutPathCmp22 <- base::paste0(testDirOut, testDirRepoCmp22)
    
    expect_true(file.exists(testDirOutPathCmp22, recursive = TRUE))
    
    # Check that flags directory exists
    flagsDir <- file.path(testDirOutPathCmp22, 'flags')
    expect_true(dir.exists(flagsDir))
    
    # Check that custom flags file was created
    flagFiles <- list.files(flagsDir, pattern = "customFlags.parquet", full.names = FALSE)
    expect_true(length(flagFiles) > 0)
  } else {
    skip(paste0("Test data directory does not exist: ", testDirInCmp22))
  }
  
  # Test 3: Shadow flag with termTest and shadowSource specified
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  testDirInShadow <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirInShadow)) {
    wrap.rad.flags.cstm(DirIn=testDirInShadow,
                        DirOutBase=testDirOut,
                        FlagsRad=c("Shadow"),
                        termTest="shortwaveRadiation",
                        shadowSource="LR",
                        DirSubCopy=NULL,
                        log=log)
    
    InfoDirInShadow <- NEONprocIS.base::def.dir.splt.pach.time(testDirInShadow)
    testDirRepoShadow <- InfoDirInShadow$dirRepo
    testDirOutPathShadow <- base::paste0(testDirOut, testDirRepoShadow)
    
    expect_true(file.exists(testDirOutPathShadow, recursive = TRUE))
    
    # Check that flags directory exists
    flagsDir <- file.path(testDirOutPathShadow, 'flags')
    expect_true(dir.exists(flagsDir))
    
    # Check that custom flags file was created
    flagFiles <- list.files(flagsDir, pattern = "customFlags.parquet", full.names = FALSE)
    expect_true(length(flagFiles) > 0)
  } else {
    skip(paste0("Test data directory does not exist: ", testDirInShadow))
  }
  
  # Test 4: Multiple flags specified (both Shadow and Cmp22Heater)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  testDirInMulti <-file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirInMulti)) {
    wrap.rad.flags.cstm(DirIn=testDirInMulti,
                        DirOutBase=testDirOut,
                        FlagsRad=c("Shadow", "Cmp22Heater"),
                        termTest="shortwaveRadiation",
                        shadowSource="LR",
                        DirSubCopy=NULL,
                        log=log)
    
    InfoDirInMulti <- NEONprocIS.base::def.dir.splt.pach.time(testDirInMulti)
    testDirRepoMulti <- InfoDirInMulti$dirRepo
    testDirOutPathMulti <- base::paste0(testDirOut, testDirRepoMulti)
    
    expect_true(file.exists(testDirOutPathMulti, recursive = TRUE))
    
    # Check that flags directory exists
    flagsDir <- file.path(testDirOutPathMulti, 'flags')
    expect_true(dir.exists(flagsDir))
    
    # Check that custom flags file was created
    flagFiles <- list.files(flagsDir, pattern = "customFlags.parquet", full.names = FALSE)
    expect_true(length(flagFiles) > 0)
  } else {
    skip(paste0("Test data directory does not exist: ", testDirInMulti))
  }
})


##############################################################################################
# Unit tests for individual def functions
##############################################################################################

# Test def.cmp22.heater.flags
test_that("Unit test of def.cmp22.heater.flags.R", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Test 1: Both heaters off - should return 0
  test_data <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00", "2026-02-20 12:02:00"), tz = "UTC"),
    heater_1 = c(0, 0, 0),
    heater_2 = c(0, 0, 0)
  )
  
  test_flagDf <- data.frame(
    readout_time = test_data$readout_time
  )
  
  result <- def.cmp22.heater.flags(data = test_data, flagDf = test_flagDf, log = log)
  
  expect_true("heaterQF" %in% names(result))
  expect_equal(result$heaterQF, c(0, 0, 0))
  
  # Test 2: Heater 1 on - should return 1
  test_data2 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00"), tz = "UTC"),
    heater_1 = c(1, 0),
    heater_2 = c(0, 0)
  )
  
  test_flagDf2 <- data.frame(
    readout_time = test_data2$readout_time
  )
  
  result2 <- def.cmp22.heater.flags(data = test_data2, flagDf = test_flagDf2, log = log)
  
  expect_equal(result2$heaterQF, c(1, 0))
  
  # Test 3: Heater 2 on - should return 1
  test_data3 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00"), tz = "UTC"),
    heater_1 = c(0, 0),
    heater_2 = c(1, 0)
  )
  
  test_flagDf3 <- data.frame(
    readout_time = test_data3$readout_time
  )
  
  result3 <- def.cmp22.heater.flags(data = test_data3, flagDf = test_flagDf3, log = log)
  
  expect_equal(result3$heaterQF, c(1, 0))
  
  # Test 4: Both heaters NA - should return -1
  test_data4 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00"), tz = "UTC"),
    heater_1 = c(NA, NA),
    heater_2 = c(NA, NA)
  )
  
  test_flagDf4 <- data.frame(
    readout_time = test_data4$readout_time
  )
  
  result4 <- def.cmp22.heater.flags(data = test_data4, flagDf = test_flagDf4, log = log)
  
  expect_equal(result4$heaterQF, c(-1, -1))
  
  # Test 5: Both heaters on - should return 1
  test_data5 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00"), tz = "UTC"),
    heater_1 = c(1),
    heater_2 = c(1)
  )
  
  test_flagDf5 <- data.frame(
    readout_time = test_data5$readout_time
  )
  
  result5 <- def.cmp22.heater.flags(data = test_data5, flagDf = test_flagDf5, log = log)
  
  expect_equal(result5$heaterQF, c(1))
  
  # Test 6: Missing heater_1 column - should warn and add NA column, result in -1
  test_data6 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00"), tz = "UTC"),
    heater_2 = c(0)
  )
  
  test_flagDf6 <- data.frame(
    readout_time = test_data6$readout_time
  )
  
  result6 <- def.cmp22.heater.flags(data = test_data6, flagDf = test_flagDf6, log = log)
  
  expect_equal(result6$heaterQF, c(-1))
  
  # Test 7: Missing both heater columns - should warn and add NA columns, result in -1
  test_data7 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00"), tz = "UTC"),
    some_other_col = c(100)
  )
  
  test_flagDf7 <- data.frame(
    readout_time = test_data7$readout_time
  )
  
  result7 <- def.cmp22.heater.flags(data = test_data7, flagDf = test_flagDf7, log = log)
  
  expect_equal(result7$heaterQF, c(-1))
  
  # Test 8: Mixed values - one heater NA, one on
  test_data8 <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00"), tz = "UTC"),
    heater_1 = c(NA, 0),
    heater_2 = c(1, NA)
  )
  
  test_flagDf8 <- data.frame(
    readout_time = test_data8$readout_time
  )
  
  result8 <- def.cmp22.heater.flags(data = test_data8, flagDf = test_flagDf8, log = log)
  
  expect_equal(result8$heaterQF, c(1, -1)) # First: heater_2 on, Second: one NA one off
})

# Test def.rad.shadow.flags - Basic error handling and structure
test_that("Unit test of def.rad.shadow.flags.R - error handling", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Test 1: Missing readout_time in flagDf - should error
  test_flagDf_bad <- data.frame(
    timestamp = as.POSIXct(c("2026-02-20 12:00:00"), tz = "UTC")
  )
  
  expect_error(
    def.rad.shadow.flags(
      DirIn = "dummy/path",
      flagDf = test_flagDf_bad,
      termTest = "shortwaveRadiation",
      shadowSource = "LR",
      log = log
    )
  )
  
  # Test 2: NULL termTest - should handle gracefully
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c("2026-02-20 12:00:00", "2026-02-20 12:01:00"), tz = "UTC")
  )
  
  # This should handle the NULL termTest and stop execution
  # We'll skip this if the test directories don't exist
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirIn)) {
    expect_error(
      def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = NULL,
        shadowSource = "LR",
        log = log
      )
    )
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
  
  # Test 3: NULL shadowSource - should handle gracefully
  if (dir.exists(testDirIn)) {
    expect_error(
      def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = NULL,
        log = log
      )
    )
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})

# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with valid test data", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c(
      "2026-02-20 12:00:00",
      "2026-02-20 13:00:00",
      "2026-02-20 14:00:00"
    ), tz = "UTC")
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "LR",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (0, 1, or -1)
      expect_true(all(result$shadowQF %in% c(-1, 0, 1)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})

# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with valid test but missing shadow source", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c(
      "2026-02-20 12:00:00",
      "2026-02-20 13:00:00",
      "2026-02-20 14:00:00"
    ), tz = "UTC")
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "sdfa",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (-1)
      expect_true(all(result$shadowQF %in% c(-1)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})


# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with valid test but North Azimuth", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c(
      "2026-02-20 12:00:00",
      "2026-02-20 13:00:00",
      "2026-02-20 14:00:00"
    ), tz = "UTC")
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/NorthAzrad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "LR",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (-1)
      expect_true(all(result$shadowQF %in% c(-1, 0, 1)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})



# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with valid data but no shadows", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c(
      "2026-02-20 12:00:00",
      "2026-02-20 13:00:00",
      "2026-02-20 14:00:00"
    ), tz = "UTC")
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/rad-short-primary_HARV000060/cmp22/CFGLOC100487/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "LR",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (-1)
      expect_true(all(result$shadowQF %in% c( 0)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})
# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with missing locations", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf <- data.frame(
    readout_time = as.POSIXct(c(
      "2026-02-20 12:00:00",
      "2026-02-20 13:00:00",
      "2026-02-20 14:00:00"
    ), tz = "UTC")
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/2026/02/20/NoLocrad-short-primary_CLBJ000050/cmp22/CFGLOC104968/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "LR",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (0, 1, or -1)
      expect_true(all(result$shadowQF %in% c(-1)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})

# Test def.rad.shadow.flags with valid data
test_that("Unit test of def.rad.shadow.flags.R - with data that will flag (modified thresholds/locations)", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Create test data with timestamps
  test_flagDf  <- data.frame(
    readout_time = seq(
      from = as.POSIXct("2026-10-20 21:00:00", tz = "UTC"),
      to   = as.POSIXct("2026-10-20 21:58:00", tz = "UTC"),
      by   = "2 min"
    )
  )
  
  # Test with actual test data directory if it exists
  testDirIn <- file.path(getwd(), "pfs/radShortPrimary_custom_flags/will_flag/2026/02/20/rad-short-primary_BONA000050/cmp22/CFGLOC111996/")
  
  if (dir.exists(testDirIn)) {
    # Check if necessary subdirectories exist
    if (dir.exists(file.path(testDirIn, "location")) && 
        dir.exists(file.path(testDirIn, "threshold"))) {
      
      result <- def.rad.shadow.flags(
        DirIn = testDirIn,
        flagDf = test_flagDf,
        termTest = "shortwaveRadiation",
        shadowSource = "LR",
        log = log
      )
      
      # Check that shadowQF column was added
      expect_true("shadowQF" %in% names(result))
      
      # Check that shadowQF values are valid (0, 1, or -1)
      expect_true(all(result$shadowQF %in% c(0,1)))
      
      # Check that original columns are preserved
      expect_true("readout_time" %in% names(result))
      
      # Check that we have the same number of rows
      expect_equal(nrow(result), nrow(test_flagDf))
      
    } else {
      skip("Test data missing required subdirectories (location/threshold)")
    }
  } else {
    skip(paste0("Test data directory does not exist: ", testDirIn))
  }
})

# Test edge cases for both functions
test_that("Unit test of edge cases for custom flag functions", {
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Edge case 1: Empty data frame for heater flags
  empty_data <- data.frame(
    readout_time = as.POSIXct(character(0), tz = "UTC"),
    heater_1 = numeric(0),
    heater_2 = numeric(0)
  )
  
  empty_flagDf <- data.frame(
    readout_time = as.POSIXct(character(0), tz = "UTC")
  )
  
  expect_error(def.cmp22.heater.flags(data = empty_data, flagDf = empty_flagDf, log = log))
  
  
  # Edge case 2: Single row with all zeros
  single_data <- data.frame(
    readout_time = as.POSIXct("2026-02-20 12:00:00", tz = "UTC"),
    heater_1 = 0,
    heater_2 = 0
  )
  
  single_flagDf <- data.frame(
    readout_time = single_data$readout_time
  )
  
  result_single <- def.cmp22.heater.flags(data = single_data, flagDf = single_flagDf, log = log)
  
  expect_equal(nrow(result_single), 1)
  expect_equal(result_single$heaterQF, 0)
  
  # Edge case 3: Large dataset with varying heater states
  n_rows <- 100
  large_data <- data.frame(
    readout_time = seq(
      as.POSIXct("2026-02-20 00:00:00", tz = "UTC"),
      by = "1 min",
      length.out = n_rows
    ),
    heater_1 = sample(c(0, 1, NA), n_rows, replace = TRUE),
    heater_2 = sample(c(0, 1, NA), n_rows, replace = TRUE)
  )
  
  large_flagDf <- data.frame(
    readout_time = large_data$readout_time
  )
  
  result_large <- def.cmp22.heater.flags(data = large_data, flagDf = large_flagDf, log = log)
  
  expect_equal(nrow(result_large), n_rows)
  expect_true(all(result_large$heaterQF %in% c(-1, 0, 1)))
  
  # Verify logic: if either heater is 1, flag should be 1
  for (i in 1:n_rows) {
    if (!is.na(large_data$heater_1[i]) && large_data$heater_1[i] == 1) {
      expect_equal(result_large$heaterQF[i], 1)
    }
    if (!is.na(large_data$heater_2[i]) && large_data$heater_2[i] == 1) {
      expect_equal(result_large$heaterQF[i], 1)
    }
    if (isTRUE(large_data$heater_1[i] == 0 && large_data$heater_2[i] == 0)) {
      expect_equal(result_large$heaterQF[i], 0)
    }
    if (is.na(large_data$heater_1[i]) && is.na(large_data$heater_2[i])) {
      expect_equal(result_large$heaterQF[i], -1)
    }
  }
})
