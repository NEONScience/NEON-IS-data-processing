########################################################################################################
#' @title Unit tests for temperature flag functions

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description Comprehensive unit tests for the envscn temperature flags workflow,
#' including helper functions def.load.temp.sensors, def.find.temp.sensor, 
#' def.calc.temp.flags, def.apply.temp.flags, def.sort.qf.cols, and the main 
#' wrap.envscn.temp.flags function.

########################################################################################################

# Define test context

# Load required libraries
library(testthat)
library(data.table)
library(arrow)
library(jsonlite)

# uncomment if running directly, change dirs as needed
# Source the functions under test
# setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")
# 
# flow_dir <- "~/GitHub/NEON-IS-data-processing/flow/flow.envscn.temp.flags/"
# 
# source(file.path(flow_dir, "def.load.temp.sensors.R"))
# source(file.path(flow_dir, "def.find.temp.sensor.R"))
# source(file.path(flow_dir, "def.calc.temp.flags.R"))
# source(file.path(flow_dir, "def.apply.temp.flags.R"))
# source(file.path(flow_dir, "def.sort.qf.cols.R"))
# source(file.path(flow_dir, "wrap.envscn.temp.flags.R"))

########################################################################################################
# Test 1: def.sort.qf.cols
########################################################################################################
test_that("Test def.sort.qf.cols sorts columns correctly", {
  # Test input with mixed order
  cols <- c("tempTestDepth03QF", "readout_time", "depth01SoilMoisturePlausibilityQF",
            "tempTestDepth01QF", "depth02SoilMoisturePlausibilityQF", 
            "tempTestDepth02QF")
  
  sorted <- def.sort.qf.cols(cols)
  
  # Check that static columns come first
  expect_equal(sorted[1], c("readout_time"))
  
  # Check that tempTest columns are sorted numerically
  tempCols <- sorted[grepl("tempTest", sorted)]
  expect_equal(tempCols, c("tempTestDepth01QF", "tempTestDepth02QF", "tempTestDepth03QF"))
  
  # Check that all columns are present
  expect_equal(length(sorted), length(cols))
  expect_true(all(cols %in% sorted))
})

test_that("Test def.sort.qf.cols handles empty input", {
  expect_equal(def.sort.qf.cols(character(0)), character(0))
})

test_that("Test def.sort.qf.cols handles columns without depth numbers", {
  cols <- c("readout_time", "genericQF", "someOtherColumnQF")
  sorted <- def.sort.qf.cols(cols)
  
  # readout_time should be first
  expect_equal(sorted[1], "readout_time")
  # Other columns should be present
  expect_equal(length(sorted), length(cols))
})

########################################################################################################
# Test 2: def.find.temp.sensor
########################################################################################################
test_that("Test def.find.temp.sensor finds closest sensor", {
  # Create test sensor data
  sensorDf <- data.frame(
    sensor_id = c("temp-soil_SITE001", "temp-soil_SITE002", "temp-soil_SITE003"),
    depth_m = c(-0.10, -0.30, -0.50),
    data_path = c("/path/to/data1.parquet", "/path/to/data2.parquet", "/path/to/data3.parquet"),
    location_path = c("/path/to/loc1.json", "/path/to/loc2.json", "/path/to/loc3.json"),
    stringsAsFactors = FALSE
  )
  
  # Test finding sensor closest to -0.25m
  result <- def.find.temp.sensor(targetDepth = -0.25, sensorDepthDf = sensorDf, log = NULL)
  
  expect_equal(result$closest$sensor_id, "temp-soil_SITE002")
  expect_equal(result$closest$depth_m, -0.30)
  
  # Check neighbors
  expect_equal(result$neighbors$higher$sensor_id, "temp-soil_SITE001")  # -0.10 is higher (less negative)
  expect_equal(result$neighbors$lower$sensor_id, "temp-soil_SITE003")   # -0.50 is lower (more negative)
})

test_that("Test def.find.temp.sensor with no valid temperature depths", {
  # Create test sensor data
  sensorDf <- data.frame(
    sensor_id = c("temp-soil_SITE001", "temp-soil_SITE002", "temp-soil_SITE003"),
    depth_m = c(NA,NA,NA),
    data_path = c("/path/to/data1.parquet", "/path/to/data2.parquet", "/path/to/data3.parquet"),
    location_path = c("/path/to/loc1.json", "/path/to/loc2.json", "/path/to/loc3.json"),
    stringsAsFactors = FALSE
  )
  
  # Test finding sensor closest to -0.25m
  result <- def.find.temp.sensor(targetDepth = -0.25, sensorDepthDf = sensorDf, log = NULL)
  
  expect_null(result)
  
})


test_that("Test def.find.temp.sensor handles tie with shallower preference", {
  # Create sensors equidistant from target
  sensorDf <- data.frame(
    sensor_id = c("temp-soil_SITE001", "temp-soil_SITE002"),
    depth_m = c(-0.20, -0.40),
    data_path = c("/path/to/data1.parquet", "/path/to/data2.parquet"),
    location_path = c("/path/to/loc1.json", "/path/to/loc2.json"),
    stringsAsFactors = FALSE
  )
  
  # Target at -0.30 is equidistant from both (-0.20 and -0.40)
  result <- def.find.temp.sensor(targetDepth = -0.30, sensorDepthDf = sensorDf, log = NULL)
  
  # Should prefer the shallower sensor (-0.20 is less negative)
  expect_equal(result$closest$sensor_id, "temp-soil_SITE001")
  expect_equal(result$closest$depth_m, -0.20)
})

test_that("Test def.find.temp.sensor handles edge sensors without neighbors", {
  sensorDf <- data.frame(
    sensor_id = c("temp-soil_SITE001", "temp-soil_SITE002", "temp-soil_SITE003"),
    depth_m = c(-0.10, -0.30, -0.50),
    data_path = c("/path/to/data1.parquet", "/path/to/data2.parquet", "/path/to/data3.parquet"),
    location_path = c("/path/to/loc1.json", "/path/to/loc2.json", "/path/to/loc3.json"),
    stringsAsFactors = FALSE
  )
  
  # Test shallowest sensor
  result <- def.find.temp.sensor(targetDepth = -0.05, sensorDepthDf = sensorDf, log = NULL)
  expect_equal(result$closest$sensor_id, "temp-soil_SITE001")
  expect_null(result$neighbors$higher)  # No shallower sensor
  expect_equal(result$neighbors$lower$sensor_id, "temp-soil_SITE002")
  
  # Test deepest sensor
  result <- def.find.temp.sensor(targetDepth = -0.60, sensorDepthDf = sensorDf, log = NULL)
  expect_equal(result$closest$sensor_id, "temp-soil_SITE003")
  expect_equal(result$neighbors$higher$sensor_id, "temp-soil_SITE002")
  expect_null(result$neighbors$lower)  # No deeper sensor
})

test_that("Test def.find.temp.sensor filters NA depths", {
  sensorDf <- data.frame(
    sensor_id = c("temp-soil_SITE001", "temp-soil_SITE002", "temp-soil_SITE003"),
    depth_m = c(-0.10, NA, -0.50),
    data_path = c("/path/to/data1.parquet", "/path/to/data2.parquet", "/path/to/data3.parquet"),
    location_path = c("/path/to/loc1.json", "/path/to/loc2.json", "/path/to/loc3.json"),
    stringsAsFactors = FALSE
  )
  
  # Should ignore sensor with NA depth
  result <- def.find.temp.sensor(targetDepth = -0.30, sensorDepthDf = sensorDf, log = NULL)
  
  # Should choose between the two valid sensors
  expect_true(result$closest$sensor_id %in% c("temp-soil_SITE001", "temp-soil_SITE003"))
  expect_false(is.na(result$closest$depth_m))
})

########################################################################################################
# Test 3: def.load.temp.sensors
########################################################################################################
test_that("Test def.load.temp.sensors with test data", {
  # Use the actual test data
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/multiple_depth/2025/10/18/conc-h2o-soil-salinity_GRSM005501")
  
  # Only run this test if the test data exists
  if (dir.exists(DirTemp)) {
    result <- def.load.temp.sensors(DirTemp = DirTemp, log = NULL)
    
    # Check that we got a data frame back
    expect_s3_class(result, "data.frame")
    
    # Check required columns exist
    expect_true(all(c("sensor_id", "depth_m", "data_path", "location_path") %in% names(result)))
    
    # Check that sensor_ids contain the expected prefix
    if (nrow(result) > 0) {
      expect_true(all(grepl("^temp-soil_", result$sensor_id)))
      
      # Check that depths are numeric and negative (below surface)
      expect_true(all(is.numeric(result$depth_m)))
      expect_true(all(result$depth_m <= 0, na.rm = TRUE))
      
      # Check that paths exist
      expect_true(all(file.exists(result$data_path)))
      expect_true(all(file.exists(result$location_path)))
    }
  } else {
    skip("Test data not available")
  }
})

# Test with empty directory (no temperature sensor data)
test_that("Test def.load.temp.sensors with no data", {
  # Use fake location
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/2025/10/14/")
  
  # Only run this test if the test data exists
  
  result <- def.load.temp.sensors(DirTemp = DirTemp, log = NULL)
  
  # Check that we got a data frame back
  expect_equal(nrow(result), 0)
  
})

# Test with multiple location files for same sensor (should average depths)
test_that("Test def.load.temp.sensors with more than one depth", {
  # Use the actual test data
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/multiple_depth/2025/10/18/conc-h2o-soil-salinity_GRSM005501/")
  
  # Only run this test if the test data exists
  if (dir.exists(DirTemp)) {
    result <- def.load.temp.sensors(DirTemp = DirTemp, log = NULL)
    # Should average multiple depths for same sensor
    expect_equal(result$depth_m[result$sensor_id=="temp-soil_GRSM005501"], mean(c(-.005, -.009)))
  } else {
    skip("Test data not available")
  }
})

# Test with no location files 
test_that("Test def.load.temp.sensors no location data found", {
  # Use the test data
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/no_locations/2025/10/17/conc-h2o-soil-salinity_GRSM005501/")
  
  # Only run this test if the test data exists
  if (dir.exists(DirTemp)) {
    result <- def.load.temp.sensors(DirTemp = DirTemp, log = NULL)
    # Should return empty data frame
    expect_equal(nrow(result), 0)
  } else {
    skip("Test data not available")
  }
})

# Test with missing z_offset in location file (should result in NA depth)
test_that("Test def.load.temp.sensors with missing z_offset", {
  # Use the actual test data
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/no_z_offset/2025/10/17/conc-h2o-soil-salinity_GRSM005501/")
  # Only run this test if the test data exists
  if (dir.exists(DirTemp)) {
    result <- def.load.temp.sensors(DirTemp = DirTemp, log = NULL)
    # Should have NA in depth_m column
    expect_true(any(is.na(result$depth_m)))
  } else {
    skip("Test data not available")
  }
})


########################################################################################################
# Test 4: def.calc.temp.flags
########################################################################################################
test_that("Test def.calc.temp.flags basic functionality", {
  # Create temp directory for test data
  tmpDir <- tempdir()
  testDataPath <- file.path(tmpDir, "test_temp_data.parquet")
  
  # Create test temperature data
  testTempData <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00", 
                                 "2025-10-17 00:02:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00", 
                               "2025-10-17 00:03:00"), tz = "UTC"),
    soilTempMean = c(5.0, -2.0, 3.0),           # Temperature values
    soilTempExpUncert = c(1.0, 1.0, 1.0),       # Uncertainty values
    finalQF = c(0L, 0L, 0L)                      # All good data
  )
  
  # Write test data
  arrow::write_parquet(testTempData, testDataPath)
  
  # Create sensor info
  sensorInfo <- list(
    closest = data.frame(
      sensor_id = "temp-soil_TEST001",
      depth_m = -0.20,
      data_path = testDataPath,
      location_path = "/fake/path.json",
      stringsAsFactors = FALSE
    ),
    neighbors = list(
      higher = NULL,
      lower = NULL
    )
  )
  
  # Calculate flags
  result <- def.calc.temp.flags(sensorInfo = sensorInfo, log = NULL)
  
  # Check structure
  expect_true(all(c("startDateTime", "endDateTime", "temp_flag") %in% names(result)))
  expect_equal(nrow(result), 3)
  
  # Check flag logic: temp_flag = 1 if soilTempMean < soilTempExpUncert
  # Row 1: 5.0 < 1.0 = FALSE -> 0
  # Row 2: -2.0 < 1.0 = TRUE -> 1
  # Row 3: 3.0 < 1.0 = FALSE -> 0
  expect_equal(result$temp_flag, c(0L, 1L, 0L))
  
  # Cleanup
  unlink(testDataPath)
})

test_that("Test def.calc.temp.flags neighbor too far", {
  # Create temp directory for test data
  tmpDir <- tempdir()
  testDataPath1 <- file.path(tmpDir, "test_temp_data1.parquet")
  testDataPath2 <- file.path(tmpDir, "test_temp_data2.parquet")
  testDataPath3 <- file.path(tmpDir, "test_temp_data3.parquet")
  
  # Create test temperature data with some flagged intervals
  testTempData1 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(5.0, -2.0),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(1L, 1L)  # Both intervals are flagged
  )
  
  # Neighbor sensors with good data
  testTempData2 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(4.5, 0.5),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(0L, 0L)  # All good
  )
  
  testTempData3 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(4.8, 0.3),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(0L, 0L)  # All good
  )
  
  # Write test data
  arrow::write_parquet(testTempData1, testDataPath1)
  arrow::write_parquet(testTempData2, testDataPath2)
  arrow::write_parquet(testTempData3, testDataPath3)
  
  # Create sensor info with neighbors
  sensorInfo <- list(
    closest = data.frame(
      sensor_id = "temp-soil_TEST001",
      depth_m = -0.20,
      data_path = testDataPath1,
      location_path = "/fake/path1.json",
      stringsAsFactors = FALSE
    ),
    neighbors = list(
      higher = data.frame(
        sensor_id = "temp-soil_TEST002",
        depth_m = -0.10,
        data_path = testDataPath2,
        location_path = "/fake/path2.json",
        stringsAsFactors = FALSE
      ),
      lower = data.frame(
        sensor_id = "temp-soil_TEST003",
        depth_m = -1.5,
        data_path = testDataPath3,
        location_path = "/fake/path3.json",
        stringsAsFactors = FALSE
      )
    )
  )
  
  # Calculate flags with distance check
  result <- def.calc.temp.flags(
    sensorInfo = sensorInfo,
    targetDepth = -0.20,
    distThreshold = 0.15,
    log = NULL
  )
  
  # Check structure
  expect_equal(nrow(result), 2)
  
  # First interval: primary sensor flagged and neighbor too far (>0.15m)
  expect_equal(result$temp_flag[1], -1L)  
  
  
  # Cleanup
  unlink(c(testDataPath1, testDataPath2, testDataPath3))
})


test_that("Test def.calc.temp.flags with flagged primary sensor", {
  # Create temp directory for test data
  tmpDir <- tempdir()
  testDataPath1 <- file.path(tmpDir, "test_temp_data1.parquet")
  testDataPath2 <- file.path(tmpDir, "test_temp_data2.parquet")
  testDataPath3 <- file.path(tmpDir, "test_temp_data3.parquet")
  
  # Create test temperature data with some flagged intervals
  testTempData1 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(5.0, -2.0),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(0L, 1L)  # Second interval is flagged
  )
  
  # Neighbor sensors with good data
  testTempData2 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(4.5, 0.5),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(0L, 0L)  # All good
  )
  
  testTempData3 <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    soilTempMean = c(4.8, 0.3),
    soilTempExpUncert = c(1.0, 1.0),
    finalQF = c(0L, 0L)  # All good
  )
  
  # Write test data
  arrow::write_parquet(testTempData1, testDataPath1)
  arrow::write_parquet(testTempData2, testDataPath2)
  arrow::write_parquet(testTempData3, testDataPath3)
  
  # Create sensor info with neighbors
  sensorInfo <- list(
    closest = data.frame(
      sensor_id = "temp-soil_TEST001",
      depth_m = -0.20,
      data_path = testDataPath1,
      location_path = "/fake/path1.json",
      stringsAsFactors = FALSE
    ),
    neighbors = list(
      higher = data.frame(
        sensor_id = "temp-soil_TEST002",
        depth_m = -0.10,
        data_path = testDataPath2,
        location_path = "/fake/path2.json",
        stringsAsFactors = FALSE
      ),
      lower = data.frame(
        sensor_id = "temp-soil_TEST003",
        depth_m = -0.30,
        data_path = testDataPath3,
        location_path = "/fake/path3.json",
        stringsAsFactors = FALSE
      )
    )
  )
  
  # Calculate flags with distance check
  result <- def.calc.temp.flags(
    sensorInfo = sensorInfo,
    targetDepth = -0.20,
    distThreshold = 0.15,
    log = NULL
  )
  
  # Check structure
  expect_equal(nrow(result), 2)
  
  # First interval: primary sensor is good, should use it
  expect_equal(result$temp_flag[1], 0L)  # 5.0 < 1.0 = FALSE -> 0
  
  # Second interval: primary sensor is flagged, should use neighbor average
  # Neighbor avg: ((0.5 - 1.0) + (0.3 - 1.0)) / 2 = (-0.5 + -0.7) / 2 = -0.6
  # avgZeroCheck < 1.0 = TRUE -> 1
  expect_equal(result$temp_flag[2], 1L)
  
  # Cleanup
  unlink(c(testDataPath1, testDataPath2, testDataPath3))
})

########################################################################################################
# Test 5: def.apply.temp.flags
########################################################################################################
test_that("Test def.apply.temp.flags joins flags correctly", {
  # Create high-frequency soil moisture data
  dataSm <- data.frame(
    readout_time = as.POSIXct(c(
      "2025-10-17 00:00:05", "2025-10-17 00:00:15", "2025-10-17 00:00:25",
      "2025-10-17 00:01:05", "2025-10-17 00:01:15", "2025-10-17 00:01:25"
    ), tz = "UTC"),
    depth01SoilMoisture = c(0.25, 0.26, 0.24, 0.27, 0.25, 0.26),
    tempTestDepth01QF = rep(-1L, 6)  # Initialize to -1
  )
  
  # Create minute-interval temperature flags
  tempData <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    temp_flag = c(0L, 1L)
  )
  
  # Apply flags
  result <- def.apply.temp.flags(
    dataSm = dataSm,
    tempData = tempData,
    qfColName = "tempTestDepth01QF",
    log = NULL
  )
  
  # Check that flags were applied correctly
  # First 3 rows fall in first minute interval (flag = 0)
  expect_equal(result$tempTestDepth01QF[1:3], c(0L, 0L, 0L))
  
  # Last 3 rows fall in second minute interval (flag = 1)
  expect_equal(result$tempTestDepth01QF[4:6], c(1L, 1L, 1L))
})

test_that("Test def.apply.temp.flags handles missing intervals", {
  # Create data with some points outside the temp data intervals
  dataSm <- data.frame(
    readout_time = as.POSIXct(c(
      "2025-10-17 00:00:05",  # Inside first interval
      "2025-10-17 00:02:05",  # Outside any interval
      "2025-10-17 00:01:05"   # Inside second interval
    ), tz = "UTC"),
    depth01SoilMoisture = c(0.25, 0.26, 0.27),
    tempTestDepth01QF = rep(-1L, 3)
  )
  
  tempData <- data.frame(
    startDateTime = as.POSIXct(c("2025-10-17 00:00:00", "2025-10-17 00:01:00"), tz = "UTC"),
    endDateTime = as.POSIXct(c("2025-10-17 00:01:00", "2025-10-17 00:02:00"), tz = "UTC"),
    temp_flag = c(0L, 1L)
  )
  
  result <- def.apply.temp.flags(
    dataSm = dataSm,
    tempData = tempData,
    qfColName = "tempTestDepth01QF",
    log = NULL
  )
  
  # First row should get flag 0
  expect_equal(result$tempTestDepth01QF[1], 0L)
  
  # Second row has no matching interval, should remain -1
  expect_equal(result$tempTestDepth01QF[2], -1L)
  
  # Third row should get flag 1
  expect_equal(result$tempTestDepth01QF[3], 1L)
})



########################################################################################################
# Test 6: Integration tests with real data - various edge cases and failure scenarios
########################################################################################################
test_that("Integration test with good data (baseline)", {
  # Use actual test data paths
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Only run if test data exists
  if (dir.exists(DirIn) && dir.exists(DirTemp)) {
    # Clean output directory
    if (dir.exists(DirOutBase)) {
      unlink(DirOutBase, recursive = TRUE)
    }
    
    # Run the wrap function
    
    wrap.envscn.temp.flags(
      DirIn = DirIn,
      DirOutBase = DirOutBase,
      DirTemp = DirTemp,
      DirSubCopy = c("data", "location", "threshold"),
      log = NULL
    )
    
    # Check that output was created
    expect_true(dir.exists(DirOutBase))
    
    # Check that flags directory was created
    flagsDir <- file.path(DirOutBase, "enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/flags")
    expect_true(dir.exists(flagsDir))
    
    # Check that flag file exists
    flagFiles <- list.files(flagsDir, pattern = "flagsPlausibility.parquet")
    expect_equal(length(flagFiles), 1)
    
    # Read and validate the output
    flagData <- arrow::read_parquet(file.path(flagsDir, flagFiles[1]))
    
    # Check that tempTest columns were added
    tempTestCols <- names(flagData)[grepl("tempTestDepth", names(flagData))]
    expect_true(length(tempTestCols) == 8)
    
    # Check that flags have valid values (0, 1, or -1)
    for (col in tempTestCols) {
      expect_true(all(flagData[[col]] %in% c(-1L, 0L, 1L)))
    }
    
    # Check that timestamps are present
    expect_true("readout_time" %in% names(flagData))
    
    # Cleanup
    unlink(DirOutBase, recursive = TRUE)
  } else {
    skip("Test data not available")
  }
})

# Test with missing depth values in soil moisture data
test_that("Integration test with missing soil moisture depths", {
  # Use actual test data paths
  # Soil moisture data missing depth information
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/no_depths/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  # Temperature data is good
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Only run if test data exists
  if (dir.exists(DirIn) && dir.exists(DirTemp)) {
    # Clean output directory
    if (dir.exists(DirOutBase)) {
      unlink(DirOutBase, recursive = TRUE)
    }
    
    # Run the wrap function
    
    wrap.envscn.temp.flags(
      DirIn = DirIn,
      DirOutBase = DirOutBase,
      DirTemp = DirTemp,
      DirSubCopy = c("data", "location", "threshold"),
      log = NULL
    )
    
    # Check that output was created
    expect_true(dir.exists(DirOutBase))
    
    # Check that flags directory was created
    flagsDir <- file.path(DirOutBase, "enviroscan/tests/no_depths/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/flags")
    expect_true(dir.exists(flagsDir))
    
    # Check that flag file exists
    flagFiles <- list.files(flagsDir, pattern = "flagsPlausibility.parquet")
    expect_equal(length(flagFiles), 1)
    
    # Read and validate the output
    flagData <- arrow::read_parquet(file.path(flagsDir, flagFiles[1]))
    
    # Check that data is all -1 due to missing depth information
    expect_true(all(flagData$tempTestDepth01QF), -1)
    
    tempTestCols <- names(flagData)[grepl("tempTestDepth", names(flagData))]
    
    # Check that flags have valid values (0, 1, or -1)
    for (col in tempTestCols) {
      expect_true(all(flagData[[col]] %in% c(-1L, 0L, 1L)))
    }
    
    # Check that timestamps are present
    expect_true("readout_time" %in% names(flagData))
    
    # Cleanup
    unlink(DirOutBase, recursive = TRUE)
  } else {
    skip("Test data not available")
  }
})


# Test with freezing temperature conditions
test_that("Integration test with all freezing data", {
  # Use actual test data paths
  # Good soil moisture data
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  # Temperature data with all freezing values
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/all_freezing/2025/10/17/conc-h2o-soil-salinity_GRSM005501/")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Only run if test data exists
  if (dir.exists(DirIn) && dir.exists(DirTemp)) {
    # Clean output directory
    if (dir.exists(DirOutBase)) {
      unlink(DirOutBase, recursive = TRUE)
    }
    
    # Run the wrap function
    
    wrap.envscn.temp.flags(
      DirIn = DirIn,
      DirOutBase = DirOutBase,
      DirTemp = DirTemp,
      DirSubCopy = c("data", "location", "threshold"),
      log = NULL
    )
    
    # Check that output was created
    expect_true(dir.exists(DirOutBase))
    
    # Check that flags directory was created
    flagsDir <- file.path(DirOutBase, "enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/flags")
    expect_true(dir.exists(flagsDir))
    
    # Check that flag file exists
    flagFiles <- list.files(flagsDir, pattern = "flagsPlausibility.parquet")
    expect_equal(length(flagFiles), 1)
    
    # Read and validate the output
    flagData <- arrow::read_parquet(file.path(flagsDir, flagFiles[1]))
    
    # Check that data was flagged as frozen
    expect_true(all(flagData$tempTestDepth02QF), 1)
    
    tempTestCols <- names(flagData)[grepl("tempTestDepth", names(flagData))]
    
    # Check that flags have valid values (0, 1, or -1)
    for (col in tempTestCols) {
      expect_true(all(flagData[[col]] %in% c(-1L, 0L, 1L)))
    }
    
    # Check that timestamps are present
    expect_true("readout_time" %in% names(flagData))
    
    # Cleanup
    unlink(DirOutBase, recursive = TRUE)
  } else {
    skip("Test data not available")
  }
})

# Test with missing temperature data directory
test_that("Integration test with no temperature data", {
  # Use actual test data paths
  # Good soil moisture data
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  # Non-existent temperature data path
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/no_temp_data/2020/10/14")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Run the wrap function
  
  wrap.envscn.temp.flags(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirTemp = DirTemp,
    DirSubCopy = c("data", "location", "threshold"),
    log = NULL
  )
  
  # Check that output was created
  expect_true(dir.exists(DirOutBase))
  
  # Check that flags directory was created
  flagsDir <- file.path(DirOutBase, "enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/flags")
  expect_true(dir.exists(flagsDir))
  
  # Check that flag file exists
  flagFiles <- list.files(flagsDir, pattern = "flagsPlausibility.parquet")
  expect_equal(length(flagFiles), 1)
  
  # Read and validate the output
  flagData <- arrow::read_parquet(file.path(flagsDir, flagFiles[1]))
  
  
  tempTestCols <- names(flagData)[grepl("tempTestDepth", names(flagData))]
  
  # All flags should be -1 when no temperature data is available
  for (col in tempTestCols) {
    expect_true(all(flagData[[col]] %in% c(-1L)))
  }
  
  # Check that all 8 columns are present
  expect_true(length(tempTestCols)==8)
  
  # Check that timestamps are present
  expect_true("readout_time" %in% names(flagData))
  
  # Cleanup
  unlink(DirOutBase, recursive = TRUE)
  
})


# Test with missing z_offset in temperature sensor location files
test_that("Integration test with missing z_offset", {
  # Use actual test data paths
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  # Temperature data with location files missing z_offset field
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/no_z_offset/2025/10/17/conc-h2o-soil-salinity_GRSM005501/")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Run the wrap function
  
  wrap.envscn.temp.flags(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirTemp = DirTemp,
    DirSubCopy = c("data", "location", "threshold"),
    log = NULL
  )
  
  # Check that output was created
  expect_true(dir.exists(DirOutBase))
  
  # Check that flags directory was created
  flagsDir <- file.path(DirOutBase, "enviroscan/tests/good_data/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/flags/")
  expect_true(dir.exists(flagsDir))
  
  # Check that flag file exists
  flagFiles <- list.files(flagsDir, pattern = "flagsPlausibility.parquet")
  expect_equal(length(flagFiles), 1)
  
  # Read and validate the output
  flagData <- arrow::read_parquet(file.path(flagsDir, flagFiles[1]))
  
  tempTestCols <- names(flagData)[grepl("tempTestDepth", names(flagData))]
  
  # Flags should have valid values (may have some -1 for missing z_offset)
  for (col in tempTestCols) {
    expect_true(all(flagData[[col]] %in% c(0L, 1L,-1L)))
  }
  
  # Check that all 8 columns are present
  expect_true(length(tempTestCols)==8)
  
  # Check that timestamps are present
  expect_true("readout_time" %in% names(flagData))
  
  # Cleanup
  unlink(DirOutBase, recursive = TRUE)
  
})


test_that("Integration test with missing threshold file (expect error)", {
  # Use actual test data paths
  # Soil moisture data without threshold configuration file
  DirIn <- file.path(getwd(), "pfs/envscn_temp_flags/enviroscan/tests/no_thresholds/2025/10/17/conc-h2o-soil-salinity_GRSM005501/enviroscan/CFGLOC105360/")
  # Temperature data path (test should fail before reaching here)
  DirTemp <- file.path(getwd(), "pfs/envscn_temp_flags/temp/tests/no_temp_data/2020/10/14")
  DirOutBase <- file.path(tempdir(), "test_output")
  
  # Run the wrap function
  
  expect_error(wrap.envscn.temp.flags(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirTemp = DirTemp,
    DirSubCopy = c("data", "location", "threshold"),
    log = NULL
  ))
  
  # Cleanup
  unlink(DirOutBase, recursive = TRUE)
  
})
