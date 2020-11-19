#library(testthat)
#source("R/def.loc.meta.R")


context("Location metadata tests")


test_that("When no restriction, get all locations",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile)
            expect_true (length(locationMetaData$site) > 0)
            
          })


test_that("When restricted to location, return only that location",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            nameLoc <- "CFGLOC101663"
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, NameLoc = nameLoc)
            expect_true (length(locationMetaData$site) == 1)
           # expect_true (locationMetaData[1], equals("POSE"))

          })

test_that("location that are install before or equal to time Begin ",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)

          })

test_that("location that have removal date before time Begin",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-05-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 0)

          })

test_that("location that have removal date before time Begin",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)

          })


test_that("location that have install, removal, transaction dates null",
          {
            nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations_alldates_null.json"
            timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)
            testthat::equals(locationMetaData$install_date, "NA")
            testthat::equals(locationMetaData$remove_date, "NA")
            testthat::equals(locationMetaData$transaction_date, "NA")
            expect_true (length(locationMetaData$active_periods) == 1)

          })

test_that("location that have install, removal, and remove data after timeBgn",
          {
            nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations.json"
            timeBgn <- base::as.POSIXct('2018-09-05',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn, TimeEnd=timeEnd)
            expect_true (length(locationMetaData$site) == 1)
            expect_true (length(locationMetaData$active_periods) == 1)
            
          })
