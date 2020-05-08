# library(testthat)
# source("R/def.loc.meta.R")


test_that("When No restriction, get all locations",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"

            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile)
            expect_true (length(locationMetaData$site) > 0)

          })


test_that("When restricted to location, then return only that locaiton",
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




