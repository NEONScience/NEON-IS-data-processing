library(testthat)
source("R/def.loc.meta.R")


test_that("When No restriction, get all locations",
          {
            nameFile <- "tests/testthat/def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"

            locationMetaData <- def.loc.meta(NameFile = nameFile)
            expect_true (length(locationMetaData$site) > 0)

          })


test_that("When restricted to location, then return only that locaiton",
          {
            nameFile <- "tests/testthat/def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            nameLoc <- "CFGLOC101663"
            locationMetaData <- def.loc.meta(NameFile = nameFile, NameLoc = nameLoc)
            expect_true (length(locationMetaData$site) == 1)
            expect_true (locationMetaData[1], equals("POSE"))
          
          })



