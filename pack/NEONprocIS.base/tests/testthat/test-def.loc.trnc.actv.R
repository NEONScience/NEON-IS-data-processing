#library(testthat)
#source("R/def.loc.trnc.actv.R")
test_that("valid nameFileIn",
          {
            #NameFileIn, NameFileOut = NULL, TimeBgn, TimeEnd = NULL, log = NULL
            #nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn)
            testthat::expect_true(is.list(loc))
            testthat::equals(loc$type, "FeatureCollection")
            testthat::equals(length(loc$features), 1)

          }

)
test_that("nameFileIn validation failure",
          {
           # nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101256.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            loc <- try(NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn),
                       silent = TRUE)
            testthat::expect_true((class(loc)[1] == "try-error"))

          }

)
test_that("valid nameFileIn and NameFileOut",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::equals(loc$type, "FeatureCollection")
            testthat::equals(length(loc$features), 1)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}



          }
)
test_that("start_date is not in the test file,  output file should have the start_date as the timeBng",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255_1.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::equals(loc$type, "FeatureCollection")
            testthat::equals(length(loc$features), 1)
            testthat::equals(loc$features[1]$properties$active_periods[1]$start_date, timeBgn)
            testthat::equals(loc$features[1]$properties$active_periods[1]$end_date, timeEnd)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}

          }
)
test_that("active period end date is less than or equal to the timeEnd, get the end time from the test file",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255_2.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2018-03-05T00:00:00Z', tz = 'GMT')
            expectedTimeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::equals(loc$type, "FeatureCollection")
            testthat::equals(length(loc$features), 1)
            testthat::equals(loc$features[1]$properties$active_periods[1]$start_date, timeBgn)
            testthat::equals(loc$features[1]$properties$active_periods[1]$end_date, timeEnd)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
)

