#library(testthat)
#source("R/def.loc.trnc.actv.R")
test_that("valid nameFileIn",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn)
            testthat::expect_true(is.list(loc))
            testthat::expect_equal(loc$type, "FeatureCollection")
            testthat::expect_equal(length(loc$features), 1)

          }

)
test_that("valid nameFileIn and NameFileOut, one active period outside range of interest",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::expect_equal(loc$type, "FeatureCollection")
            testthat::expect_equal(length(loc$features[[1]]$properties$active_periods), 1)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}



          }
)
test_that("start_date & end date not in the test file,  output should have the start_date as the timeBgn and end_date as timeEnd",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255_1.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::expect_equal(loc$type, "FeatureCollection")
            testthat::expect_equal(length(loc$features), 1)
            testthat::expect_equal(
              base::as.POSIXct(loc$features[[1]]$properties$active_periods[[1]]$start_date, tz = 'GMT'),
              timeBgn)
            testthat::expect_equal(
              base::as.POSIXct(loc$features[[1]]$properties$active_periods[[1]]$end_date, tz = 'GMT'),
              timeEnd)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}

          }
)
test_that("active period start & end date is inside timeBgn and timeEnd, get the start & end time from the test file",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255_2.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2018-03-05T00:00:00Z', tz = 'GMT')
            expectedTimeBgn <- base::as.POSIXct('2015-03-06T00:00:00Z', tz = 'GMT')
            expectedTimeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(loc))
            testthat::expect_equal(loc$type, "FeatureCollection")
            testthat::expect_equal(length(loc$features), 1)
            testthat::expect_equal(
              base::as.POSIXct(loc$features[[1]]$properties$active_periods[[1]]$start_date, tz = 'GMT'), 
              expectedTimeBgn)
            testthat::expect_equal(
              base::as.POSIXct(loc$features[[1]]$properties$active_periods[[1]]$end_date, tz = 'GMT'),
              expectedTimeEnd)
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
)

