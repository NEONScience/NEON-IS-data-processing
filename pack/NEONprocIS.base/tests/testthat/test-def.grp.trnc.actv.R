#library(testthat)
#source("R/def.grp.trnc.actv.R")
test_that("valid nameFileIn",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2025-03-05T00:00:00Z', tz = 'GMT')
            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn, TimeEnd=timeEnd)
            testthat::expect_true(is.list(grp))
            testthat::expect_equal(grp$type, "FeatureCollection")
            testthat::expect_equal(length(grp$features), 4)
          }

)
test_that("valid nameFileIn and NameFileOut, one group outside range of interest",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245.json'
            nameFileOut = 'def.grp.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(grp))
            testthat::expect_equal(grp$type, "FeatureCollection")
            testthat::expect_equal(length(grp$features), 3)
            testthat::expect_equal(length(grp$features[[1]]$properties$active_periods), 2)
            testthat::expect_equal(length(grp$features[[2]]$properties$active_periods), 1)
            testthat::expect_equal(length(grp$features[[3]]$properties$active_periods), 1)
            testthat::expect_true(file.exists(nameFileOut))
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
          }
)
test_that("no TimeEnd specified, TimeEnd set to 1 second after TimeBgn",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn)
            testthat::expect_true(is.list(grp))
            testthat::expect_equal(grp$type, "FeatureCollection")
            testthat::expect_equal(length(grp$features), 2)
          }
          
)
test_that("start_date & end date not in the test file,  output should have the start_date as the timeBgn and end_date as timeEnd",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(grp))
            testthat::expect_equal(grp$type, "FeatureCollection")
            testthat::expect_equal(length(grp$features), 3)
            testthat::expect_equal(
              base::as.POSIXct(grp$features[[3]]$properties$active_periods[[1]]$start_date, tz = 'GMT'),
              timeBgn)
            testthat::expect_equal(
              base::as.POSIXct(grp$features[[3]]$properties$active_periods[[1]]$end_date, tz = 'GMT'),
              timeEnd)
          }
)
test_that("active period start & end date is inside timeBgn and timeEnd, get the start & end time from the test file",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245.json'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            expectedTimeBgn <- base::as.POSIXct('2016-01-03T12:16:00Z', tz = 'GMT')
            expectedTimeEnd <- base::as.POSIXct('2016-02-05T12:16:00Z', tz = 'GMT')

            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.list(grp))
            testthat::expect_equal(grp$type, "FeatureCollection")
            testthat::expect_equal(length(grp$features), 3)
            testthat::expect_equal(
              base::as.POSIXct(grp$features[[1]]$properties$active_periods[[1]]$start_date, tz = 'GMT'),
              expectedTimeBgn)
            testthat::expect_equal(
              base::as.POSIXct(grp$features[[1]]$properties$active_periods[[1]]$end_date, tz = 'GMT'),
              expectedTimeEnd)

          }
)
test_that("No applicable active periods. Output is NULL and no file is written",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245_1.json'
            nameFileOut = 'def.grp.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2022-12-01T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2022-12-02T00:00:00Z', tz = 'GMT')

            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(is.null(grp))
            testthat::expect_false(file.exists(nameFileOut))
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
)
test_that("Multiple output groups are sorted",
          {
            nameFileIn = 'def.grp.trnc.actv/CFGLOC100245_1.json'
            nameFileOut = 'def.grp.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2017-01-01T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2019-12-02T00:00:00Z', tz = 'GMT')
            
            grp <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd)
            testthat::expect_true(file.exists(nameFileOut))
            testthat::expect_true(length(grp$features) == 2)
            testthat::expect_true(grp$features[[1]]$properties$group == 'test-group_1')
            testthat::expect_true(grp$features[[2]]$properties$group == 'test-group_2')
            testthat::expect_true(grp$features[[2]]$properties$data_product_ID[1] == 'DP1.20008.001')
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
)