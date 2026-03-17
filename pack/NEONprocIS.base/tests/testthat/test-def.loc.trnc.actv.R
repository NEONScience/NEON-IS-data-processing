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
            testthat::expect_true(file.exists(nameFileOut))
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
test_that("filter for a subset of properties to include in the output",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC101255_2.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2015-03-05T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2018-03-05T00:00:00Z', tz = 'GMT')
            expectedTimeBgn <- base::as.POSIXct('2015-03-06T00:00:00Z', tz = 'GMT')
            expectedTimeEnd <- base::as.POSIXct('2017-03-05T00:00:00Z', tz = 'GMT')
            PropKeep <- c("HOR","VER","name","description","site","Data Rate","active_periods")
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd,Prop=PropKeep)
            testthat::expect_true(is.list(loc))
            testthat::expect_true(all(names(loc$features[[1]]) %in% c(PropKeep,'properties','geometry')))
            testthat::expect_true(all(names(loc$features[[1]]$properties) %in% c(PropKeep)))
            testthat::expect_false('active_periods_flag' %in% names(loc$features[[1]]$properties$active_periods[[1]]))
            
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
          
)
test_that("active period start date is timeBgn, get the active periods flag as start",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC113812.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2024-03-27T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2024-03-28T00:00:00Z', tz = 'GMT')
            PropKeep <- c("HOR","VER","name","description","site","Data Rate","active_periods")
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd,Prop=PropKeep)
            testthat::expect_true(is.list(loc))
            testthat::expect_true('active_periods_flag' %in% names(loc$features[[1]]$properties$active_periods[[1]]))
            testthat::expect_true("start" == loc$features[[1]]$properties$active_periods[[1]]$active_periods_flag)
            
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }

)
test_that("active period end date is timeBgn, get the active periods flag as end",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC113812_2.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2024-09-16T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2024-09-17T00:00:00Z', tz = 'GMT')
            PropKeep <- c("HOR","VER","name","description","site","Data Rate","active_periods")
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd,Prop=PropKeep)
            testthat::expect_true(is.list(loc))
            testthat::expect_true('active_periods_flag' %in% names(loc$features[[1]]$properties$active_periods[[1]]))
            testthat::expect_true("end" == loc$features[[1]]$properties$active_periods[[1]]$active_periods_flag)
            
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
          
)
test_that("active period is single day, get the active periods flag as both",
          {
            nameFileIn = 'def.loc.trnc.actv/CFGLOC113812_1.json'
            nameFileOut = 'def.loc.trnc.actv/output.txt'
            timeBgn <- base::as.POSIXct('2024-03-27T00:00:00Z', tz = 'GMT')
            timeEnd <- base::as.POSIXct('2024-03-28T00:00:00Z', tz = 'GMT')
            PropKeep <- c("HOR","VER","name","description","site","Data Rate","active_periods")
            
            loc <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn = nameFileIn, NameFileOut = nameFileOut, TimeBgn = timeBgn, TimeEnd = timeEnd,Prop=PropKeep)
            testthat::expect_true(is.list(loc))
            testthat::expect_true('active_periods_flag' %in% names(loc$features[[1]]$properties$active_periods[[1]]))
            testthat::expect_true('both' == loc$features[[1]]$properties$active_periods[[1]]$active_periods_flag)
            
            if (file.exists(nameFileOut)) { file.remove(nameFileOut)}
            
          }
          
)

