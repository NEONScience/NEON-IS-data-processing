#library(testthat)
#source("R/def.time.miss.R")


test_that("When complete time range is missing",
          {
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
            timeFull <- base::data.frame(timeBgn=as.POSIXct(c('2019-01-11','2019-01-13'),tz='GMT'),
                                         timeEnd=as.POSIXct(c('2019-01-12','2019-01-14'),tz='GMT'))
            timeMiss <- NEONprocIS.base::def.time.miss(TimeBgn = timeBgn, TimeEnd=timeEnd, timeFull= timeFull)
            testthat::expect_true(length(timeMiss) == 2)
            testthat::equals(timeMiss$timeBgn, "2019-01-01")
            testthat::equals(timeMiss$timeEnd, "2019-01-10")
          })


test_that("When beginning time range is missing",
          {
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
            timeFull <- base::data.frame(timeBgn=as.POSIXct(c('2019-01-05','2019-01-08'),tz='GMT'),
                                         timeEnd=as.POSIXct(c('2019-01-09','2019-01-14'),tz='GMT'))
            timeMiss <- NEONprocIS.base::def.time.miss(TimeBgn = timeBgn, TimeEnd=timeEnd, timeFull= timeFull)
            expect_true (length(timeMiss) == 2)
            testthat::equals(timeMiss$timeBgn, "2019-01-01")
            testthat::equals(timeMiss$timeEnd, "2019-01-05")

          })

test_that("When end time range is missing",
          {
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
            timeFull <- base::data.frame(timeBgn=as.POSIXct(c('2019-01-01','2019-01-04'),tz='GMT'),
                                         timeEnd=as.POSIXct(c('2019-01-05','2019-01-09'),tz='GMT'))
            timeMiss <- NEONprocIS.base::def.time.miss(TimeBgn = timeBgn, TimeEnd=timeEnd, timeFull= timeFull)
            expect_true (length(timeMiss) == 2)
            testthat::equals(timeMiss$timeBgn, "2019-01-09")
            testthat::equals(timeMiss$timeEnd, "2019-01-10")

          })

test_that("When middle time range is missing",
          {
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
            timeFull <- base::data.frame(timeBgn=as.POSIXct(c('2019-01-01','2019-01-07'),tz='GMT'),
                                         timeEnd=as.POSIXct(c('2019-01-04','2019-01-09'),tz='GMT'))
            timeMiss <- NEONprocIS.base::def.time.miss(TimeBgn = timeBgn, TimeEnd=timeEnd, timeFull= timeFull)
            expect_true (length(timeMiss) == 2)
            testthat::equals(timeMiss$timeBgn[1], "2019-01-04")
            testthat::equals(timeMiss$timeEnd[1], "2019-01-07")
            testthat::equals(timeMiss$timeBgn[2], "2019-01-09")
            testthat::equals(timeMiss$timeEnd[2], "2019-01-10")

          })


test_that("When timeFull is empty",
          {
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
            timeFull <- data.frame(timeBgn=as.Date(character()),
                                   timeEnd=as.Date(character()),
                                   stringsAsFactors=FALSE)

            timeMiss <- NEONprocIS.base::def.time.miss(TimeBgn = timeBgn, TimeEnd=timeEnd, timeFull= timeFull)
            expect_true (length(timeMiss) == 2)
            testthat::equals(timeMiss$timeBgn[1], "2019-01-01")
            testthat::equals(timeMiss$timeEnd[1], "2019-01-10")
          })

