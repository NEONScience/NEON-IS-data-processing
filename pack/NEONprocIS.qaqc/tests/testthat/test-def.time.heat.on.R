#library(testthat)
#source("R/def.time.heat.on.R")
test_that("calculate the time on dataHeat State is always false",
          {
            time1 <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
            time2 <- base::as.POSIXct('2019-05-02 00:18:28', tz = 'GMT')
            time3 <- base::as.POSIXct('2019-05-03 00:18:28', tz = 'GMT')
            timestamp <- c(time1, time2, time3)
            state <- c(FALSE, FALSE, FALSE)
            dataHeat <- data.frame(timestamp, state)
            rpt <- NEONprocIS.qaqc::def.time.heat.on(dataHeat = dataHeat)
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt$timeOff[1], as.POSIXct("2019-05-01 00:10:20",tz="GMT"))
            #  testthat::expect_that(rpt[1]$timeOn[1], equals(base::as.POSIXct("NA")))

          }

)
test_that("calculate the time on dataHeat when TimeOffAuto is not null",
          {
            time1 <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
            time2 <- base::as.POSIXct('2019-05-02 00:18:28', tz = 'GMT')
            time3 <- base::as.POSIXct('2019-05-03 00:20:28', tz = 'GMT')
            timestamp <- c(time1, time2, time3)
            state <- c(FALSE, TRUE, FALSE)
            dataHeat <- data.frame(timestamp, state)
            timeOffAuto <- base::as.difftime(30,units="mins")
            rpt <- NEONprocIS.qaqc::def.time.heat.on(dataHeat = dataHeat, TimeOffAuto= timeOffAuto)
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt$timeOff[1], as.POSIXct("2019-05-02 00:48:28",tz="GMT"))
            testthat::expect_equal(rpt$timeOn[1],  as.POSIXct("2019-05-02 00:18:28",tz="GMT"))

          }

)
test_that("calculate the time on dataHeat when TimeOffAuto is null",
          {
            time1 <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
            time2 <- base::as.POSIXct('2019-05-02 00:18:28', tz = 'GMT')
            time3 <- base::as.POSIXct('2019-05-03 00:18:28', tz = 'GMT')
            timestamp <- c(time1, time2, time3)
            state <- c(FALSE, TRUE, FALSE)
            dataHeat <- data.frame(timestamp, state)
            timeOffAuto <- base::as.difftime(30,units="mins")
            rpt <- NEONprocIS.qaqc::def.time.heat.on(dataHeat = dataHeat, TimeOffAuto = timeOffAuto)
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt$timeOff[1], as.POSIXct("2019-05-02 00:48:28",tz="GMT"))
          }

)
