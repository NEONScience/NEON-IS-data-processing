#library(testthat)
#source("R/def.thsh.slct.R")
test_that("when one of the threshold matches",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds.json')
            time1 <- base::as.POSIXct('2000-01-01 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Site="ABBY", Term="TFPrecipBulk")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2019-04-30 18:10:20 MDT")
            testthat::equals(rpt[1]$location_name, "ABBY")
          }

)

test_that("none of the thresholds matches",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/empty_thresholds.json')
            time1 <- base::as.POSIXct('2020-01-01 00:00:00', tz = 'GMT')
            rpt <- try(NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Site="ABBY", Term="TFPrecipBulk"), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
          }

)

test_that("when there are two thresholds, just consider first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Site="ABBY", Term="TFPrecipBulk", NameLoc="ABBY", RptThsh = FALSE)
            testthat::expect_false(is.list(rpt))
            testthat::equals(rpt, 1)

          }

)
test_that("For SENSOR & DOY-specific when there are two thresholds, just consider first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Site="ABBY", Term="TFPrecipBulk", NameLoc="ABBY", RptThsh = FALSE)
            testthat::expect_false(is.list(rpt))
            testthat::equals(rpt, 1)

          }

)
test_that("For SENSOR-specific",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Site="ABBY", Term="rawVSWC7", NameLoc="REALM")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2000-01-01T00:00:00Z")
            testthat::equals(rpt[1]$location_name, "REALM")
            testthat::equals(rpt[1]$number_value, 1.2)

          }
)

test_that("For SENSOR-specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Term="rawVSWC9", NameLoc="REALM")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2000-01-01T00:00:00Z")
            testthat::equals(rpt[1]$location_name, "REALM")
            testthat::equals(rpt[1]$number_value, 1.5)

          }
)
test_that("For SITE & DOY-specific",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Term="TBPrecipBulk", Site="PUMA")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2000-01-01T00:00:00Z")
            testthat::equals(rpt[1]$location_name, "PUMA")
            testthat::equals(rpt[1]$number_value, 20)
            testthat::equals(rpt[1]$end_day_of_year, 15)

          }
)
test_that("For SITE specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Term="TCPrecipBulk", Site="CEPR")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2000-01-01T00:00:00Z")
            testthat::equals(rpt[1]$location_name, "CPER")
            testthat::equals(rpt[1]$number_value, NULL)
            testthat::equals(rpt[1]$string_value, "test")

          }
)
test_that("For REALM & DOY-specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh, Time = time1, Term="TDPrecipBulk")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::equals(rpt[1]$start_date, "2000-01-01T00:00:00Z")
            testthat::equals(rpt[1]$location_name, "REALM")
            testthat::equals(rpt[1]$number_value, 20)
            testthat::equals(rpt[1]$string_value, NULL)

          }
)
