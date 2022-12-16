#' @title Unit test of def.time.heat.on.R, determine discrete periods a heater was turned on based on heater events
#' 
#' @description 
#' Definition function. Determine discrete periods a heater was turned on based on heater events.

#' @param dataHeat Data frame of heater event data as returned by NEONprocIS.base::def.read.evnt.json.R
#' At a minimum, column variables include:
#' timestamp = POSIX time of heater status
#' status = logical (TRUE=heater on)
#'  
#' @param TimeOffAuto A difftime object indicating the timeout period after which to assume the heater
#' turned off even though there is no even indicating so (e.g. base::as.difftime(30,units='mins')).
#' Default is never (NULL).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than 
#' standard R error messaging will be used.
#' 
#' @return A data frame of:
#' timeOn POSIXct Time heater turned on
#' timeOff POSIXct Time heater turned off

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' TimeOffAuto <- base::as.difftime(30,units="mins")
#' timeOnHeat <- def.heat.time.on(dataHeat,TimeOffAuto=TimeOffAuto)
#'  

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}
#' @seealso \code{\link[NEONprocIS.base]{def.read.evnt.json}}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2022-12-02)
#     modified the original test
###############################################################################################
# Define test context
context("\n   | Unit test of def.time.heat.on.R, determining discrete periods a heater was turned on based on heater events\n")

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
            rpt <- NEONprocIS.qaqc::def.time.heat.on(dataHeat = dataHeat)
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt$timeOn[1], as.POSIXct("2019-05-02 00:18:28",tz="GMT"))
          }
)
