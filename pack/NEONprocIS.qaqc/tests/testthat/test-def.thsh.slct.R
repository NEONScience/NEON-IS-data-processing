#' @title Unit test of def.thsh.slct.R, determine set of applicable QA/QC thresholds for date, location, term, and context

#' @description
#' Definition function. Given a json file of thresholds, return those that are applicable to the
#' date, term (variable), and context (all properties of the thresholds). The choice of
#' constraint/threshold to use is determined by moving up the following hierarchy
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint
#' is chosen. Threshold selection order is as follows (1 being the finest possible contraint):
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal

#' @param thsh List of thresholds, as returned from NEONprocIS.qaqc::def.read.thsh.qaqc.list
#' @param Time POSIXct value of the day to select thresholds for (assumes time resolution
#' for thresholds is 1 day). Time should be at 00:00:00 GMT
#' @param Term Character value. The term for which to select thresholds for.
#' @param Ctxt Character vector (optional) . The contexts for which to select thresholds for. Treated
#' as an AND with \code{Term}, meaning that the thresholds are selected which match both the Term
#' and all contexts. Defaults to NULL, in which case the criteria for threshold selection is limited
#'  to the term.
#' @param Site Character value. The NEON site code. (e.g. HARV). If NULL (default), the REALM
#' thresholds will be selected.
#' @param NameLoc Character value. The specific named location of the sensor. If NULL (default),
#' the REALM thresholds will be selected.
#' @param RptThsh Logical value. If TRUE, the filtered list of thresholds is output. If FALSE, the
#' indices of the selected thresholds in the input list is returned. Defaults to TRUE.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return If the RptThsh argument is TRUE, the filtered (selected) list of thresholds is output
#' in the same format as input \code{thsh}. If RptThsh is false, the indices of the selected
#' thresholds in the input list \code{thsh} is returned.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords quality control, quality assurance, QA/QC, QA/QC test
#'
#' @examples
#' Currently none

#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.df}
#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2022-12-01)
#     added location_name=REALM case to the original test by adding thresholds-REALM.json
##############################################################################################
test_that("when one of the threshold matches",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds.json')
            time1 <- base::as.POSIXct('2000-01-01 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Site = "ABBY",
                Term = "TFPrecipBulk"
              )
            testthat::expect_true(is.list(rpt))
            testthat::expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date, as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "ABBY")
          })


test_that("term_name = relativeHumidity,  location_name = REALM",
          {
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds-REALM.json')
            time1 <- base::as.POSIXct('2000-01-01 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh,
                                             Time = time1,
                                             Term = "relativeHumidity")
            testthat::expect_true(is.list(rpt))
            testthat::expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt[[1]]$start_date, as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "REALM")
          })

test_that("term_name = temperature,  location_name = REALM",
          {
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds-REALM.json')
            time1 <- base::as.POSIXct('2000-01-01 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh,
                                             Time = time1,
                                             Term = "temperature")
            testthat::expect_true(is.list(rpt))
            testthat::expect_true(length(rpt) == 2)
            testthat::expect_equal(rpt[[1]]$start_date, as.POSIXct("2000-01-01",tz="GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "REALM")
          })

test_that("none of the thresholds matches",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <-
              NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/empty_thresholds.json')
            time1 <-
              base::as.POSIXct('2020-01-01 00:00:00', tz = 'GMT')
            rpt <-
              try(NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Site = "ABBY",
                Term = "TFPrecipBulk"
              ),
              silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
          })

test_that("when there are two thresholds, just consider first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Site = "ABBY",
                Term = "TFPrecipBulk",
                NameLoc = "ABBY",
                RptThsh = FALSE
              )
            testthat::expect_false(is.list(rpt))
            testthat::expect_equal(rpt, 1)
            
          })
test_that("For SENSOR & DOY-specific when there are two thresholds, just consider first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Site = "ABBY",
                Term = "TFPrecipBulk",
                NameLoc = "ABBY",
                RptThsh = FALSE
              )
            testthat::expect_false(is.list(rpt))
            testthat::expect_equal(rpt, 1)
          })

test_that("For SENSOR-specific",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Site = "ABBY",
                Term = "rawVSWC7",
                NameLoc = "REALM"
              )
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date, as.POSIXct("2000-01-01", tz = 'GMT'))
            testthat::expect_equal(rpt[[1]]$location_name, "REALM")
            testthat::expect_equal(rpt[[1]]$number_value, 1.2)
            
          })

test_that("For SENSOR-specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds2.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Term = "rawVSWC9",
                NameLoc = "REALM"
              )
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date,  as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "REALM")
            testthat::expect_equal(rpt[[1]]$number_value, 1.5)
            
          })
test_that("For SITE & DOY-specific",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Term = "TBPrecipBulk",
                Site = "PUMA"
              )
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date,  as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "PUMA")
            testthat::expect_equal(rpt[[1]]$number_value, 20)
            testthat::expect_equal(rpt[[1]]$end_day_of_year, 15)
            
          })
test_that("For SITE specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(
                thsh = inputThsh,
                Time = time1,
                Term = "TCPrecipBulk",
                Site = "CPER"
              )
            testthat::expect_true(is.list(rpt))
            testthat::expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date,  as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "CPER")
            testthat::expect_equal(rpt[[1]]$number_value, 10)
            testthat::expect_equal(rpt[[1]]$string_value, NULL)
            
          })
test_that("For REALM & DOY-specific, when there are more than 2 thresholds, use first one",
          {
            #thsh, Time, Term, Ctxt = NULL, Site=NULL, NameLoc=NULL, RptThsh = TRUE, log = NULL
            inputThsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds3.json')
            time1 <- base::as.POSIXct('2000-01-10 00:00:00', tz = 'GMT')
            rpt <- NEONprocIS.qaqc::def.thsh.slct(thsh = inputThsh,
                                             Time = time1,
                                             Term = "TDPrecipBulk")
            testthat::expect_true(is.list(rpt))
            expect_true(length(rpt) == 1)
            testthat::expect_equal(rpt[[1]]$start_date,  as.POSIXct("2000-01-01", tz = "GMT"))
            testthat::expect_equal(rpt[[1]]$location_name, "REALM")
            testthat::expect_equal(rpt[[1]]$number_value, 20)
            testthat::expect_equal(rpt[[1]]$string_value, NULL)
            
          })
