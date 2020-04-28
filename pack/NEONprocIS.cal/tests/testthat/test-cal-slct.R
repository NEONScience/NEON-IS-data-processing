##############################################################################################
#' @title Unit test of def.cal.slct.R, determine applicable date ranges for calibration

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Run unit tests for def.cal.slct.R, determine applicable date ranges for calibration.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to def.cal.slct for the details of the function.

#' @param metaCal A data frame of calibration metadata as returned from NEONprocIS.cal::def.cal.meta
#' @param TimeBgn A POSIXct timestamp of the start date of interest (inclusive)
#' @param TimeEnd A POSIXct timestamp of the end date of interest (exclusive)
#' @param TimeExpiMax A difftime object of the maxumum time since expiration for which an expired

#' @return TRUE when a test passes. The test checks to ensure that a valid data frame is returned.
#' Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' # Not run
#' # fileCal <- c('/path/to/file1.xml','/path/to/file2.xml')
#' # metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=fileCal)
#' # TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
#' # TimeEnd <- base::as.POSIXct('2019-01-02',tz='GMT')
#' # TimeExpiMax <- base::as.difftime(30,units='days') # allow cals to be used up to 30 days after expiration
#' # NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=TimeBgn,TimeEnd=TimeEnd)

#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' To generate the test coverage of NEONprocIS.cal package after running the test:
#' library(devtools)
#' library(covr)
#' cov <- package_coverage()
#' report(cov)

#' @seealso \link[NEONprocIS.cal]{def.cal.meta}
#'
#' @export
# changelog and author contributions / copyrights
#   Mija Choi (2020-04-07)
#     original creation
##############################################################################################
# Define test context
#context("\n       |testing def.cal.slct.R, determine applicable date ranges for calibration\n")

# Test def.cal.slct.R, determine applicable date ranges for calibration
test_that("   Test of def.cal.slct.R, determine applicable date ranges for calibration",
          {
            ##########
            ##########  Happy path:::: All the input parameters are valid
            ##########
            ########## metaCal: a data frame of meta data from multiple calibration xmls
            ########## It has: name of each calibration xml,
            ########## timeValiBgn as.POSIXct,
            ########## timeValiEnd as.POSIXct and
            ########## id, which is CertificateNumber in calibration files.
            
            metaCal <- read.csv("metaCal.csv", stringsAsFactors = FALSE)
            #
            # ensure that metaCal$timeValiBgn and metaCal$timeValiEnd are in POSIXct.
            # The data in your test file are pulled in as character, so the code does not operate properly
            #
          
            metaCal$timeValiBgn <- as.POSIXct(metaCal$timeValiBgn)
            metaCal$timeValiEnd <- as.POSIXct(metaCal$timeValiEnd)
            
            cat("\n       |=====================================   Test Summary   ====================================|\n")
            
            cat("\n       |------ Positive test 1:: All the input parameters are valid with 'TimeExpiMax = NULL'      |\n")
            cat("\n       |------                   With max. time range this case will capture all calibrations      |\n")
            cat("\n       |------                   possible in the input metaCal                                     |\n")
            
            
            TimeBgn <- base::as.POSIXct('2019-05-01 00:10:20', tz='GMT')
            TimeEnd <- base::as.POSIXct('2020-03-08 00:18:28', tz='GMT')
            
            dfReturned <-
              NEONprocIS.cal::def.cal.slct(metaCal = metaCal,
                                           TimeBgn = TimeBgn,
                                           TimeEnd = TimeEnd)
            
            idList = c('162901', '162902', '162922', '162903', '162904')
            
            expect_true ((is.data.frame(dfReturned)) &&  all(metaCal$id %in% idList))
            
            cat("\n       |------                   The test ran successfully, a correct Data frame is returned       |\n")
            
            cat("\n       |------ Positive test 2:: All the input parameters are valid with 'TimeExpiMax = ~ 12 days' |\n")
            cat("\n       |------                 There are 2 cal files expiring on the same date with diff cert IDs  |\n")
            cat("\n       |------                 cal.slct will select the calibration with the higher CERT ID        |\n")
            
            TimeExpiMax <- base::as.difftime(10,units='days')
            
            TimeBgn <- base::as.POSIXct('2019-06-10 00:10:20', tz='GMT')
            TimeEnd <- base::as.POSIXct('2019-07-07 00:18:28', tz='GMT')
            
            dfReturned <- 
              NEONprocIS.cal::def.cal.slct(
                metaCal = metaCal,
                TimeBgn = TimeBgn,
                TimeEnd = TimeEnd,
                TimeExpiMax = TimeExpiMax
              )
            
            ID = '162922'
            expect_true ((is.data.frame(dfReturned)) &&  any(dfReturned$id %in% ID))
            
            cat("\n       |------ Negative test 1:: The input data frame is empty                                     |\n")
            
            TimeBgn <- base::as.POSIXct('2018-10-06T00:10:20Z')
            TimeEnd <- base::as.POSIXct('2020-02-07T00:18:28Z')
            
            data_empty <- metaCal[-c(1, 2, 3, 4, 5), ]
            
            dfReturned <-
              NEONprocIS.cal::def.cal.slct(metaCal = data_empty,
                                           TimeBgn = TimeBgn,
                                           TimeEnd = TimeEnd)
            
            expect_true ((is.data.frame(dfReturned)) && (nrow(dfReturned) == 1) && any(is.na(dfReturned)))
            
            cat("\n       |------ Negative test 2:: The input data frame has columns missing                          |\n")
            
            TimeBgn <- base::as.POSIXct('2018-10-06T00:10:20Z')
            TimeEnd <- base::as.POSIXct('2020-02-07T00:18:28Z')
            
            data_lessCol <- subset(metaCal, select = -file)
            
            dfReturned <-
              try(NEONprocIS.cal::def.cal.slct(metaCal = data_lessCol,
                                               TimeBgn = TimeBgn,
                                               TimeEnd = TimeEnd),
                  silent = TRUE)
            
            testthat::expect_true((class(dfReturned)[1] == "try-error"))
            
            cat("\n       |------ Negative test 3:: TimeBgn and TimeEnd are POSIXct. TimeEnd < TimeBgn                |\n")
            
            TimeBgn <- base::as.POSIXct('2018-10-06T00:10:20Z')
            TimeEnd <- base::as.POSIXct('2018-02-07T00:18:28Z')
            
            data_lessCol <- subset(metaCal, select = -file)
            
            dfReturned <-
              try(NEONprocIS.cal::def.cal.slct(metaCal = data_lessCol,
                                               TimeBgn = TimeBgn,
                                               TimeEnd = TimeEnd),
                  silent = TRUE)
            
            testthat::expect_true((class(dfReturned)[1] == "try-error"))
          })
