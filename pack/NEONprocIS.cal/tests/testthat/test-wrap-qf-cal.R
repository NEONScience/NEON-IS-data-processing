##############################################################################################
#' @title Unit test for wrap-qf-cal.R, wrapper for computing calibration flags for all variables and time ranges
#'
#' @author
#' Mija Choi \email{choim@battelleecology.org}
#'
#' @description
#' wrap-qf-cal.R: Wrapper function. Compute valid calibration and suspect calibration flags for NEON L0 data.
#'
#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.
#' @param calSlct A named list of data frames, list element corresponding to the variable for which
#' uncertainty coefficients are to be compiled. The data frame in each list element holds
#' information about the calibration files and time periods that apply to the variable, as returned
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function.
#' @param DirCal Character string. Relative or absolute path (minus file name) to the main calibration
#' directory. Nested within this directory are directories for each variable in calSlct, each holding
#' calibration files for that variable. Defaults to "./"
#' @param mappNameVar A data frame with in/out variable name mapping as produced by
#' NEONprocIS.base::def.var.mapp.in.out. See documentation for that function.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @return A named list of qfExpi and qfSusp, each holding data frames with the same dimension as data,
#' with the exception that the variable readout_time is removed.\cr
#' \code{qfExpi} Integer. The expired/valid calibration flag. 0 = valid, non expired calibration available;
#' 1 = no calibration or expired calibration available. \cr
#' \code{qfSusp} Integer. The suspect calibration flag. 0 = calibration not suspect, 1 = calibration suspect,
#' -1 = no cal to evaluate

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.qf.cal.susp}
#'
#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")
#'
# changelog and author contributions / copyrights
#   Mija Choi (2020-09-02)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap-qf-cal.R\n")

# Unit test of wrap-qf-cal.R
test_that("Unit test of wrap-qf-cal.R", {
   testDir = "testdata/"
   
   testData = "L0_data.csv"
   testDataPath <- paste0(testDir, testData)
   
   data <- read.csv(testDataPath, sep = ",", header = TRUE)
   data$readout_time = base::as.POSIXct(data$readout_time, tz = 'GMT')
   # Happy path 1 -
   
   testFileCal = "calibration2.xml"
   testFileCalPath <- paste0(testDir, testFileCal)
   
   infoCal <-
      NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   DirCal = "./calibrations"
   NameVarExpc = character(0)
   
   # Test 1 There are calibration files available within the time range
   
   TimeBgn = base::as.POSIXct('2019-06-11 00:10:20', tz = 'GMT')
   TimeEnd = base::as.POSIXct('2019-07-09 00:18:28', tz = 'GMT')
   
   varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
   values <- c(10, 13)
   
   NumDayExpiMax <- data.frame(var=varCal,NumDayExpiMax=values,stringsAsFactors=FALSE)
   #
   calSlct <-
      NEONprocIS.cal::wrap.cal.slct (
         DirCal = DirCal,
         NameVarExpc = character(0),
         TimeBgn = TimeBgn,
         TimeEnd = TimeEnd,
         NumDayExpiMax = NumDayExpiMax,
         log = NULL
      )

   wrapQfCal <- NEONprocIS.cal::wrap.qf.cal (data=data, calSlct=calSlct,mappNameVar=NULL, log=NULL)
   
   expiredTrue = as.integer(0)
   suspectFalse = as.integer(0)
   expect_true ((is.list(wrapQfCal)) &&
                   ((wrapQfCal$qfExpi$resistance[1]) == expiredTrue) &&
                   (wrapQfCal$qfSusp$resistance[1] == suspectFalse))
   
   # Sad Path - no calibration selected with the time range
   
   TimeBgn = base::as.POSIXct('2020-08-11 00:10:20', tz = 'GMT')
   TimeEnd = base::as.POSIXct('2020-09-09 00:18:28', tz = 'GMT')
   
   varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
   values <- c(10, 13)
   
   NumDayExpiMax <- data.frame(var=varCal,NumDayExpiMax=values,stringsAsFactors=FALSE)
   #
   calSlct <-
      NEONprocIS.cal::wrap.cal.slct (
         DirCal = DirCal,
         NameVarExpc = character(0),
         TimeBgn = TimeBgn,
         TimeEnd = TimeEnd,
         NumDayExpiMax = NumDayExpiMax,
         log = NULL
      )
   
   
   wrapQfCal <- NEONprocIS.cal::wrap.qf.cal (data=data, calSlct=calSlct,mappNameVar=NULL, log=NULL)
   
   expiredTrue = as.integer(1)
   noCalToEval = as.integer(-1)
   expect_true ((is.list(wrapQfCal)) &&
                   ((wrapQfCal$qfExpi$resistance[1]) == expiredTrue) &&
                   (wrapQfCal$qfSusp$resistance[1] == noCalToEval))
})
