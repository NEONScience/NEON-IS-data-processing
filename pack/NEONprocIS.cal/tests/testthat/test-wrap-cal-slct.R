##############################################################################################
#' @title Unit test of def.wrap.cal.slct.R, Wrapper for selecting the applicable calibrations and their time ranges for all variables

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Run unit tests for wrap.cal.slct.R,wrapper function.
#' Select the calibrations and their time ranges that apply for each selected variable.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to def.wrap.cal.slct.R for the details of the function.
#' @param DirCal Character string. Relative or absolute path (minus file name) to the main calibration
#' directory. Nested within this directory are directories for each variable, each holding
#' calibration files for that variable. Defaults to "./"
#' @param NameVarExpc Character vector of minimum variables for which to supply calibration information
#' (even if there are no applicable calibrations). Default to character(0), which will return cal info
#' for only the variable directories found in DirCal.
#' @param TimeBgn A POSIXct timestamp of the start date of interest (inclusive)
#' @param TimeEnd A POSIXct timestamp of the end date of interest (exclusive)
#' @param NumDayExpiMax A data frame indicating the max days since expiration that calibration
#' information is still considered usable for each variable. Calibrations beyond this allowance period
#' are treated as if they do not exist. Columns in this data frame are:\cr
#' \code{var} Character. Variable name.\cr
#' \code{NumDayExpiMax} Numeric. Max days after expiration that a calibration is considered usable.\cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to the variables found in the DirCal directory
#' and holding a data frame of selected calibrations for the time range of interst as output by
#' NEONprocIS.cal::def.cal.slct. See that function for details.

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.meta}
#'
#   Mija Choi (2020-04-14)
#     original creation
#   Mija Choi (2020-05-20)
#     modified the text of context and test_that
##############################################################################################
# Define test context
context("\n       testing the applicable calibrations selection and their time ranges for all variables\n")

# Test calibration conversion
test_that("testing calibration selection", {
  # Create data to calibrate
  DirCal = "./calibrations"
  NameVarExpc = character(0)
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd =  base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  
  values <- c(10, 13)
  
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = values, stringsAsFactors = FALSE)

  ##########
  ##########  Happy path 1:::: Calibrations folder has cal files and the expirations have valid values
  ##########
  
  wcsList <- NEONprocIS.cal::wrap.cal.slct (DirCal = DirCal,NameVarExpc = character(0),TimeBgn = TimeBgn,
    TimeEnd = TimeEnd,NumDayExpiMax = NumDayExpiMax,log = NULL)
  #
  ID = 162922
  
  testthat::expect_true  (is.list(wcsList))
  
  if (!(length(wcsList) == 0)) {
    testthat::expect_true (wcsList[[1]][5] == ID)
  }
  
  cat("\n       |====== Positive test 1::                                    =========================|\n")
  cat("\n       |------ Calibrations folder has cal files and the expirations have valid values       |\n")
  cat("\n       |------ Calibration selection ran successfully!                                       |\n")
  cat("\n       |=====================================================================================|\n")
  
  ##########
  ##########  Happy path 2:::: calibrations has calibration files but the expiration days have NA
  ##########
  
  DirCal = "./calibrations"
  NameVarExpc = character(0)
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd =  base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  
  values <- c(NA, NA)
  
  NumDayExpiMax <- data.frame(var = varCal,NumDayExpiMax = values,stringsAsFactors = FALSE)
  
  wcsList <- NEONprocIS.cal::wrap.cal.slct (DirCal = DirCal,NameVarExpc = character(0),TimeBgn = TimeBgn,
                                            TimeEnd = TimeEnd,NumDayExpiMax = NumDayExpiMax,log = NULL)
  testthat::expect_true (is.list(wcsList))
  
  if (!(length(wcsList) == 0)) {
    testthat::expect_true (wcsList[[1]][5] == ID)
  }
  cat("\n       |====== Positive test 2::                                        =====================|\n")
  cat("\n       |------ calibrations has calibration files but the expiration days have NA            |\n")
  cat("\n       |------ Calibration selection ran successfully with TimeExpiMax=NULL!                 |\n")
  cat("\n       |=====================================================================================|\n")

  ##########
  ##########  Sad path 1:::: calibrations has sub folders, but no calibration files
  ##########
  
  DirCal = "./calibrations_noCals"
  NameVarExpc = character(0)
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd =  base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  
  values <- c(21, 22)
  
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = values, stringsAsFactors = FALSE)
  
  wcsList <- NEONprocIS.cal::wrap.cal.slct (DirCal = DirCal,NameVarExpc = character(0),TimeBgn = TimeBgn, TimeEnd = TimeEnd,
    NumDayExpiMax = NumDayExpiMax, log = NULL)
  
  testthat::expect_true ((wcsList[[1]]$timeBgn == TimeBgn) && (wcsList[[1]]$timeEnd == TimeEnd))
  
  cat("\n       |====== Negative test 1::                                        =====================|\n")
  cat("\n       |------ calibrations has no calibration files with expiration days                    |\n")
  cat("\n       |------ Calibration selection ran successfully, but returns TimeBgn and TineEnd only! |\n")
  cat("\n       |=====================================================================================|\n")
  
  ##########  Sad path 2:::: calibrations has no sub folders
  
  DirCal = "./calibrations_noSubFldr"
  NameVarExpc = character(0)
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd =  base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  
  values <- c()
  
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = values, stringsAsFactors = FALSE)
  
  wcsList <- NEONprocIS.cal::wrap.cal.slct (DirCal = DirCal,NameVarExpc = character(0),TimeBgn = TimeBgn, TimeEnd = TimeEnd,
                                            NumDayExpiMax = NumDayExpiMax, log = NULL)
  
  testthat::expect_true (is.list(wcsList) && rapportools::is.empty(unlist(wcsList)))
  
  cat("\n       |====== Negative test 2::                                        =====================|\n")
  cat("\n       |------ calibrations has no sub folders                                               |\n")
  cat("\n       |------ Calibration selection ran successfully, but returns a null list               |\n")
  cat("\n       |=====================================================================================|\n")
  
  
})
