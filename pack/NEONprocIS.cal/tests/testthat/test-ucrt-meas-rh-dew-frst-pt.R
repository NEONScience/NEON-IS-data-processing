##############################################################################################
#' @title Unit test of NEON uncertainty quantification (def.ucrt.meas.rh.dew.frst.pt)

#' @author
#' Robert Markel \email{rmarkel@BattelleEcology.org}
#' Mija Choi \email{choim@batelleEcology.org}
#' Cove Sturtevant \email{csturtevant@batelleEcology.org}

#' @description
#' Run unit tests for uncertainty quantification function. The unit tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' @param data Data frame of raw, ununcertainty measurements. This data frame must have a column
#' called "readout_time" with POSIXct timestamps
#' @param varUcrt A character array of the target variables (columns) in the data frame \code{data} for 
#' which uncertainty output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv.dp0p for what this input is.

#' @return TRUE when a test passes. Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS uncertainty Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2021-02-12)
#     Original Creation
#   Cove Sturtevant (2025-11-17)
#     Revise unit test for changed inputs/outputs
##############################################################################################
# Define test context
context("\n                       uncertainty quantification\n")

# Test calibration conversion
test_that("testing uncertainty quantification (def.ucrt.meas.rh.dew.frst.pt", {
  
  TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
  TimeEnd <- base::as.POSIXct('2019-01-03',tz='GMT')

  calSlct = list(
    "temperature" = data.frame(
      timeBgn = as.POSIXct(c("2019-01-01","2019-01-02"), tz = "GMT"),
      timeEnd = as.POSIXct(c("2019-01-02","2019-01-03"), tz = "GMT"),
      file = c("testdata/temperature/30000000000080_WO29705_157555.xml",
               "testdata/temperature/30000000000080_WO29705_157555_validAfter.xml"),
      id = c(157555,157555),
      expi = c(FALSE,FALSE)
    ),
    "relative_humidity" = data.frame(
      timeBgn = as.POSIXct(c("2019-01-01","2019-01-02"), tz = "GMT"),
      timeEnd = as.POSIXct(c("2019-01-02","2019-01-03"), tz = "GMT"),
      file = c("testdata/relHumidity/30000000000080_WO29705_157554.xml",
               "testdata/relHumidity/30000000000080_WO29705_157554_validAfter.xml"),
      id = c(157554,157554),
      expi = c(FALSE,FALSE)
    )
  )
  
  # Create data to calibrate
  data <- data.frame(
    dew_point = c(1, -1, 5, 4, 4.5),
    temperature = c(2, -3, 6, 8, 5),
    relative_humidity = c(1, 6, 7, 0, 10),
    readout_time = as.POSIXct(
      c(
        '2019-01-01 00:00',
        '2019-01-01 04:01',
        '2019-01-01 06:02',
        '2019-01-01 08:01',
        '2019-01-02 00:00'
      ),tz = 'GMT'
    )
  )
  
  ##########
  ##########  Happy paths:::: data and ucrt not empty and have valid values
  ##########
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values for temp > 0    |\n")

  uncertainty <-
    NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, calSlct=calSlct)

  # Check the data inside the valid date range are uncertainty correctly
  ucrtExpc <- c(5.8635613,0.03618669,5.8634497)
  names(ucrtExpc) <- c('ucrtMeas','ucrt_dfpt_t_L1','ucrt_dfpt_rh_L1')
  testthat::expect_equal(ucrtExpc, unlist(uncertainty$dew_point[1,]),tolerance=1E-6)
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and have valid values for temp < 0    |\n")
  
  # Check the data inside the valid date range are uncertainty correctly
  ucrtExpc <- c(1.0193368,0.04443399,1.0183679)
  names(ucrtExpc) <- c('ucrtMeas','ucrt_dfpt_t_L1','ucrt_dfpt_rh_L1')
  testthat::expect_equal(ucrtExpc, unlist(uncertainty$dew_point[2,]),tolerance=1E-6)
  
  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ data and cal are not empty and return NaN values for rh=0    |\n")
  
  # Check the data inside the valid date range are uncertainty correctly
  ucrtExpc <- c(NaN,0,NaN)
  names(ucrtExpc) <- c('ucrtMeas','ucrt_dfpt_t_L1','ucrt_dfpt_rh_L1')
  testthat::expect_equal(ucrtExpc, unlist(uncertainty$dew_point[4,]),tolerance=1E-6)

  cat("\n       |====== Positive test::                         ==========|\n")
  cat("\n       |------ valid calibration date range inclusive of start date, exclusive of end date    |\n")
  

  # Check the last date, which falls on the boundary of the valid cal periods
  # Last date should get the second cal
  ucrtExpc <- c(1.2068974,0.03805616,1.2062972)
  names(ucrtExpc) <- c('ucrtMeas','ucrt_dfpt_t_L1','ucrt_dfpt_rh_L1')
  testthat::expect_equal(ucrtExpc, unlist(uncertainty$dew_point[5,]),tolerance=1E-6)
  
  

  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ data is before the valid date range of the cal. Return NA values. |\n\n")
  
  data2 <- data
  data2$readout_time <- as.POSIXct(c('2018-06-13','2018-06-14','2018-06-15','2018-06-16','2018-06-17'),tz='GMT')
  
  uncertainty <- NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data2, calSlct=calSlct)
  
  testthat::expect_true(all(is.na(unlist(uncertainty$dew_point))))
  
  
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cals specified for 'data'. Returns NA |\n\n")
  calSlctNoVar <- calSlct
  names(calSlctNoVar) <- c('temperature','voltage')
  uncertainty <- NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, 
                                                              calSlct=calSlctNoVar)
  testthat::expect_true(all(is.na(unlist(uncertainty$dew_point))))
  
  calSlctNoVar <- calSlct
  names(calSlctNoVar) <- c('voltage','relative_humidity')
  uncertainty <- NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, 
                                                              calSlct=calSlctNoVar)
  testthat::expect_true(all(is.na(unlist(uncertainty$dew_point))))
  
  #
  cat("\n       |======= Positive test::                      ============|\n")
  cat("\n       |------ No cal available for a period of data. return NA.              |\n\n")
  #
  
  calSlctNoFile <- calSlct
  calSlctNoFile$temperature$file[1] = NA
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, calSlct=calSlctNoFile), silent = TRUE)
  
  testthat::expect_true(all(is.na(unlist(uncertainty$dew_point[1:4,]))))
  
  calSlctNoFile <- calSlct
  calSlctNoFile$relative_humidity$file[1] = NA
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, calSlct=calSlctNoFile), silent = TRUE)
  
  testthat::expect_true(all(is.na(unlist(uncertainty$dew_point[1:4,]))))
  
  
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ Cannot compute uncertainty for character variable   |\n\n")
  #


  data$dew_point_char <- as.character(data$dew_point)
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, varUcrt='dew_point_char', calSlct=calSlct), silent = TRUE)
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ cal is has no applicable uncertainty coefficients.              |\n\n")
  #
  
  calSlctBadTemp <- calSlct
  calSlctBadTemp$temperature$file[1] = "calibrations/voltage/calibration44.xml"
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, calSlct=calSlctBadTemp), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  calSlctBadRh <- calSlct
  calSlctBadRh$relative_humidity$file[1] = "calibrations/voltage/calibration44.xml"
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data, calSlct=calSlctBadRh), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))

  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ data missing readout_time variable    |\n\n")
  
  data2 <- data[,setdiff(names(data),'readout_time')]
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt(data = data2, calSlct=calSlct), silent = TRUE)
  
  testthat::expect_true((class(uncertainty)[1] == "try-error"))
  
  #
  cat("\n       |======= Negative test::                      ============|\n")
  cat("\n       |------ readout_time not POSIXt    |\n\n")
  data$readout_time <- as.character(data$readout_time)
  uncertainty <- try(NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt (data = data, 
                                                       calSlct=calSlct),
                    silent=TRUE)
  testthat::expect_true ("try-error" %in% class(uncertainty))
  
  
  
})
