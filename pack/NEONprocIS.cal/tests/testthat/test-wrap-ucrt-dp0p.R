##############################################################################################
#' @title Unit test of wrap.ucrt.dp0p.R,
#' wrapper for computing individual measurement uncertainty for calibrated data at native
#' frequency (NEON L0' data).

#' @description
#' Run unit tests for wrap.ucrt.dp0p.R. The input json for the test is ucrt-coef-fdas-input.json.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to wrap.ucrt.dp0p.R for the details of the function.

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.
#' @param FuncUcrt A data frame of the functions and variables for which individual measurement 
#' and/or FDAS uncertainty is to be calculated. Columns include:\cr
#' \code{FuncUcrt} A character string indicating the individual measurement (calibration) or FDAS 
#' uncertainty function within the NEONprocIS.cal package. For most NEON data products, 
#' this will be "def.ucrt.meas.cnst" or "def.ucrt.meas.mult" for measurement/calibration 
#' uncertainty, and "def.ucrt.fdas.rstc.poly" or "def.ucrt.fdas.volt.poly" for FDAS 
#' (data acquisition system) uncertainty. Note that any alternative function must accept 
#' the same arguments as these functions, even if they are unused, and return the same 
#' output format. See one of those functions for details. \cr
#' \code{var} Character. The variable(s) in input data frame 'data' that will be used in the 
#' uncertainty function specified in FuncUcrt. In most cases, this will be a single L0 
#' variable for which to compute uncertainty, but it can be any character string so long 
#' as the specified (custom) uncertainty function knows what to do with it. Note that the 
#' uncertainty function is responsible for naming the output list containing 
#' uncertainty data frames for each variable, and that any overlap in the names across 
#' the output list will cause the uncertainty data frames to be combined (intentionally -
#' see return information). Thus, ensure that the column names of data frames for the 
#' same variable (list name) are unique. In the standard measurement and FDAS uncertainty functions, 
#' the output list names will match the name of the L0 variable specified in \code{var}.\cr
#' @param calSlct A named list of data frames, list elements typically corresponding to the variables in
#' FuncUcrt$var. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable.
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' Note that the standard fdas uncertainty functions require fdas uncertainty coefficients to be 
#' provided in Meta$ucrtCoefFdas.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to those in ParaUcrt$var and holding a data
#' frame of uncertainty data. Note that each row in each data frame corresponds to the times in
#' data$readout_time, but the variable readout_time is not included in the output. One column
#' in each data frame is labeled ucrtComb, corresponding to the combined measurement uncertainty
#' of the individual measurements and FDAS (if applicable). If FDAS uncertainty does not apply,
#' ucrtComb is simply a copy of ucrtMeas. \cr

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.cal")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.cal")

# changelog and author contributions / copyrights
#   Mija Choi (2020-08-05)
#     Original Creation
#   Mija Choi (2020-09-24)
#     adjusted calls to uncertainty funcs to conform to new generic format
#     This includes inputting the entire data frame, the
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
#     Changed input to also specify the FDAS uncertainty function to use, instead of
#     determining it within the code
#     Changed input argument ParaUcrt to FuncUcrt, and changed input column names to support above changes
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.dp0p.R\n")

# Unit test of wrap.ucrt.dp0p.R
test_that("Unit test of wrap.ucrt.dp0p.R", {
   # The input json has Name, Value, and .attrs
   
   testDir = "testdata/"
   testJson = "ucrt-coef-fdas-input.json"
   testJsonPath <- paste0(testDir, testJson)
   
   Meta <- list()
   Meta$ucrtCoefFdas <- NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = testJsonPath)
   
   testFileCal = "calibration.xml"
   testFileCalPath <- paste0(testDir, testFileCal)
   
   infoCal <- NEONprocIS.cal::def.read.cal.xml (testFileCalPath, Vrbs = TRUE)
   
   DirCal = "./calibrations"
   NameVarExpc = character(0)
   TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
   TimeEnd = base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
   
   varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
   
   values <- c(10, 13)
   
   NumDayExpiMax <- data.frame(var = varCal,NumDayExpiMax = values,stringsAsFactors = FALSE)
   #
   calSlct <- NEONprocIS.cal::wrap.cal.slct (
      DirCal = DirCal,
      NameVarExpc = character(0),
      TimeBgn = TimeBgn,
      TimeEnd = TimeEnd,
      NumDayExpiMax = NumDayExpiMax,
      log = NULL
   )
   
   # Happy path 1 - test calibration in resistance
   #
   testData = "L0_data_resistance.csv"
   testDataPath <- paste0(testDir, testData)
   
   data <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   data$readout_time <- as.POSIXct(data$readout_time, tz = 'GMT')
   data = data.frame(data)
   
   FuncUcrtMeas = "def.ucrt.meas.cnst"
   FuncUcrtFdas = "def.ucrt.fdas.rstc.poly"
   var = c("resistance")
   
   FuncUcrt <-
      data.frame(
         var = rep(var,2),
         FuncUcrt = c(FuncUcrtMeas,FuncUcrtFdas),
         stringsAsFactors = FALSE
      )
   
   nameVarIn = c('resistance')
   nameVarOut = c('resistance')
   nameVarDfltSame = c('resistance')
   
   mappNameVar <-
      base::data.frame(
         nameVarIn = nameVarIn,
         nameVarOut = nameVarOut,
         stringsAsFactors = FALSE
      )
   newVar <- nameVarDfltSame[!(nameVarDfltSame %in% nameVarIn)]
   mappNameVar <-
      base::rbind(
         mappNameVar,
         base::data.frame(
            nameVarI = newVar,
            nameVarOut = newVar,
            stringsAsFactors = FALSE
         )
      )
   
   wudp0pList_returned <-
      NEONprocIS.cal::wrap.ucrt.dp0p (
         data,
         FuncUcrt,
         Meta = Meta,
         calSlct = calSlct
      )
   
   elementsList = c(
      "ucrtMeas",
      "raw",
      "dervCal",
      "ucrtFdas",
      "ucrtComb",
      "ucrtExpn"
   )
   
   expect_true ((is.list(wudp0pList_returned)))
   expect_true(!(is.null(wudp0pList_returned)))
   expect_true(all(names(wudp0pList_returned$resistance) == elementsList))
   expect_true(!any(is.na(wudp0pList_returned$resistance[1,])))
   

   #  Happy path 2 - calibration xml selected has the time expired
   #
   TimeBgn = base::as.POSIXct('2020-06-12 00:10:20', tz = 'GMT')
   TimeEnd = base::as.POSIXct('2020-07-07 00:18:28', tz = 'GMT')
   
   #
   calSlct <- NEONprocIS.cal::wrap.cal.slct (
      DirCal = DirCal,
      NameVarExpc = character(0),
      TimeBgn = TimeBgn,
      TimeEnd = TimeEnd,
      NumDayExpiMax = NumDayExpiMax,
      log = NULL
   )
   
   wudp0pList_returned <-
      NEONprocIS.cal::wrap.ucrt.dp0p (
         data,
         FuncUcrt,
         calSlct = calSlct,
         Meta = Meta
      )
   
   expect_true ((is.list(wudp0pList_returned)))
   expect_true(!(is.null(wudp0pList_returned)))
   expect_true(all(names(wudp0pList_returned$resistance) == elementsList))
   expect_true(sum(is.na(wudp0pList_returned$resistance[1,]))==5)
   expect_true(all(!is.na(wudp0pList_returned$resistance$raw)))
   
   
   # Negative test - no functions input
   wudp0pList_returned <-
      NEONprocIS.cal::wrap.ucrt.dp0p (
         data,
         FuncUcrt=data.frame(),
         calSlct = calSlct,
         Meta = Meta
      )
   expect_true(is.list(wudp0pList_returned))
   expect_true(length(wudp0pList_returned)==0)
   
})
