##############################################################################################
#' @title Unit test of wrap.ucrt.coef.R

#' @description
#' Run unit tests for wrap.ucrt.coef.R. The input json for the test is ucrt-coef-fdas-input.json.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to wrap.ucrt.coef.R for the details of the function.

#' @param calSlct Required for the unit test. A named list of data frames, list element corresponding to the variable for which
#' uncertainty coefficients are to be compiled. The data frame in each list element holds
#' information about the calibration files and time periods that apply to the variable, as returned
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function.
#' @param DirCal Required for the unit test. Character string. Relative or absolute path (minus file name) to the main calibration
#' directory. Nested within this directory are directories for each variable in calSlct, each holding
#' calibration files for that variable. Defaults to "./"
#' @param ParaUcrt A data frame indicating which (if any) type of FDAS uncertainty coefficients
#' apply to each variable. Columns must include:\cr
#' \code{var} Character. The variable/term name (same as those in calSlct)\cr
#' \code{typeFdas} A single character indicating the type of measurement made by NEON's field
#' data acquisision system (GRAPE). Acceptable values are "R" for resistance measurements, "V"
#' for voltage measurements, or NA (no quotes) for measurements in which FDAS uncertainty does
#' not apply (e.g. digital L0 output). \cr
#' A NULL entry for ParaUcrt (default) or variables missing from ParaUcrt indicate that FDAS
#' uncertainty does not apply.
#' @param ucrtCoefFdas Required for the unit test. A data frame of FDAS uncertainty coefficients, as read by
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr
#' Defaults to NULL, in which case no variables in ParaUcrt may indicate that FDAS uncertainty
#' applies.
#' @param mappNameVar A data frame with in/out variable name mapping as produced by
#' NEONprocIS.base::def.var.mapp.in.out. See documentation for that function. If input (default is
#' NULL), input variable names in the output data frames will be replaced by their corresponding
#' output name.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to those in ParaUcrt$var and holding a data
#' frame of uncertainty coefficients and applicable time ranges.  \cr

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
#   Mija Choi (2020-06-03)
#     Original Creation
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.ucrt.coef.R\n")

# Unit test of wrap.ucrt.coef.R
test_that("Unit test of wrap.ucrt.coef.R", {
   # The input json has Name, Value, and .attrs
   
   testDir = "testdata/"
   testJson = "ucrt-coef-fdas-input.json"
   testJsonPath <- paste0(testDir, testJson)
   
   rucfDf_returned <- NEONprocIS.cal::def.read.ucrt.coef.fdas (NameFile = testJsonPath, log =  NULL)
   
   testData = "L0_data.csv"
   testDataPath <- paste0(testDir, testData)
   
   data <- read.csv(testDataPath, sep = ",", header = TRUE)
   
   data$readout_time <- as.POSIXct(data$readout_time, tz = 'GMT')
   # FuncConv
   FuncConv = "def.cal.conv.poly"
   var = c("resistance")
   FuncConv <- data.frame(var = var, FuncConv = FuncConv, stringsAsFactors = FALSE)
   #
   Name = c("CVALA1", "CVALA2", "CVALA3")
   Value = c("0.9", "0.88", "0.77")
   cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
   infoCal <- list(cal = cal)
   
   DirCal = "./calibrations"
   NameVarExpc = character(0)
   TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
   TimeEnd = base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
   
   varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
   
   values <- c(10, 13)
   
   NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = values, stringsAsFactors = FALSE)
   #
   calSlct <- NEONprocIS.cal::wrap.cal.slct (
         DirCal = DirCal,
         NameVarExpc = character(0),
         TimeBgn = TimeBgn,
         TimeEnd = TimeEnd,
         NumDayExpiMax = NumDayExpiMax,
         log = NULL
      )
   
   # Test 1 - pass minimun parameters, i.e., NULL for ParaUcr and mappNameVar
   wucList_returned <-
      NEONprocIS.cal::wrap.ucrt.coef (calSlct, 
                                      ucrtCoefFdas = rucfDf_returned
                                      )
   
   elementsList = c('id', 'timeBgn','timeEnd', 'file', 'expi', 'Name', 'Value', '.attrs', 'var')
   
   expect_true ((is.list(wucList_returned)) &&
                   !(is.null(wucList_returned)) &&
                   all((
                      names(wucList_returned$resistance) == elementsList
                   ))
                )
   
   # Test 2 - pass non NULL for all parameters except log
   
   fdas = c("R", "V")
   var2 = c("resistance", "voltage")
   ParaUcrt <- data.frame(var = var2, typeFdas = fdas, stringsAsFactors = FALSE)
   
   nameVarIn = c('resistance', 'voltage')
   nameVarOut = c('resistance', 'voltage')
   nameVarDfltSame = c('resistance', 'voltage')
   
   mappNameVar <- base::data.frame(nameVarIn = nameVarIn, nameVarOut = nameVarOut, stringsAsFactors = FALSE)
   newVar <- nameVarDfltSame[!(nameVarDfltSame %in% nameVarIn)]
   mappNameVar <- base::rbind(mappNameVar, base::data.frame( nameVarIn = newVar, nameVarOut = newVar, stringsAsFactors = FALSE))
   
   #
   wucList_returned <-
      NEONprocIS.cal::wrap.ucrt.coef (
         calSlct,
         ucrtCoefFdas = rucfDf_returned,
         mappNameVar = mappNameVar
      )
   
   expect_true ((is.list(wucList_returned)) &&
                   !(is.null(wucList_returned)) &&
                   all((
                      names(wucList_returned$resistance) == elementsList
                   )))
   
})
