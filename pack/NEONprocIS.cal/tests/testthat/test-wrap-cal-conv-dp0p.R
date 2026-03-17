##############################################################################################
#' @title Unit test of wrap.cal.conv.dp0p.R, Wrapper for applying calibration conversion to NEON L0 data
#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Run unit tests for wrap.cal.conv.dp0p.R, wrapper function.
#' @description
#' Wrapper function. Apply calibration conversion function to NEON L0 data, thus generating NEON
#' L0' data.

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time. 
#' The L0 data used for this tesing is retrievd from pfs/prt_calibration_filter/.
#' It is .parquet format and converted to data frame by def.read.parq(NameFile = "path to the parquet file")
#' It has "source_id","readout_time","site_id","resistance".
#' 
#' @param calSlct A named list of data frames, list element corresponding to the variable for which
#' uncertainty coefficients are to be compiled. The data frame in each list element holds
#' information about the calibration files and time periods that apply to the variable, as returned
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' calSlct used for this testing will be generated. 
#' An example is, calSlct[1] will have "resistance" and calSlct[2] "voltage"
#' calSlct[1][1] will have 
#'           timeBgn             timeEnd             file              id      expi
#'           2019-06-12 00:10:20 2019-07-07 00:18:28 calibration22.xml 162922  FALSE
#'              
#' @param FuncConv A data frame indicating the calibration functions to apply and (optionally) the 
#' L0 terms to apply them to. The columns of the data frame are:
#' \code{FuncConv}: Character. The calibration conversion function within the NEONprocIS.cal package . Note that 
#' any and all calibration functions specified here must accept arguments "data", "infoCal", "varCal", "slctCal", 
#' "Meta", and "log", even if they are unused in the function. See any def.cal.conv.____.R 
#' \code{var}: Character. The name of the variable/term to be calibrated. Typically this will be a single L0 term matching
#' a column in the input data frame. However, it can be a term not found in the input data frame, multiple terms separated 
#' by pipes (e.g. "resistance|voltage") or no term at all (indicated by an NA). These uncommon cases are acceptable so long 
#' as the calibration conversion function is able to handle the case, for example if multiple L0 terms are used to create 
#' a single calibrated output. \cr
#' 
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the converted (calibrated) L0' data, limited to the variables in FuncConv.

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.func.poly}

#' @export
#'
#   Mija Choi (2020-05-20)
#     original creation
#   Mija Choi (2020-08-03)
#     Modified to reorganize the test input xml and json files
#   Cove Sturtevant (2025-12-01)
#     Adjust for changes to wrap.cal.conv.dp0p
##############################################################################################
# Define test context
context("\n                       calibration conversion wrapper\n")

# Test calibration conversion
test_that("testing calibration conversion wrapper", {
  
  # Create data to calibrate
  testDir = "testdata/"
  testData = "L0_data.csv"
  testDataPath <- paste0(testDir, testData)
  
  data <- read.csv(testDataPath, sep = ",", header = TRUE)
  data$readout_time <- as.POSIXct(data$readout_time, tz = 'GMT')

  # FuncConv
  FuncConv = c("def.cal.conv.poly","def.cal.conv.poly")
  var = c("resistance","resistance")
  FuncConv <- data.frame(var = var, FuncConv = FuncConv, stringsAsFactors = FALSE)
  #
  DirCal = "./calibrations"
  NameVarExpc = character(0)
  
  #### Test 1 Successful run through a single cal function
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd = base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  values <- c(10, 13)
  NumDayExpiMax <- data.frame(var = varCal, NumDayExpiMax = values, stringsAsFactors = FALSE)
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
  
  # Run through only the 1st cal function
  df_wcc <- NEONprocIS.cal::wrap.cal.conv.dp0p (data, calSlct, FuncConv[1,,drop=F], log = NULL)
  
  expect_true (is.data.frame(df_wcc))
  expect_true (all(!(is.na(df_wcc[1:3, 1]))))
  expect_equal(df_wcc$resistance[2],69.90183,tolerance=1E-5)
  expect_true(all(df_wcc$site_id=="BONA"))
  
  #### Test 2 Successful sequential run through two cal functions acting on same variable
  
  df_wcc <- NEONprocIS.cal::wrap.cal.conv.dp0p (data, calSlct, FuncConv, log = NULL)
  
  expect_true (is.data.frame(df_wcc))
  expect_true (all(!(is.na(df_wcc[1:3, 1]))))
  expect_equal(df_wcc$resistance[2],48.93128,tolerance=1E-5)
  expect_true(all(df_wcc$site_id=="BONA"))

  #### Test 3 Specify a function that does not exist in NEONprocIS.cal

  FuncConvBad <- FuncConv
  FuncConvBad$FuncConv[1] <- "def.cal.conv.no.exst"
  df_wcc <- try(NEONprocIS.cal::wrap.cal.conv.dp0p (data, calSlct, FuncConvBad, log = NULL))
  
  expect_true("try-error" %in% class(df_wcc))
  
})
