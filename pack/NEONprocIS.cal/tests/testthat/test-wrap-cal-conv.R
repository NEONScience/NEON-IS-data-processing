##############################################################################################
#' @title Unit test of wrap.cal.conv.R, Wrapper for applying calibration conversion to NEON L0 data
#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Run unit tests for wrap.cal.conv.R, wrapper function.
#' @description
#' Wrapper function. Apply calibration conversion function to NEON L0 data, thus generating NEON
#' L0' data.

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time. 
#' The L0 data used for this tesing is retrievd from pfs/prt_calibration_filter/.
#' It is .parquet format and converted to data frame by def.read.parq(NameFile = "path to the parquet file")
#' It has "source_id","readout_time","site_id","resistance".
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
#' @param FuncConv A data frame of the variables for to convert and the function to convert
#' them with. Columns include:\cr
#' \code{var} Character. The variable in data for which to compute uncertainty, for example, "resistance" \cr
#' \code{FuncConv} A character string indicating the calibration conversion function
#' within the NEONprocIS.cal package that should be used. For most NEON data products, this will be
#' "def.cal.conv.poly". Note that any alternative function must accept arguments "data", "infoCal",
#' and "log", even if they are unused in the function, and must return a vector of converted data.
#' Input "data" to the function is an array of the data for the applicable term. Input "infoCal" is
#' a data frame of calibration information (including uncertainty) as returned from
#' NEONprocIS.cal::def.read.cal.xml. If no calibration files are associated with the term, infoCal
#' would be passed in to the function as NULL. Input "log" is a logger object as generated by
#' NEONprocIS.base::def.log.init and used in this script to generate hierarchical logging.\cr
#' @param DirCal Character string. Relative or absolute path (minus file name) to the main calibration
#' directory. Nested within this directory are directories for each variable in calSlct, each holding
#' calibration files for that variable. Defaults to "./"
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
  FuncConv = "def.cal.conv.poly"
  var = c("resistance")
  FuncConv <- data.frame(var = var, FuncConv = FuncConv, stringsAsFactors = FALSE)
  #
  Name = c("CVALA1", "CVALA2", "CVALA3")
  Value = c("0.9", "0.88", "0.77")
  cal <- data.frame(Name, Value, stringsAsFactors = FALSE)
  infoCal <- list(cal = cal)
  #
  DirCal = "./calibrations"
  NameVarExpc = character(0)
  
  # Test 1 There are calibration files available within the time range
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
  
  df_wcc <- NEONprocIS.cal::wrap.cal.conv (data, calSlct, FuncConv, log = NULL)
  
  expect_true (is.data.frame(df_wcc) & all(!(is.na(df_wcc[1:3, 1]))))
  
  cat("\n       |====== Test 1:::::::  infoCal is not empty and            ========================================|\n")
  cat("\n       |------       :::::::  and there is a calibration file available within the time range given ======|\n")
  cat("\n       |------ wrap.cal.conv returns the calibration information with                   ==================|\n")
  cat("\n       |------ the first 3 rows have meaningful values and the rest NA                  ==================|\n")
  cat("\n       |==================================================================================================|\n")
  
  # Test 2 There are no calibration files available within the time range

  TimeBgn = base::as.POSIXct('2020-06-12 00:10:20', tz = 'GMT')
  TimeEnd = base::as.POSIXct('2020-07-07 00:18:28', tz = 'GMT')
  NameVarExpc = character(0)

  calSlct <- NEONprocIS.cal::wrap.cal.slct (DirCal = DirCal,NameVarExpc = character(0),TimeBgn = TimeBgn,
                                            TimeEnd = TimeEnd,NumDayExpiMax = NumDayExpiMax,log = NULL)
  
  df_wcc <- NEONprocIS.cal::wrap.cal.conv (data, calSlct, FuncConv, log = NULL)
  
  expect_true (is.data.frame(df_wcc) & all((is.na(df_wcc[]))))
  
  cat("\n       |====== Test 2:::::::  infoCal is not empty, but           ========================================|\n")
  cat("\n       |------       :::::::  there is NO calibration file available within the time range given =========|\n")
  cat("\n       |------ wrap.cal.conv returns all NAs                                            ==================|\n")

 # Test 3  There are calibration files available within the time range, but infoCal is empty
  cat("\n       |====== Test 3:::::::  infoCal is empty                    ========================================|\n")
  cat("\n       |------       :::::::  and there is a calibration file available within the time range given ======|\n")
  
  infoCal <- NULL
  TimeBgn = base::as.POSIXct('2019-06-12 00:10:20', tz = 'GMT')
  TimeEnd = base::as.POSIXct('2019-07-07 00:18:28', tz = 'GMT')
  calSlct <-
    NEONprocIS.cal::wrap.cal.slct (
      DirCal = DirCal,
      NameVarExpc = character(0),
      TimeBgn = TimeBgn,
      TimeEnd = TimeEnd,
      NumDayExpiMax = NumDayExpiMax,
      log = NULL
    )
  df_wcc <- NEONprocIS.cal::wrap.cal.conv (data, calSlct, FuncConv, log = NULL)
})
