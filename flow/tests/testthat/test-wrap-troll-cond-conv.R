##############################################################################################
#' @title Unit test for wrapper of Missing Temp Flag and Conductivity Conversion
#' 
#' @description Wrapper function. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' For example:
#' Input path = pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681 with nested folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#'
#' @param SchmQf (optional) A json-formatted character string containing the schema for the flags output
#' by this function. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Corrected conductivity data and associated flags for missing temperature data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.cond.conv <- function(DirIn="~/pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681",
#'                               DirOutBase="~/pfs/out",
#'                               SchmDataOut="~/R/NEON-IS-avro-schemas/dp0p/aquatroll200_cond_corrected.avsc",
#'                               SchmQf="~/R/NEON-IS-avro-schemas/dp0p/flags_troll_specific_temp.avsc",
#'                               DirSubCopy=NULL,
#'                               log=log)
#'                               
#' 
#' @seealso None currently
#' 
#' 
# changelog and author contributions / copyrights
#   Mija Choi (2024-01-09)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.cond.conv.R\n")

# Unit test of wrap.troll.cond.conv.R
test_that("Unit test of wrap.troll.cond.conv.R", {
  
  source('../../flow.troll.cond.conv/wrap.troll.cond.conv.R')
  library(stringr)
  #
  testOutputBase = "pfs/out"
  #
  # Test 1. Only the input of empty directories in calibration/ and output directry are passed in
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDir <- 'pfs/prt_noDir_inCalibration/14491/2019/01/01'
  
  returnedOutputDir <-try(wrap.cal.conv.dp0p(DirIn = testInputDir, DirOutBase = testOutputBase),silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  #
  # Test 2. Only the input of directories, resistance and voltage, and output directry are passed in
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDir <- 'pfs/prt/14491/2019/01/01'
  testInputDataDir <- base::paste0(testInputDir, '/', 'data/')
  testOutputUncertCoefDir <- base::paste0(gsub("prt", "out", testInputDir), '/', 'uncertainty_coef/')
  
  fileData <- base::dir(testInputDataDir)
  fileUncertCoef <- base::dir(testOutputUncertCoefDir)
  
  returnedOutputDir <- wrap.cal.conv.dp0p(DirIn = testInputDir, DirOutBase = testOutputBase)
  
  testthat::expect_true (any(file.exists(testOutputBase, fileData, recursive = TRUE)))
  testthat::expect_true (any(file.exists(testOutputBase, fileUncertCoef, recursive = TRUE)))
  
  # Test 3. Avro schema has 'resistance', dataIn has 'resistance' and param, 'resistance', passed in
  # NumDayExpiMax has     var       |     NumDayExpiMax
  # --------------------------------------------------
  #                     resistance  |       2
  #                     voltage     |       3
  # --------------------------------------------------
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  FuncConv <- data.frame(var = 'resistance', FuncConv = 'def.cal.conv.poly', stringsAsFactors = FALSE)
  FuncUcrt <- data.frame(
    var = 'resistance',
    FuncUcrtMeas = 'def.ucrt.meas.cnst',
    FuncUcrtFdas = 'def.ucrt.fdas.rstc.poly'
    , stringsAsFactors = FALSE
  )
  
  ucrtCoefFdas  <-NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'testdata/fdas_calibration_uncertainty_general.json')
  SchmDataOutList <-NEONprocIS.base::def.schm.avro.pars(FileSchm = 'testdata/prt_calibrated.avsc')
  SchmQf <- base::paste0(base::readLines('testdata/flags_calibration.avsc'),collapse = '')
  
  DirCal = base::paste0(testInputDir, '/calibration')
  NameVarExpc = character(0)
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  values <- c(2, 3)
  NumDayExpiMax <- data.frame(var = varCal,NumDayExpiMax = values,stringsAsFactors = FALSE)
  
  returnedOutputDir <- wrap.cal.conv.dp0p(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  )
  
  testInputDataDir <- base::paste0(testInputDir, '/', 'data/')
  testOutputFlagsDir <-base::paste0(gsub("prt", "out", testInputDir), '/', 'flags/')
  testOutputUncertCoefDir <-base::paste0(gsub("prt", "out", testInputDir), '/', 'uncertainty_coef/')
  testOutputUncertDataDir <-base::paste0(gsub("prt", "out", testInputDir), '/', 'uncertainty_data/')
  
  fileData <- base::dir(testInputDataDir)
  fileFlags <- base::dir(testOutputFlagsDir)
  fileUncertCoef <- base::dir(testOutputUncertCoefDir)
  fileUncertData <- base::dir(testOutputUncertDataDir)
  
  testthat::expect_true (any(file.exists(testOutputBase, fileData, recursive = TRUE)))
  testthat::expect_true (any(file.exists(testOutputBase, fileFlags, recursive = TRUE)))
  testthat::expect_true (any(file.exists(testOutputBase, fileUncertCoef, recursive = TRUE)))
  testthat::expect_true (any(file.exists(testOutputBase, fileUncertData, recursive = TRUE)))
  
  # Test 3.a test an additional sub folder by passing DirSubCopy <- c("abc")
  #
  
  testInputDirSubCopy <- "pfs/prt_DirSubCopy/14491/2019/01/01"
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <- wrap.cal.conv.dp0p(
    DirIn = testInputDirSubCopy,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf,
    DirSubCopy <- c("abc")
  )
  
  testOutputAbcDir <- base::paste0(gsub("prt_DirSubCopy", "out", testInputDirSubCopy),'/','abc')
  fileAbc <- base::dir(testOutputAbcDir)
  
  testthat::expect_true (any(file.exists(testOutputAbcDir, fileAbc, recursive = TRUE)))
  #
  # Test 4. SchmDataOutList = NULL and the rest are same as in test 3.
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  FuncConv <- data.frame(var = 'voltage', FuncConv = 'def.cal.conv.poly', stringsAsFactors = FALSE)
  
  SchmDataOutList <- NULL
  
  returnedOutputDir <- try(wrap.cal.conv.dp0p(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = TRUE)
  #
  # Test 5. Avro schema has 'resistance', dataIn has 'resistance' and param, 'resistance', passed in
  # but the calibration has wrong folders, no_resistance and no_voltage
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  FuncConv <- data.frame(var = 'resistance', FuncConv = 'def.cal.conv.poly', stringsAsFactors = FALSE)
  
  testInputWrongDir <- "pfs/prt_wrong_dir_inCalibration/14491/2019/01/01"
  
  returnedOutputDir <- wrap.cal.conv.dp0p(
    DirIn = testInputWrongDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  )
  #
  # Test 6. Avro schema has 'resistance', dataIn has 'resistance' and param, 'voltage', passed in
  # err out due to 'voltage' missing from the data frame
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <- try(wrap.cal.conv.dp0p(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'voltage',
    NumDayExpiMax = unlist(NumDayExpiMax),
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
  # test 7. the test dir  has a wrong data, avro, not parquet
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputWrongDataDir <- "pfs/prt_wrong_data/14491/2019/01/01"
  
  returnedOutputDir <- try(wrap.cal.conv.dp0p(
    DirIn = testInputWrongDataDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    ucrtCoefFdas = ucrtCoefFdas,
    TermQf = 'resistance',
    NumDayExpiMax = NA,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
})
