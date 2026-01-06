########################################################################################################
#' @title Unit test of wrap.cal.conv.R, calibration conversion module for NEON IS data processing

#' @author
#' Mija Choi\email{choim@battelleecology.org} \cr
#'
#' @description wrap.cal.conv.R is to apply calibration and uncertainty functions to L0 data and save applicable
#' uncertainty coefficients. Optionally compute FDAS (datalogger) uncertainty.
#' Valid date ranges and certificate numbers in calibration files are used to determine the most relevant calibration to apply.
#' The most relevant cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#'
#' Note that calibrations with a valid date range beginning after the date range of interest and
#' calibrations that are expired more than their max allowable days since expiration are treated
#' as if they don't exist. Data points are turned to NA if no valid or expired valibration is found.
#' Quality flags are output indicating whether an expired calibration was used.
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, where # indicates any number of parent and child directories
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /calibration/STREAM
#' The data folder holds a single daily data file corresponding to the yyyy/mm/dd in the input path.
#' The STREAM folder(s) may be any name and there may be any number of STREAM folders at this level,
#' each containing the calibration files applicable to STREAM.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01 with nested folders:
#'    /data
#'    /calibration/soilPRTResistance
#'    /calibration/heaterVoltage
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param FuncConv (optional) A data frame indicating the calibration functions to apply and (optionally) the 
#' L0 terms to apply them to. The columns of the data frame are:
#' \code{FuncConv}: Character. The calibration conversion function within the NEONprocIS.cal package . Note that 
#' any and all calibration functions specified here must accept arguments "data", "varCal", "slctCal", 
#' "Meta", and "log", even if they are unused in the function. See any def.cal.conv.____.R 
#' function in the NEONprocIS.cal package for explanation of these inputs, but in short, the entire input data frame and 
#' available calibration information are passed into each calibration function. 
#' \code{var}: Character. The name of the variable/term to be calibrated. Typically this will be a single L0 term matching
#' a column in the input data frame. However, it can be a term not found in the input data frame, multiple terms separated 
#' by pipes (e.g. "resistance|voltage") or no term at all (indicated by an NA). These uncommon cases are acceptable so long 
#' as the calibration conversion function is able to handle the case, for example if multiple L0 terms are used to create 
#' a single calibrated output. \cr
#'
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
#'
#' @param TermQf (optional) A character vector of L0 terms/variables for which to provide calibration
#' flags. For example, if calibration information is expected for the terms "resistance" and
#' "voltage", then enter c("resistance","voltage"). Terms listed here should match the names of the
#' expected subfolders in the calibration directory. If no subfolder exists matching the term names here, the valid
#' calibration flag will be 1, and the suspect calibration flag will be -1. Note that the output flags can be renamed 
#' as necessary in \code{SchmQf}.
#'
#' @param NumDayExpiMax (optional) A single numeric value or data frame indicating the max days since expiration that
#' calibration information is still considered usable for each L0 data term. Calibrations beyond this allowance period are treated as if they do
#' not exist. Thus, if no other applicable calibration file exists, calibrated values will be NA and uncertainty coefficients will
#' not be recorded for these periods. Note that use of expired calibration information or the lack of any usable calibration
#' information will always cause the expired/valid calibration flag to be raised, regardless of the values indicated here.
#' If a single value is entered, it will be used for all calibrations found in the calibration directory. The default is NA, which
#' indicates that calibrations are usable for an unlimited period after expiration. Enter a data frame to
#' specify a value for each term expected to have calibration information. The columns of the data frame are:
#' \code{var}: Character. The name of the L0 variable/term for which calibration information is expected. Thus is should
#' include, at a minimum, the variables indicated in inputs \code{FuncConv$var}, \code{TermQf}, and \code{FuncUcrt$var}. Any
#' calibrations found for terms not listed here will usable for an unlimited period past expiration. \cr
#' \code{NumDayExpiMax}: Numeric. The numeric days after the expiration of the calibration file that the information contained
#' in the calibration file is still usable. A value of NA indicates that calibrations are usable for an unlimited
#' period past expiration.
#'
#' @param SchmDataOutList (optional) The list output from parsing the schema for the calibrated output, as generated
#' from NEONprocIS.base::def.schm.avro.pars. If not input, the same schema as the input data will be used for output.
#' Note that the column order of the output data will be identical to the column order of the input. Terms/variables
#' not calibrated will still be included in the output, just passed through. Note that any term names that are
#' changed between the input schema that the data are read in with and the output schema will be applied also
#' to uncertainty data, coefficients, and calibration flags. For example, if the term 'resistance' is changed to
#' 'temperature' in the output data schema, the uncertainty information will be output using the term 'temperature'.
#'
#' @param SchmQf (optional) A json-formatted character string containing the schema for the calibration flags output
#' by this function. If not input, the schema will be created automatically.
#' The output  is ordered as follows:
#' readout_time, all terms output for the valid cal flag, all terms output for the suspect cal flag. Note that the
#' term order for each flag will match the order of the terms listed in TermQf. ENSURE THAT ANY
#' OUTPUT SCHEMA MATCHES THIS ORDER, otherwise the columns will be mislabeled. If no schema is input, default column
#' names other than "readout_time" are a combination of the term, '_', and the flag name ('QfExpi' or 'QfSusp').
#' For example, for terms 'resistance' and 'voltage' each having calibration information. The default column naming
#' (and order) is "readout_time", "resistance_qfExpi","voltage_qfExpi","resistance_qfSusp","voltage_qfSusp".
#'
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' By default, the datum path specified in input DirIn will be included in Meta$PathDatum. 
#' If the 'location' directory is found in DirIn (not nested further), all location metadata files in that 
#' directory will be read in and combined with NEONprocIS.base::wrap.loc.meta.comb and added to the Meta 
#' object in Meta$Locations. Note that if any uncertainty function needs FDAS uncertainty coefficients, they
#' should be included in Meta$ucrtCoefFdas. Meta$ucrtCoefFdas should be a data frame of FDAS uncertainty 
#' coefficients, as produced by NEONprocIS.cal::def.read.ucrt.coef.fdas. See that function for details. 
#' These coefficients will be added to the uncertainty coefficients found in any calibration
#' files and output to the ucrt_coef folder, as well as input into any uncertainty functions 
#' indicated in \code{FuncUcrt$FuncUcrtFdas}.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is).
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Directories for calibrated data (data), uncertainty coefficients (uncertainty_coef), valid calibration flags (flags)(if indicated), 
#' uncertainty data (uncertainty_data) (if indicated) and any other additional subfolders specified in DirSubCopy symbolically linked in 
#' directory DirOut, where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains
#' the child directory structure of the input path. By default, the 'calibration' directory of the input path is dropped
#' unless specified in the DirSubCopy argument. Further details on the outputs in each of these directories are as follows: \cr
#' \cr
#' \code{data}: Calibrated data. If the input argument \code{FuncConv} is not NULL, the calibrated output is 
#' dependent on the transformations that each calibration function performs on the input data frame, performed in sequence 
#' according to the rows of FuncConv input argument. For example, consider two rows in FuncConv, where row 1 contains 
#' FuncConv=def.cal.conv.poly; var=voltage and row 2 contains FuncConv=def.cal.conv.cust; var=NA". The first 
#' function is a standard polynomial conversion function requiring the L0 term to be converted, and its calibrated output 
#' replaces the data in the "voltage" column of the data frame. The output will then be passed into the custom def.cal.conv.cust 
#' function specified in row 2, which does not require any term to be specified in the "var" column. Whatever output data frame 
#' that function returns will be passed to any successive functions specified in additional rows of FuncConv. Note that the columns 
#' of the final output data frame as returned by the function indicated in the final row of FuncConv may be relabeled 
#' as specified in the output schema provided SchmDataOutList. If input argument \code{FuncConv} is not 
#' included or NULL, no calibration conversion will be performed for any L0 data, and the output data will be identical to the 
#' L0 data, aside from any relabeling of the columns as specified in SchmDataOutList. \cr
#' \cr
#' \code{uncertainty_coef}: All uncertainty coefficients in the calibration files and FDAS uncertainty (if applicable) will be 
#' output in the uncertainty_coef folder (json format) for all terms with calibration information supplied in the calibration folder,
#' regardless of whether they are specified in the input arguemnts for output of individual measurement uncertainty data 
#' (output in the uncertainty_data folder). \cr
#' \code{uncertainty_data}: L0' uncertainty data. In addition to all outputs produced by the uncertainty functions listed in 
#' \code{FuncUcrt}, combined and expanded individual measurement uncertainty are also output in the resulting uncertainty data file. 
#' Note that there is no option to provide an alternate output schema for uncertainty data, as column naming is depended on for 
#' downstream processing and needs to be consistent with the calibrated data. Output column naming is a combination of the term, 
#' un underscore, and the column names output from the uncertainty functions (e.g. resistance_ucrtMeas).If an output schema was 
#' provided in FileSchmData, any mappings between input terms and output (converted) terms will also be performed for uncertainty 
#' data. For example, if term "resistance" in the L0 data was converted to "temp" in the calibrated output, and "ucrtMeas" is an 
#' output column from the measurement uncertainty function, then the column output by this script  would be temp_ucrtMeas. \cr
#' \cr
#' \code{flags}: Output calibration quality flags for the terms in input argument \code{TermQf}, and renamed as desired with the schema indicated
#' in input argument \code{SchmQf}. See the description for input argument \code{SchmQf} for column ordering and naming. 
#'

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' FuncConv <- data.frame(var='resistance',
#'                        FuncConv='def.cal.conv.poly',
#'                        stringsAsFactors=FALSE)
#' FuncUcrt <- data.frame(var=c('resistance','resistance'),
#'                        FuncUcrt=c('def.ucrt.meas.cnst','def.ucrt.fdas.rstc.poly'),
#'                        stringsAsFactors=FALSE)
#' Meta <- list()
#' Meta$ucrtCoefFdas  <- NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'fdas_calibration_uncertainty_general.json')
#' SchmDataOutList <- NEONprocIS.base::def.schm.avro.pars(FileSchm = 'prt_calibrated.avsc')
#' SchmQf <- base::paste0(base::readLines('flags_calibration.avsc'), collapse = '')
#'
#' wrap.cal.conv(DirIn="~/pfs/hmp155_data_calibration_group",
#'                    DirOutBase="~/pfs/out",
#'                    FuncConv=FuncConv,
#'                    FuncUcrt=FuncUcrt,
#'                    Meta = Meta,
#'                    TermQf='resistance',
#'                    NumDayExpiMax=NA,
#'                    SchmDataOutList=SchmDataOutList,
#'                    SchmQf=SchmQf,
#'                    DirSubCopy=c("abc")
#' )
# Note. To use non NULL DirSubCopy param, the input path should have the sub folder.
# To see an example, refer to the test input path, pfs/prt_DirSubCopy/...
#
#' @seealso None currently

# changelog and author contributions / copyrights
#   Mija Choi (2021-08-26)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.cal.conv.R\n")

# Unit test of wrap.cal.conv.R
test_that("Unit test of wrap.cal.conv.R", {
  
  source('../../flow.cal.conv/wrap.cal.conv.R')
  library(stringr)
  #
  testOutputBase = "pfs/out"
  #
  # Test 1. Only the input of empty directories in calibration/ and output directry are passed in
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
    }
  
  testInputDir <- 'pfs/prt_noDir_inCalibration/14491/2019/01/01'
  
  output <-try(wrap.cal.conv(DirIn = testInputDir, DirOutBase = testOutputBase),silent = TRUE)
  
  testthat::expect_true((class(output)[1] == "try-error"))
  #
  # Test 2. Only the input of directories, resistance and voltage, and output directry are passed in
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDir <- 'pfs/prt/14491/2019/01/01'
  testInputDataDir <- base::paste0(testInputDir, '/', 'data/')
  testOutputDataDir <- base::paste0(gsub("prt", "out", testInputDataDir))
  testOutputUncertCoefDir <- base::paste0(gsub("prt", "out", testInputDir), '/', 'uncertainty_coef/')
  
  output <- wrap.cal.conv(DirIn = testInputDir, DirOutBase = testOutputBase)

  testthat::expect_true (file.exists(testOutputDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertCoefDir, recursive = TRUE))
  
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
      var = c('resistance','resistance'),
      FuncUcrt = c('def.ucrt.meas.cnst','def.ucrt.fdas.rstc.poly'),
      stringsAsFactors = FALSE
      )
  Meta <- list()
  Meta$ucrtCoefFdas  <-NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'testdata/fdas_calibration_uncertainty_general.json')
  SchmDataOutList <-NEONprocIS.base::def.schm.avro.pars(FileSchm = 'testdata/prt_calibrated.avsc')
  SchmQf <- base::paste0(base::readLines('testdata/flags_calibration.avsc'),collapse = '')
  
  DirCal = base::paste0(testInputDir, '/calibration')
  NameVarExpc = character(0)
  varCal <- base::unique(c(NameVarExpc, base::dir(DirCal)))
  values <- c(2, 3)
  NumDayExpiMax <- data.frame(var = varCal,NumDayExpiMax = values,stringsAsFactors = FALSE)
  
  output <- try(wrap.cal.conv(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ), silent = FALSE)
  
  testOutputFlagsDir <-base::paste0(gsub("prt", "out", testInputDir), '/', 'flags/')
  testOutputUncertDataDir <-base::paste0(gsub("prt", "out", testInputDir), '/', 'uncertainty_data/')
  
  testthat::expect_true (file.exists(testOutputDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputFlagsDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertCoefDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertDataDir, recursive = TRUE))
  
  # Test 3.a test an additional sub folder by passing DirSubCopy <- c("abc")
  #
  
  testInputDirSubCopy <- "pfs/prt_DirSubCopy/14491/2019/01/01"
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  output <- try(wrap.cal.conv(
    DirIn = testInputDirSubCopy,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf,
    DirSubCopy <- c("abc")
  ), silent = FALSE)
  
  testOutputAbcDir <- base::paste0(gsub("prt_DirSubCopy", "out", testInputDirSubCopy),'/','abc')
  fileAbc <- base::dir(testOutputAbcDir)
  
  testthat::expect_true (file.exists(testOutputAbcDir, recursive = TRUE))
  #
  # Test 4. SchmDataOutList = NULL and the rest are same as in test 3.
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  SchmDataOutList <- NULL
  
  output <- try(wrap.cal.conv(
    DirIn = testInputDirSubCopy,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf,
    DirSubCopy <- c("abc")
  ),  silent = FALSE)
  
  testthat::expect_true (file.exists(testOutputDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputFlagsDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertCoefDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputAbcDir, recursive = TRUE))
  
  #
  # Test 5. Avro schema has 'resistance', dataIn has 'resistance' and param, 'resistance', passed in
  # but the calibration has wrong folders, no_resistance. Still produces output (albeit all NA)
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }

  testInputWrongDir <- "pfs/prt_wrong_dir_inCalibration/14491/2019/01/01"
  
  output <- wrap.cal.conv(
    DirIn = testInputWrongDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = NumDayExpiMax,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  )
  
  testthat::expect_true (file.exists(testOutputDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputFlagsDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertCoefDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertDataDir, recursive = TRUE))
  
  #
  # Test 6. Bad formatting for NumDayExpiMax
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  output <- try(wrap.cal.conv(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = unlist(NumDayExpiMax),
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = TRUE)
  
  testthat::expect_true((class(output)[1] == "try-error"))
  
  # test 7. the test dir  has a wrong data, avro, not parquet
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputWrongDataDir <- "pfs/prt_wrong_data/14491/2019/01/01"
  
  output <- try(wrap.cal.conv(
    DirIn = testInputWrongDataDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'resistance',
    NumDayExpiMax = NA,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = FALSE)
  
  testthat::expect_true((class(output)[1] == "try-error"))
  
  # test 8. input data does not include term that we want quality flags for
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  output <- try(wrap.cal.conv(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    FuncConv = FuncConv,
    FuncUcrt = FuncUcrt,
    Meta = Meta,
    TermQf = 'voltage',
    NumDayExpiMax = NA,
    SchmDataOutList = SchmDataOutList,
    SchmQf = SchmQf
  ),  silent = TRUE)
  
  testthat::expect_true((class(output)[1] == "try-error"))
  
  # test 9. Test detection and use of location data, as well as outputs for uncertainty data and coefficients from the cal function
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDirSWC <- "pfs/enviroscan_calibration_group_and_convert/enviroscan/2025/09/05/12294"
  FuncConvMultiOut <- data.frame(var = 'rawVSWC1', FuncConv = 'def.cal.conv.test.multi.out', stringsAsFactors = FALSE)

  output <- wrap.cal.conv(
    DirIn = testInputDirSWC,
    DirOutBase = testOutputBase,
    FuncConv = FuncConvMultiOut,
    FuncUcrt = NULL,
    Meta = list(),
    TermQf = "rawVSWC1",
    NumDayExpiMax = NA,
    SchmDataOutList = NULL,
    SchmQf = NULL
  )
  
  testOutputDir <- base::paste0(gsub("enviroscan_calibration_group_and_convert", "out", testInputDirSWC))
  testOutputDataDir <- fs::path(testOutputDir,'data')
  testOutputUncertCoefDir <- fs::path(testOutputDir,'uncertainty_coef') 
  testOutputFlagsDir <- fs::path(testOutputDir,'flags') 
  testOutputUncertDataDir <- fs::path(testOutputDir,'uncertainty_data')  

  testthat::expect_true (file.exists(testOutputDataDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputFlagsDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertCoefDir, recursive = TRUE))
  testthat::expect_true (file.exists(testOutputUncertDataDir, recursive = TRUE))
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
})
