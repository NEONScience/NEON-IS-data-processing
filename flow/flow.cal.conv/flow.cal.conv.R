##############################################################################################
#' @title Calibration conversion module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Workflow. Apply calibration and uncertainty functions to L0 data and save applicable
#' uncertainty coefficients. Optionally compute FDAS (datalogger) uncertainty. Valid date
#' ranges and certificate numbers in calibration files are used to determine the most relevant
#' calibration to apply. The most relevant cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#' Note that calibrations with a valid date range beginning after the date range of interest and
#' calibrations that are expired more than their max allowable days since expiration are treated
#' as if they don't exist. Data points are turned to NA if no valid or expired calibration is found.
#' Quality flags are output indicating whether an expired calibration was used.
#'
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Read in FDAS (datalogger) uncertainty coefficients if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy over (by symbolic link) unmodified components
#'      Read in L0 data
#'      Compile calibration metadata and applicable time ranges
#'      Apply calibration function to the L0 using applicable calibration(s)
#'      Assess calibration flags (valid calibration, suspect calibration)
#'      Compile uncertainty coefficients
#'      Compute individual measurement uncertainty
#'     Write out the calibrated data, quality flags, uncertainty information, and uncertainty data
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the input path, structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#,
#' where # indicates any number of parent and child directories of any name, so long as they are not 'pfs'
#' or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 2-digit month, and
#' 2-digit day.
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
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileSchmData=value" (optional), where value is the full path to schema for calibrated data 
#' output by this workflow. If not input, the same schema as the input data will be used for output. 
#' Note that the column order of the output data will be identical to the column order of the input. Terms/variables
#' not calibrated will still be included in the output, just passed through. Note that any term names that are
#' changed between the input schema that the data are read in with and the output schema will be applied also
#' to uncertainty data, coefficients, and calibration flags. For example, if the term 'resistance' is changed to
#' 'temperature' in the output data schema, the uncertainty information will be output using the term 'temperature'.
#'
#' 5. "FileSchmQf=value" (optional), where value is the full path to schema for calibration quality flags
#' output by this workflow. If not input, the schema will be created automatically.
#' The output  is ordered as follows:
#' readout_time, all terms output for the valid cal flag, all terms output for the suspect cal flag. Note that the
#' term order for each flag will match the order of the terms listed in TermQf. ENSURE THAT ANY
#' OUTPUT SCHEMA MATCHES THIS ORDER, otherwise the columns will be mislabeled. If no schema is input, default column
#' names other than "readout_time" are a combination of the term, '_', and the flag name ('QfExpi' or 'QfSusp').
#' For example, for terms 'resistance' and 'voltage' each having calibration information. The default column naming
#' (and order) is "readout_time", "resistance_qfExpi","voltage_qfExpi","resistance_qfSusp","voltage_qfSusp".
#'
#' 6-N. "ConvFuncTermX=value" (optional), where X is an integer beginning at 1 and value contains the calibration
#' conversion function and the associated term name(s) to access within a single call of that function. Begin each "value" 
#' with the calibration conversion function to use within the NEONprocIS.cal package, followed by a 
#' colon (:), and then the L0 term name(s) to pass to the calibration function. 
#' For example: "ConvFuncTerm1=def.cal.conv.poly:resistance" indicates that the L0 term "resistance"
#' will be accessed in the function def.cal.conv.poly. Use additional instances of the ConvFuncTermX 
#' argument to indicate additional functions and associated L0 term(s) to calibrate, incrementing the X integer with each 
#' additional argument. For example, if another L0 term "voltage" also uses the
#' def.cal.conv.poly function, create and additional argument "ConvFuncTerm2=def.cal.conv.poly:voltage". Note
#' the increment in X. An unlimited number of ConvFuncTermX arguments are allowed. 
#'    In most cases a single call to a calibration function will convert a single L0 term, as shown in the examples above. 
#' However, this is not required. A calibration function can produce multiple calibrated outputs from a single L0 
#' input term, and/or multiple L0 input terms can produce a single calibrated output. The relationship between L0 
#' input term(s) and calibrated output term(s) can be one:one, one:many, many:one, or many:many. If multiple L0 
#' terms are used/converted in the same call to the calibration function, delimit the terms with pipes (|). For example, 
#' if both L0 terms "resistance" and "voltage" are used to produce one or more calibrated outputs in the same call to 
#' the calibration function def.cal.conv.cust, the argument would be "ConvFuncTermX=def.cal.conv.cust:resistance|voltage".
#'    Note that any and all calibration functions specified here must accept arguments "data", "infoCal", "varCal", "slctCal", 
#' "Meta", and "log", even if they are unused in the function. See any def.cal.conv.____.R function in the NEONprocIS.cal package for 
#' explanation of these inputs, but in short, the entire input data frame and available calibration information and additional 
#' metadata (see the PathMeta argument below) are passed into each calibration function.
#' 
#'    In the typical case where standard calibration functions are used (such as the def.cal.conv.poly series)
#' and term(s) listed in this set of arguments match L0 term(s) in the input data, the calibrated output will overwrite the 
#' original L0 data (the columns may be relabeled as specified in the output schema provided in FileSchmData). 
#' However, if the L0 terms are desired to be retained, or there is a special case where a 
#' calibrated output is generated without any L0 terms, the ConvFuncTermX argument need not specify any L0 term(s), or the
#' specified L0 term(s) need not match any terms in the L0 data, so long as the specified (custom) calibration function knows 
#' how to handle this. The calibrated output data frame is entirely 
#' dependent on the transformations that each calibration function performs on the input data frame, performed in sequence 
#' according to the ConvFuncTermX arguments. For example, consider two ConvFuncTermX arguments
#' specified as follows: "ConvFuncTerm1=def.cal.conv.poly:voltage" and "ConvFuncTerm2=def.cal.conv.cust:". The first 
#' function is a standard polynomial conversion function requiring the L0 term to be converted, and its calibrated output 
#' replaces the data in the "voltage" column of the data frame. The output will then be passed into the def.cal.conv.cust 
#' function, and whatever output data frame that function returns will be passed to any successive functions specified in 
#' arguments ConvFuncTerm3, ConvFuncTerm4, etc. Thus, the user should know what operations each function performs in order to 
#' create a schema for the final output data frame. Best practice is to first run this code without specifying an output schema 
#' to verify the outputs and column ordering. Then provide an output schema to relabel the output columns as desired. 
#' 
#' If this argument is not included, no calibration conversion will be performed for any L0 data, and the output data will be
#' identical to the input data, aside from any relabeling of the columns as specified in FileSchmData. 
#' 
#'
#' 7. "NumDayExpiMax=value" (optional), where value contains the max days since expiration that calibration information is
#' still considered usable. Calibrations beyond this allowance period are treated as if they do not exist. Thus,
#' if no other applicable calibration file exists, calibrated values will be NA and uncertainty coefficients will
#' not be recorded for these periods. Value may be a single number, in which case it will apply to all terms with
#' calibration information, or multiple values in which case the argument is formatted as term:value|term:value...
#' where term is the term in the calibration directory that the value applies to, and multiple term:value pairs
#' are separated by pipes (|). For example, "NumDayExpiMax=resistance:10|voltage:365|fdom:7" indicates that
#' calibrations for the term resistance are usable 10 days past expiration, those for the term volatage are usable
#' 365 days since expiration, and those for the term fdom are usable 7 days past expiration. Another example,
#' "NumDayExpiMax=365" indicates that all calibrations are usable up to 365 days past expiration, and
#' "NumDayExpiMax=NA" (default behavior) indicates that all calibrations are usable for an unlimited period past
#' expiration. Any terms found in the calibration directory not represented in this argument will be assigned
#' "NumDayExpiMax=NA". Note that use of expired calibration information and the lack of any usable calibration
#' information will always cause the expired/valid calibration flag to be raised.
#'
#' 8. "TermQf=value" (optional), where value contains any number of L0 terms/variables for which to provide calibration
#' flags, separated by pipes (|). For example, if calibration information is expected for the terms "resistance" and
#' "voltage", then enter the argument as "TermQf=resistance|voltage".Terms listed here should match the names of the
#' expected subfolders in the calibration directory. If no subfolder exists matching the term names here, the valid
#' calibration flag will be 1, and the suspect calibration flag will be -1.
#'
#'Make this UcrtFuncTermX
#' 9. "UcrtFuncTermX=value (optional), where X is an integer beginning at 1 and value contains the uncertainty
#' function and associated term(s) to pass to a single call to that function. Begin each "value" 
#' with the uncertainty function to use within the NEONprocIS.cal package, followed by a 
#' colon (:), and then the L0 term name(s) to pass to it. 
#'
#' Custom uncertainty functions may output any amount of variables/columns as needed, but the variable naming is important. 
#' At least one output column name from the measurement uncertainty function must contain "ucrtMeas", and any number of output 
#' columns also containing "ucrtMeas" indicate other sources of uncertainty (except FDAS) that should be added in quadrature to yield
#' the combined individual measurement uncertainty. Indeed, combined and expanded individual measurement uncertainty are also output in 
#' the resulting uncertainty data file. Any variables in the output data frame(s) of the uncertainty 
#' functions indicated here that begin with 'ucrtMeas' or 'ucrtFdas' (typically output from the FDAS uncertainty function) will be 
#' added in quadrature to represent the combined L0' uncertainty for the indicated term. 

#'
#' 9. "TermFuncUcrt=value" (optional), where value contains the combination of the L0 term and the associated uncertainty 
#' function to use within the NEONprocIS.cal package to compute individual measurement uncertainty. The argument is formatted 
#' as term:functionMeas,functionFdas|term:functionMeas,functionFdas... where term is the L0 term, functionMeas is the function 
#' to use for computing in the individual measurement (calibration) uncertainty, and functionFdas is the function to use for 
#' computing the FDAS uncertainty, if applicable. Note that functionMeas is required, whereas functionFdas is not, and they are
#' separated by a comma if both are input. Multiple term:functionMeas,functionFdas sets are separated by pipes (|). 
#' For example, "TermFuncUcrt=resistance:def.ucrt.meas.cnst,def.ucrt.fdas.rstc.poly|relative_humidity:def.ucrt.meas.mult" 
#' indicates that the function def.ucrt.meas.cnst will be used to compute the individual measurement (calibration) uncertainty 
#' for the term 'resistance' and the function def.ucrt.fdas.rstc.poly will be used to compute the FDAS uncertainty for this same term. 
#' The function def.ucrt.meas.mult will be used to compute the individual measurement (calibration) uncertainty for the term 
#' 'relative_humidity', and FDAS uncertainty will not be computed.  
#' Note that any and all uncertainty functions specified here must accept arguments "data", "infoCal", "varUcrt", "slctCal", 
#' and "log", even if they are unused in the function. See any def.ucrt.meas.____.R function in the NEONprocIS.cal package for 
#' explanation of these inputs, but in short, the entire L0 data frame and available calibration information are passed into each 
#' uncertainty function.
#' In the typical case, a term listed in this argument will match a L0 term in the input data. However, it need not match any of 
#' the L0 terms in the input data, so long as it the specified (custom) uncertainty function knows this and the term matches one 
#' of the terms listed in the TermFuncConv. 
#' Custom uncertainty functions may output any amount of variables/columns as needed, but the variable naming is important. 
#' At least one output column name from the measurement uncertainty function must start with "ucrtMeas", and any number of output 
#' columns beginning with "ucrtMeas" indicate other sources of uncertainty (except FDAS) that should be added in quadrature to yield
#' the combined individual measurement uncertainty. Indeed, combined and expanded individual measurement uncertainty are also output in 
#' the resulting uncertainty data file. Any variables in the output data frame(s) of the uncertainty 
#' functions indicated here that begin with 'ucrtMeas' or 'ucrtFdas' (typically output from the FDAS uncertainty function) will be 
#' added in quadrature to represent the combined L0' uncertainty for the indicated term. 
#' Note that there is no option to provide an alternate output schema for uncertainty data, as column naming is depended on for 
#' downstream processing and needs to be consistent with the calibrated data. Output column naming is a combination of the term, 
#' un underscore, and the column names output from the uncertainty functions (e.g. resistance_ucrtMeas).If an output schema was provided 
#' in FileSchmData, any mappings between input terms and output (converted) terms will also be performed for uncertainty data. For example, 
#' if term "resistance" was converted to "temp", and "ucrtMeas" is an output column from the measurement uncertainty function, then the column 
#' output by this script  would be temp_ucrtMeas. 
#' Finally, note that all uncertainty coefficients in the calibration files and FDAS uncertainty (if applicable) will be output 
#' in the ucrt_coef folder for all terms with calibration information supplied in the calibration folder, regardless of whether they are 
#' specified here for output of individual measurement uncertainty data (output in the ucrt_data folder).
#'
#' 10. "FileUcrtFdas=value" (optional), where value is the full path to the uncertainty coefficients for the FDAS.
#' Must be provided if FDAS uncertainty applies. These coefficients will be added to the uncertainty coefficients found in any calibration
#' files and output to the ucrt_coef folder, as well as input into any uncertainty functions indicated in TermFuncUcrt.
#'
#' 11. "PathMeta=value" (optional), where value is a named list of paths (directories or files), relative or absolute,
#'  and separated by pipes (|), that will be passed to any/all functions specified for calibration and uncertainty 
#'  for use within those functions (in the 'Meta' list object). Use the format: "PathMeta=name1:path1|name2:path2|name3:path3", etc. 
#'  In this example, name1 will be the name of the list element and path1 will be the path string. The same goes for 
#'  name2:path2, name3:path3, etc. If any path is relative  (does not begin with a forward-slash), the path will 
#'  be prepended with the path to the datum being processed (see the DirIn input above). Note that if the "location" 
#'  directory is found in the datum path (alongside "data" and "calibration"), one list element of the Meta object 
#'  passed to calibration and uncertainty functions will automatically be 'locations', which will contain the combined 
#'  location metadata read from all files found in that directory. Note that it is up to the calibration and uncertainty
#'  functions themselves to know what do with with any location metadata or directory/file paths sent in the Meta
#'  object. Most calibration functions do nothing with this metadata.
#' 
#' 12. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at
#' the same level as the data folder in the input path that are to be copied with a symbolic link to the
#' output path.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Directories for calibrated data (data), uncertainty coefficients (uncertainty_coef), valid calibration flags (flags)(if indicated), 
#' uncertainty data (uncertainty_data) (if indicated) and any other additional subfolders specified in DirSubCopy symbolically linked in 
#' directory DirOut, where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains
#' the child directory structure of the input path. By default, the 'calibration' directory of the input path is dropped
#' unless specified in the DirSubCopy argument.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.cal.conv.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_calibration.avsc" "TermFuncConv=resistance:def.cal.conv.poly" "NumDayExpiMax=NA" "TermFuncUcrt=resistance:def.ucrt.meas.cnst,def.ucrt.fdas.rstc.poly"
#'
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/prt_calibration_filter/prt/2019/01/01')
#' Rscript flow.cal.conv.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_calibration.avsc" TermFuncConv=resistance:def.cal.conv.poly" "NumDayExpiMax=NA" "TermUcrt=resistance:def.ucrt.meas.cnst,def.ucrt.fdas.rstc.poly"
#'
#' Stepping through the code in Rstudio
#' Sys.setenv(DIR_IN='/scratch/pfs/prt_calibration_filter')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "FileSchmData=/scratch/pfs/avro_schemas/dp0p/prt_calibrated.avsc", "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_calibration.avsc", TermFuncConv=resistance:def.cal.conv.poly", "NumDayExpiMax=NA", "TermUcrt=resistance:def.ucrt.meas.cnst,def.ucrt.fdas.rstc.poly")
#' # Then copy and paste rest of workflow into the command window

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-28)
#     original creation with read/write in csv until AVRO R-package available
#   Cove Sturtevant (2019-05-01)
#     add looping through datums
#   Cove Sturtevant (2019-05-09)
#     add logging
#   Cove Sturtevant (2019-05-21)
#     updated call to newly created NEONprocIS.cal package
#   Cove Sturtevant (2019-06-11)
#     changed name to flow.cal.conv.R
#   Cove Sturtevant (2019-06-18)
#     added ingest and application of output schemas for data and valid calibration flags
#     added placeholder output for uncertainty coefficients.
#   Cove Sturtevant (2019-09-12)
#     added reading of input path and file schemas from environment variables
#     simplified fatal errors to not stifle the R error message
#   Cove Sturtevant (2019-09-26)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2019-10-23)
#     added (actual) output of uncertainty information
#     map input terms to output terms for uncertainty based on calibrated output schema
#   Cove Sturtevant (2019-10-28)
#     add FDAS uncertainty
#   Cove Sturtevant (2010-01-04)
#     add option to use alternate uncertainty function
#   Cove Sturtevant (2020-02-10)
#     pulled out major code functionality into functions & simplified cal selection logic
#     add options for alternate uncertainty or calibration functions
#     add option for output of individual combined & expanded measurement uncertainty
#     add suspect calibration flag
#   Cove Sturtevant (2020-02-25)
#     implement selection of which terms to supply calibration flags for
#   Cove Sturtevant (2020-03-03)
#     accept data repositories without a calibration folder and handle accordingly
#   Cove Sturtevant (2020-03-31)
#     remove dirSubCopy from mandatory directories to identify datums. Require data folder only.
#   Cove Sturtevant (2020-04-06)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2020-09-01)
#     restructured the arguments and code to allow multiple L0 terms to create a single
#     calibrated output, and to pass all data and cal info to calibration and uncertainty
#     functions
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
#   Cove Sturtevant (2021-07-21)
#     Move main functionality to wrapper function
#   Cove Sturtevant (2021-08-10)
#     Add datum error routing
#   Nora Catolico (2023-01-26)
#     Update dirSubCopy to allow copying of individual files as opposed to the whole directory
#   Cove Sturtevant (2025-08-10)
#     Incorporate an optional input to specific directory or file paths to send
#     in as metadata to specified calibration and uncertainty functions
#     Refactor to allow greater flexibility in custom functions, like calibrating multiple 
#       variables in a single function call, creating new variables, etc.
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.cal.conv.dp0p.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
} 
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut", "DirErr"),
    NameParaOptn = c(
      "FileSchmData",
      "FileSchmQf",
      base::paste0("ConvFuncTerm",1:100),
      "NumDayExpiMax",
      "TermQf",
      base::paste0("UcrtFuncTerm",1:100),
      "FileUcrtFdas",
      "PathMeta",
      "DirSubCopy"
    ),
    ValuParaOptn = base::list(
      TermQf = NULL,
      NumDayExpiMax = NA,
      PathMeta = NULL
    ),
    TypePara = base::list(NumDayExpiMax =
                            'numeric'),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))
log$debug(base::paste0('Terms to output calibration flags: ',
                       base::paste0(Para$TermQf, collapse = ',')
))

# Read in the schemas so we only have to do it once and not every
# time in the avro writer.
if (!base::is.null(Para$FileSchmData)) {
  # Retrieve and interpret the output data schema
  SchmDataOutList <-
    NEONprocIS.base::def.schm.avro.pars(FileSchm = Para$FileSchmData, log =
                                          log)
} else {
  SchmDataOutList <- NULL
}
if (!base::is.null(Para$FileSchmQf)) {
  SchmQf <-
    base::paste0(base::readLines(Para$FileSchmQf), collapse = '')
} else {
  SchmQf <- NULL
}


# Parse the cal functions and associated terms
nameParaConv <- base::names(Para)[names(Para) %in% base::paste0("ConvFuncTerm",1:100)]
if(base::length(nameParaConv > 0)){
  spltConv <- Para[nameParaConv]
  FuncConv <- base::lapply(spltConv,
                           FUN=function(argSplt){
                             func <- argSplt[1]
                             var <- setdiff(utils::tail(x=argSplt,n=-1),func)
                             if (base::length(var) == 0){
                               var<-NA
                             } else {
                               var <- base::paste0(var,collapse="|")
                             }
                             return(base::data.frame(FuncConv=func,var=var))
                           })
  FuncConv <- base::do.call(rbind,FuncConv)
  log$debug(base::paste0(
    'Functions and associated terms to calibrate: ',
    base::paste0(apply(as.matrix(FuncConv),1,base::paste0,collapse=':'), collapse = ', ')
  ))
  
} else {
  FuncConv <- NULL
  log$debug('Functions and associated terms to calibrate: None')
}

# Display choice for NumDayExpiMax
log$debug(
  base::paste0(
    'Number of days calibration information may be used past expiration: ',
    base::paste0(Para$NumDayExpiMax, collapse = ',')
  )
)

# Parse the uncertainty functions and associated terms
nameParaUcrt <- base::names(Para)[names(Para) %in% base::paste0("UcrtFuncTerm",1:100)]
if(base::length(nameParaUcrt > 0)){
  spltUcrt <- Para[nameParaUcrt]
  FuncUcrt <- base::lapply(spltUcrt,
                           FUN=function(argSplt){
                             # Parse the uncertainty funcs (meas vs. fdas)
                             func <- argSplt[1]
                             funcUcrtSplt <- base::strsplit(func,',')[[1]]
                             funcUcrtSplt <- data.frame(FuncUcrtMeas=funcUcrtSplt[1],FuncUcrtFdas=funcUcrtSplt[2],stringsAsFactors=FALSE)
                             
                             # Parse the variables
                             var <- setdiff(utils::tail(x=argSplt,n=-1),func)
                             if (base::length(var) == 0){
                               var<-NA
                             } else {
                               var <- base::paste0(var,collapse="|")
                             }
                             funcUcrtSplt$var <- var
                             return(funcUcrtSplt)
                           })
  FuncUcrt <- base::do.call(rbind,FuncUcrt)
  log$debug(base::paste0(
    'Functions and associated terms to compute uncertainty: ',
    base::paste0(lapply(spltUcrt,base::paste0,collapse=':'), collapse = ', ')
  ))
  
} else {
  FuncUcrt <- NULL
  log$debug('Functions and associated terms to compute uncertainty: None')
}

# Assign NumDayExpiMax if input argument specified anything other than a blanket default
if(base::length(Para$NumDayExpiMax) > 1){
  NumDayExpiMax <-
    NEONprocIS.base::def.vect.pars.pair(
      vect = Para$NumDayExpiMax,
      ValuDflt = NA,
      NameCol = c('var', 'NumDayExpiMax'),
      Type = c('character', 'numeric'),
      log = log
    )
  
} else {
  NumDayExpiMax <- Para$NumDayExpiMax
}

# Open FDAS uncertainty file
if (base::is.null(Para$FileUcrtFdas) && !base::is.null(FuncUcrt) && base::any(!base::is.na(FuncUcrt$FuncUcrtFdas))) {
  log$fatal('Path to FDAS uncertainty file must be input in argument FileUcrtFdas.')
  stop()
  
} else if (!base::is.null(Para$FileUcrtFdas)){
  ucrtCoefFdas  <-
    NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = Para$FileUcrtFdas,
                                            log = log)
} else {
  ucrtCoefFdas  <- NULL
}

# Additional metadata to be sent into calibration and uncertainty functions
if(!base::is.null(Para$PathMeta) && 
   base::length(Para$PathMeta) %% 2 > 0){
  log$fatal('Input argument PathMeta must contain name:path pairs, separated by pipes.')
  stop()
}
if (!base::is.null(Para$PathMeta) &&
    base::length(Para$PathMeta) > 0) {
  Meta <-
    NEONprocIS.base::def.vect.pars.pair(
      vect = Para$PathMeta,
      NameCol = c('name', 'path'),
      log = log
    )
  if(base::any(base::duplicated(Meta$name))){
    log$fatal('Names of PathMeta argument must be unique (e.g. PathMeta=name1:path2|name2:path2).')
    stop()
  }
  Meta <- stats::setNames(base::as.list(Meta$path),Meta$name)
  log$debug(base::paste0(
    'Paths sent in as additional metadata for use in calibration and uncertainty functions: ',
    paste0(paste0(names(Meta),'=',unlist(Meta)), collapse = ', ')
  ))
} else {
  Meta <- base::list()
  log$debug('Paths sent in as additional metadata for use in calibration and uncertainty functions: None')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
# It's possible that calibration folder will not exist if the
# sensor has no calibrations. This is okay, as the flags and
# conversion handle this.
nameDirSub <- base::as.list(c('data'))
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Prepend local paths in Meta object
  idxMeta <- Meta
  str01 <- substr(idxMeta,1,1)
  setPrp <- str01 != '/'
  idxMeta[setPrp] <- base::lapply(idxMeta[setPrp],
                                  FUN=function(pathIdx){
                                    fs::path(idxDirIn,pathIdx)
                                  })
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.cal.conv.dp0p(DirIn=idxDirIn,
                         DirOutBase=Para$DirOut,
                         FuncConv=FuncConv,
                         FuncUcrt=FuncUcrt,
                         ucrtCoefFdas=ucrtCoefFdas,
                         TermQf=Para$TermQf,
                         NumDayExpiMax=NumDayExpiMax,
                         SchmDataOutList=SchmDataOutList,
                         SchmQf=SchmQf,
                         Meta=idxMeta,
                         DirSubCopy=DirSubCopy,
                         log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        
        # Re-route the failed datum
        NEONprocIS.base::def.err.datm(
          err=err,
          call.stack=call.stack,
          DirDatm=idxDirIn,
          DirErrBase=Para$DirErr,
          RmvDatmOut=TRUE,
          DirOutBase=Para$DirOut,
          log=log
        )
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
  
  return()
  
} # End loop around datum paths
