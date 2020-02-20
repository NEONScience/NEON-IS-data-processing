##############################################################################################
#' @title Calibration conversion module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Workflow. Apply polyomial calibration function to L0 data and save applicable
#' uncertainty coefficients. Optionally compute FDAS (datalogger) uncertainty. Valid date
#' ranges and certificate numbers in calibration files are used to determine the most relevant
#' calibration to apply. The most relevant cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#' Note that calibrations with a valid date range beginning after the date range of interest and
#' calibrations that are expired more than their max allowable days since expiration are treated
#' as if they don't exist. Data points are turned to NA if no valid or expired valibration is found.
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
#' 3. "FileSchmData=value" (optional), where value is the full path to schema for calibrated data output by this
#' workflow. If not input, the schema will be created from the data.
#' Note that the column order of the output data will be identical to the column order of the input. Terms/variables
#' not calibrated will still be included in the output, just passed through. Note that any term names that are
#' changed between the input schema that the data are read in with and the output schema will be applied also
#' to uncertainty data, coefficients, and calibration flags. For example, if the term 'resistance' is changed to
#' 'temperature' in the output data schema, the uncertainty information will be output using the term 'temperature'.
#'
#' 4. "FileSchmQf=value" (optional), where value is the full path to schema for valid calibration quality flags
#' output by this workflow. If not input, the schema will be created from the data.
#' Note that the column order of the output will match the column order of the input data, with the exception that any
#' terms without calibration information supplied will be omitted from the output. The output is ordered as follows:
#' readout_time, all terms output for the valid cal flag, all terms output for the suspect cal flag. ENSURE THAT ANY
#' OUTPUT SCHEMA MATCHES THIS ORDER, otherwise the columns will be mislabeled. If no schema is input, default column
#' names other than "readout_time" are a combination of the term, '_', and the flag name ('QfExpi' or 'QfSusp').
#' For example, for terms 'resistance' and 'voltage' each having calibration information. The default column naming
#' (and order) is "readout_time", "resistance_qfExpi","voltage_qfExpi","resistance_qfSusp","voltage_qfSusp".
#'
#' 5. "TermConv=value" (optional), where value contains any number of terms/variables of the input data to convert the data
#' by applying the calibration function, separated by pipes (|). For example, to apply calibration to the terms
#' "resistance" and "voltage", then enter the argument as "TermConv=resistance|voltage".
#'
#' 6. "FuncConv=value" (optional), where value contains the name of the calibration conversion function to use within
#' the NEONprocIS.cal package. Value may be a single function name, which will be used to convert all terms indicated
#' in TermConv, or multiple function names in which case the argument is formatted as term:function|term:function...
#' where term is the term in TermConv for which the corresponding calibration function will be used. Multiple term:function
#' pairs are separated by pipes (|). For example, "FuncConv=resistance:def.cal.conv.poly|voltage:cal.func"
#' indicates that the function def.cal.conv.poly will be used for the resistance term, and the function cal.func will
#' be used for the voltage term. Another example, "FuncConv=def.cal.conv.poly" indicates that function def.conv.poly will
#' be used for all terms in the TermConv argument. If this argument is not included, the standard polynomial calibration
#' function def.cal.conv.poly will be used for all variables in TermConv. Note that any alternative function
#' must accept arguments "data", "infoCal", and "log", even if they are unused in the function. Input "data" is an array
#' of the data for the applicable term. Input "infoCal" is a data frame of calibration information as returned from
#' NEONprocIS.cal::def.read.cal.xml. If no calibration files are associated with the term, infoCal would be passed in to
#' the function as NULL. Input "log" is a logger object as generated by NEONprocIS.base::def.log.init and used in this
#' script to generate hierarchical logging.
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
#' 8. "TermUcrt=value" (optional), where value contains any number of terms/variables for which to output
#' individual measurement uncertainty using the uncertainty coefficients in the calibration file -- OR A SEPARATE FUNCTION. Separate multiple
#' terms with pipes (|). If the measurements were collected with a resistance- or voltage-based field data aquisition
#' system (FDAS), indicate whether the resistance or voltage uncertainty applies by adding a parentheses with either
#' R or V, respectively, inside. For example, if terms resistance and voltage are subject to FDAS uncertainty, and
#' their respective raw measurement units are ohms and volts, the argument is "TermUcrt=resistance(R)|voltage(V)".
#' Terms with FDAS uncertainty must be a subset of the terms listed in the TermConv argument above, whereas other
#' terms may or may not be a subset of those in the TermConv argument. Note that uncertainty coefficients will be output
#' for all terms with calibration information supplied in the calibration folder, regardless of whether they are
#' specified here for output of individual measurement uncertainty.
#'
#' 9. "FuncUcrt=value" (optional), where value contains the name of the function to use within the NEONprocIS.cal
#' package to compute individual measurement uncertainty. Value may be a single function name, which will be used
#' to compute uncertainty for all terms indicated in TermUcrt, or multiple function names in which case the argument
#' is formatted as term:function|term:function... where term is the term in TermUcrt for which the corresponding
#' uncertainty function will be used. Multiple term:function pairs are separated by pipes (|). For example,
#' "FuncUcrt=resistance:def.ucrt.meas|voltage:ucrt.func" indicates that the function def.ucrt.meas will be used for
#' the resistance term, and the function ucrt.func will be used for the voltage term. Another example,
#' "FuncUrt=def.ucrt.meas" indicates that function def.ucrt.meas will be used for all terms in the TermUcrt argument.
#' If this argument is not included, the standard uncertainty function def.ucrt.meas will be used for all variables
#' in TermUcrt. Note that any alternative function must accept arguments "data", "infoCal", and "log", even if they
#' are unused in the function. See documentation for NEONprocIS.cal::def.ucrt.meas for input/output format. Note that
#' one output column must be labeled "ucrtMeas", corresponding to the individual measurement uncertainty. The
#' measurement uncertainty (ucrtMeas) returned from the indicated function will be added in quadrature with
#' FDAS uncertainty if applicable (see TermUcrt). Combined and expanded individual measurement uncertainty are also
#' output. No alternate functions for these quantities are accepted at this time, and there is no option to provide
#' an alternate output schema for uncertainty data, as column naming is depended on for downstream processing. Output
#' column naming is a combination of the term, un underscore, and the uncertainty quantity (e.g. resistance_ucrtMeas).
#' If an output schema was provided in FileSchmData, any mappings between input terms and output (converted) terms
#' will also be performed for uncertainty data. For example, if term "resistance" was converted to "temp", then
#' the output column for measurement uncertainty would be temp_ucrtMeas.
#'
#' 10. "FileUcrtFdas=value" (optional), where value is the full path to the uncertainty coefficients for the FDAS.
#' Must be provided if any terms in argument TermUcrt are indicated as subject to FDAS uncertainty.
#'
#' 11. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at
#' the same level as the calibration folder in the input path that are to be copied with a symbolic link to the
#' output path.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Directories for calibrated data, valid calibration flags, uncertainty information, uncertainty data (if
#' indicated) and any other additional subfolders specified in DirSubCopy symbolically linked in directory DirOut,
#' where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains
#' the child directory structure of the input path. By default, the 'calibration' directory of the input path is dropped
#' unless specified in the DirSubCopy argument.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.cal.conv.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_calibration.avsc" "TermConv=resistance" "NumDayExpiMax=NA" "TermUcrt=resistance(R)"
#'
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/prt_calibration_filter/prt/2019/01/01')
#' Rscript flow.cal.conv.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_calibration.avsc" "TermConv=resistance" "NumDayExpiMax=NA" "TermUcrt=resistance(R)"
#' 
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/scratch/pfs/prt_calibration_filter')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "FileSchmData=/scratch/pfs/avro_schemas/dp0p/prt_calibrated.avsc", "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_calibration.avsc", "TermConv=resistance", "NumDayExpiMax=NA", "TermUcrt=resistance(R)")
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
##############################################################################################
options(digits.secs = 3)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut"),
    NameParaOptn = c(
      "FileSchmData",
      "FileSchmQf",
      "TermConv",
      "FuncConv",
      "NumDayExpiMax",
      "TermUcrt",
      "FuncUcrt",
      "FileUcrtFdas",
      "DirSubCopy"
    ),
    ValuParaOptn = base::list(
      TermConv = NULL,
      FuncConv = "def.cal.conv.poly",
      TermUcrt = NULL,
      FuncUcrt = "def.ucrt.meas",
      NumDayExpiMax =
        NA
    ),
    TypePara = base::list(NumDayExpiMax =
                            'numeric'),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))
log$debug(base::paste0(
  'Terms to apply calibration function: ',
  base::paste0(Para$TermConv, collapse = ',')
))
numConv <- base::length(Para$TermConv)

# Read in the schemas so we only have to do it once and not every
# time in the avro writer.
if (!base::is.null(Para$FileSchmData)) {
  # Retrieve and interpret the output data schema
  SchmDataOutAll <-
    NEONprocIS.base::def.schm.avro.pars(FileSchm = Para$FileSchmData, log =
                                          log)
  SchmDataOut <- SchmDataOutAll$schmJson
  SchmDataOutVar <- SchmDataOutAll$var
} else {
  SchmDataOut <- NULL
}
if (!base::is.null(Para$FileSchmQf)) {
  SchmQf <- base::paste0(base::readLines(Para$FileSchmQf), collapse = '')
} else {
  SchmQf <- NULL
}

# Determine the calibration function to be used for each converted term
log$debug(base::paste0(
  'Calibration conversion function(s) to be used: ',
  base::paste0(Para$FuncConv, collapse = ',')
))
if (!base::is.null(Para$TermConv) && base::length(Para$TermConv) > 0) {
  FuncConv <-
    NEONprocIS.base::def.vect.pars.pair(
      vect = Para$FuncConv,
      KeyExp = Para$TermConv,
      ValuDflt = 'def.cal.conv.poly',
      NameCol = c('var', 'FuncConv'),
      log = log
    )
} else {
  FuncConv <- NULL
}

# Error check & convert NumDayExpiMax for internal use
log$debug(
  base::paste0(
    'Number of days calibration information may be used past expiration: ',
    base::paste0(Para$NumDayExpiMax, collapse = ',')
  )
)

# Parse parameters for individual measurement uncertainty
if (!base::is.null(Para$TermUcrt) && base::length(Para$TermUcrt) > 0) {
  ParaUcrt <- base::lapply(
    Para$TermUcrt,
    FUN = function(argSplt) {
      if (base::grepl(pattern = '(R)', x = argSplt)) {
        # Terms in which FDAS resistance uncertainty applies
        return(base::data.frame(
          var = gsub(
            pattern = '[(R)]',
            replacement = '',
            x = argSplt
          ),
          typeFdas = 'R',
          stringsAsFactors = FALSE
        ))
      } else if (base::grepl(pattern = '(V)', x = argSplt)) {
        return(base::data.frame(
          var = gsub(
            pattern = '[(V)]',
            replacement = '',
            x = argSplt
          ),
          typeFdas = 'V',
          stringsAsFactors = FALSE
        ))
      } else {
        return(base::data.frame(
          var = argSplt,
          typeFdas = NA,
          stringsAsFactors = FALSE
        ))
      }
    }
  )
  ParaUcrt <- base::do.call(base::rbind, ParaUcrt)
  
  # Open FDAS uncertainty file
  if (base::any(!base::is.na(ParaUcrt$typeFdas)) &&
      base::is.null(Para$FileUcrtFdas)) {
    log$fatal('Path to FDAS uncertainty file must be input in argument FileUcrtFdas.')
    stop()
    
  } else {
    ucrtCoefFdas  <-
      NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = Para$FileUcrtFdas, log =
                                                log)
    
  }
  
} else {
  ParaUcrt <- NULL
}
log$debug(
  base::paste0(
    'Terms for which to compute individual measurement uncertainty: ',
    base::paste0(Para$TermUcrt, collapse = ',')
  )
)

# Which uncertainty function(s) are we using?
log$debug(
  base::paste0(
    'Individual measurement uncertainty function(s) to be used: ',
    base::paste0(Para$FuncUcrt, collapse = ',')
  )
)
if (!base::is.null(ParaUcrt)) {
  FuncUcrt <-
    NEONprocIS.base::def.vect.pars.pair(
      vect = Para$FuncUcrt,
      KeyExp = ParaUcrt$var,
      ValuDflt = 'def.ucrt.meas',
      NameCol = c('var', 'FuncUcrt'),
      log = log
    )
  ParaUcrt <- base::merge(x = ParaUcrt, y = FuncUcrt, by = 'var')
} else {
  FuncUcrt <- NULL
}

# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(
    Para$DirSubCopy,
    c('data', 'uncertainty_coef', 'uncertainty_data', 'flags')
  ))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('data', 'calibration', DirSubCopy))
log$debug(base::paste0(
  'Expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum
for (idxDirIn in DirIn) {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data and calibration(s)
  idxDirData <- base::paste0(idxDirIn, '/data')
  idxDirCal <- base::paste0(idxDirIn, '/calibration')
  fileData <- base::dir(idxDirData)
  varCal <- base::dir(idxDirCal)
  
  # Assign NumDayExpiMax now that calibration folder names are known
  NumDayExpiMax <-
    NEONprocIS.base::def.vect.pars.pair(
      vect = Para$NumDayExpiMax,
      KeyExpc = varCal,
      ValuDflt = NA,
      NameCol = c('var', 'NumDayExpiMax'),
      Type = c('character', 'numeric'),
      log = log
    )
  
  # Check that the data streams we want to calibrate have calibration folders. If not, issue a warning.
  exstCal <- Para$TermConv %in% varCal
  if (numConv > 0 && !base::all(exstCal)) {
    log$warn(
      base::paste0(
        'No calibration folder exists for term(s): ',
        base::paste0(Para$TermConv[!exstCal], collapse = ','),
        ' in datum path ',
        idxDirCal
      )
    )
  }
  
  # ------- Create the output directories for data, flags, and uncertainty --------
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <- InfoDirIn$time # start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut, '/data')
  idxDirOutQf <- base::paste0(idxDirOut, '/flags')
  idxDirOutUcrtCoef <- base::paste0(idxDirOut, '/uncertainty_coef')
  NEONprocIS.base::def.dir.crea(
    DirBgn = '/',
    DirSub = c(idxDirOutData, idxDirOutQf, idxDirOutUcrtCoef),
    log = log
  )
  
  if (!base::is.null(ParaUcrt)) {
    idxDirOutUcrtData <- base::paste0(idxDirOut, '/uncertainty_data')
    NEONprocIS.base::def.dir.crea(DirBgn = '/',
                                  DirSub = idxDirOutUcrtData,
                                  log = log)
  }
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn, '/', DirSubCopy), idxDirOut, log =
                                         log)
  }
  
  # --------- Load the data ----------
  # Load in data file in AVRO format into data frame 'data'. Grab the first file only, since there should only be one.
  # Note, this AVRO reader is a developmental version.
  data  <-
    base::try(NEONprocIS.base::def.read.avro.deve(
      NameFile = base::paste0(idxDirData, '/', fileData),
      NameLib = '/ravro.so',
      log = log
    ),
    silent = FALSE)
  fileData <- fileData[1]
  if (base::class(data) == 'try-error') {
    # Generate error and stop execution
    log$error(base::paste0('File ', idxDirData, '/', fileData, ' is unreadable.'))
    base::stop()
  }
  
  # Validate the data
  valiData <-
    NEONprocIS.base::def.validate.dataframe(
      dfIn = data,
      TestNameCol = c('readout_time', Para$TermConv, ParaUcrt$var),
      log = log
    )
  if (!valiData) {
    base::stop()
  }
  
  # Create a mapping between terms in the input data and terms in the output data (if we have an output schema)
  nameVarIn <- base::names(data)
  if (!base::is.null(Para$FileSchmData)) {
    nameVarOut <- SchmDataOutVar$name
  } else {
    nameVarOut <- nameVarIn
  }
  mappNameVar <-
    NEONprocIS.base::def.var.mapp.in.out(
      nameVarIn = nameVarIn,
      nameVarOut = nameVarOut,
      nameVarDfltSame = varCal,
      log = log
    )
  
  # ------- Select which calibrations apply to this day --------
  calSlct <- NULL
  calSlct <- NEONprocIS.cal::wrap.cal.slct(
    DirCal = idxDirCal,
    NameVarExpc=base::unique(c(Para$TermConv,ParaUcrt$var)),
    TimeBgn = timeBgn,
    TimeEnd = timeEnd,
    NumDayExpiMax = NumDayExpiMax,
    log = log
  )
  
  # ------- Apply the calibration function to the selected terms ---------
  dataConv <- NULL
  dataConv <-
    NEONprocIS.cal::wrap.cal.conv(
      data = data,
      calSlct = calSlct,
      FuncConv = FuncConv,
      DirCal = idxDirCal,
      log = log
    )
  
  
  # ------- Populate valid calibration and suspect calibration flags for all variables with cal info ---------
  qfCal <- NULL
  qfCal <-
    NEONprocIS.cal::wrap.qf.cal(
      data = data,
      calSlct = calSlct,
      DirCal = idxDirCal,
      mappNameVar = mappNameVar,
      log = log
    )
  
  # Combine the flag output
  base::names(qfCal$qfExpi) <-
    base::paste0(base::names(qfCal$qfExpi), '_qfExpi')
  base::names(qfCal$qfSusp) <-
    base::paste0(base::names(qfCal$qfSusp), '_qfSusp')
  qfCal <-
    base::list(data['readout_time'], qfCal$qfExpi, qfCal$qfSusp)
  qfCal <- base::do.call(base::cbind, qfCal)
  
  
  # ------- Compile uncertainty coefficients for all variables with cal info ---------
  ucrtCoef <- NULL
  ucrtCoef <-
    NEONprocIS.cal::wrap.ucrt.coef(
      calSlct = calSlct,
      DirCal = idxDirCal,
      ParaUcrt = ParaUcrt,
      ucrtCoefFdas = ucrtCoefFdas,
      mappNameVar = mappNameVar,
      log = log
    )
  
  # Simplify & make pretty the uncertainty information
  ucrtCoef <-
    base::Reduce(f = base::rbind, x = ucrtCoef) # merge uncertainty coefs for all terms
  ucrtCoef$timeBgn <-
    base::format(ucrtCoef$timeBgn, format = '%Y-%m-%dT%H:%M:%OSZ') # Convert POSIX to character
  ucrtCoef$timeEnd <-
    base::format(ucrtCoef$timeEnd, format = '%Y-%m-%dT%H:%M:%OSZ')
  ucrtCoef <-
    ucrtCoef[c('id',
               'var',
               'timeBgn',
               'timeEnd',
               'expi',
               'Name',
               'Value',
               '.attrs')] # reorganize columns
  base::names(ucrtCoef) <-
    c(
      'calibration_id',
      'term',
      'start_date',
      'end_date',
      'expired',
      'Name',
      'Value',
      '.attrs'
    ) # rename columns
  
  
  # ------- Apply the uncertainty function to the selected terms ---------
  ucrtData <- NULL
  ucrtData <-
    NEONprocIS.cal::wrap.ucrt.dp0p(
      data = data,
      ParaUcrt = ParaUcrt,
      ucrtCoefFdas = ucrtCoefFdas,
      calSlct = calSlct,
      DirCal = idxDirCal,
      mappNameVar = mappNameVar,
      log = log
    )
  
  if(base::length(ucrtData) > 0){
    # Combine uncertainty data frames for all variables
    base::names(ucrtData) <- NULL # Preserves column names 
    ucrtData <- base::do.call(base::cbind, ucrtData)
    ucrtData <- cbind(data['readout_time'],ucrtData) # Add timestamps
  }
  
  
  # ------------ Output ---------------
  
  # Replace the original data with the calibrated data
  data[, base::names(dataConv)] <- dataConv
  
  # Write out the calibrated data
  NameFileOutData <- base::paste0(idxDirOutData, '/', fileData)
  rptData <-
    base::try(NEONprocIS.base::def.wrte.avro.deve(
      data = data,
      NameFile = NameFileOutData,
      NameFileSchm = NULL,
      Schm = SchmDataOut,
      NameLib = '/ravro.so'
    ),
    silent = TRUE)
  if (base::class(rptData) == 'try-error') {
    log$error(base::paste0(
      'Cannot write Calibrated data to ',
      NameFileOutData,
      '. ',
      attr(rptData, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0('Calibrated data written successfully in ', NameFileOutData))
  }
  
  # Write out the valid calibration flags
  NameFileOutQf <-
    NEONprocIS.base::def.file.name.out(nameFileIn = fileData, sufx = '_flagsCal')
  NameFileOutQf <- base::paste0(idxDirOutQf, '/', NameFileOutQf)
  rptQfCal <-
    NEONprocIS.base::def.wrte.avro.deve(
      data = qfCal,
      NameFile = NameFileOutQf,
      NameFileSchm = NULL,
      Schm = SchmQf,
      NameLib = '/ravro.so'
    )
  if (rptQfCal == 0) {
    log$info(base::paste0('Calibration flags written successfully in ', NameFileOutQf))
  }
  
  # Write uncertainty info to json format
  ucrtList <-
    base::split(ucrtCoef, base::seq(base::nrow(ucrtCoef))) # Turn into a list for writing out in json format
  base::names(ucrtList) <- NULL
  ucrtList <- base::lapply(ucrtList, base::as.list)
  
  NameFileOutUcrtCoef <-
    NEONprocIS.base::def.file.name.out(nameFileIn = fileData,
                                       sufx = '_uncertaintyCoef',
                                       ext = 'json')
  NameFileOutUcrtCoef <-
    base::paste0(idxDirOutUcrtCoef, '/', NameFileOutUcrtCoef)
  rptUcrt <-
    base::write(rjson::toJSON(ucrtList, indent = 3), file = NameFileOutUcrtCoef)
  log$info(
    base::paste0(
      'Uncertainty coefficients written successfully in ',
      NameFileOutUcrtCoef
    )
  )
  
  # Write out uncertainty data
  if (!base::is.null(ParaUcrt)) {
    NameFileOutUcrtData <-
      NEONprocIS.base::def.file.name.out(nameFileIn = fileData, sufx = '_uncertaintyData')
    NameFileOutUcrtData <-
      base::paste0(idxDirOutUcrtData, '/', NameFileOutUcrtData)
    rptUcrtFdas <-
      NEONprocIS.base::def.wrte.avro.deve(data = ucrtData,
                                          NameFile = NameFileOutUcrtData,
                                          NameLib = '/ravro.so')
    if (rptUcrtFdas == 0) {
      log$info(base::paste0(
        'Uncertainty data written successfully in ',
        NameFileOutUcrtData
      ))
    }
  }
  
}
