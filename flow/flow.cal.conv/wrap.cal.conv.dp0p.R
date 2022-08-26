##############################################################################################
#' @title Calibration conversion module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Apply calibration and uncertainty functions to L0 data and save applicable
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
#' @param FuncConv (optional) A data frame indicating the terms to apply calibration conversion and their corresponding 
#' calibration functions. The columns of the data frame are:
#' \code{var}: Character. The name of the variable/term to be calibrated. Typically this must match a column name in 
#' the input (L0) data, but it is not required so long as the calibration conversion function handles this case, for example 
#' if multiple L0 terms are used to create a single calibrated output. In the latter case, provide the term name of the 
#' single calibrated output.  \cr
#' \code{FuncConv}: Character. The calibration conversion function within the NEONprocIS.cal package for the term listed 
#' in the \code{var} column of the same row. Note that any and all calibration functions specified here must accept arguments 
#' "data", "infoCal", "varCal", "slctCal", and "log", even if they are unused in the function. See any def.cal.conv.____.R 
#' function in the NEONprocIS.cal package for explanation of these inputs, but in short, the entire L0 data frame and 
#' available calibration information are passed into each calibration function. 
#' 
#' @param FuncUcrt (optional) A data frame indicating the terms for which to generate L0' uncertainty data and their 
#' corresponding functions. The columns of the data frame are:
#' \code{var}: Character. The name of the variable/term to compute L0' uncertainty. Typically this must match a 
#' column name in input (L0) data, but it is not required so long as the uncertainty function handles this case, for example 
#' if multiple L0 terms are used to create a single uncertainty output. In the latter case, provide the term name of the 
#' single uncertainty output.  \cr
#' \code{FuncUcrtMeas}: Character. The function within the NEONprocIS.cal package to use for computing the individual measurement 
#' (calibration) uncertainty for the term listed in the \code{var} column of the same row. Note that any and all uncertainty 
#' functions specified here must accept arguments "data", "infoCal", "varUcrt", "slctCal", and "log", even if they are unused 
#' in the function. See any def.ucrt.meas.____.R function in the NEONprocIS.cal package for explanation of these inputs, 
#' but in short, the entire L0 data frame and available calibration information are passed into each uncertainty function. \cr
#' \code{FuncUcrtFdas}: Character. The function within the NEONprocIS.cal package to use for computing the FDAS uncertainty, 
#' if applicable. If not applicable for the term, use NA. The same input requirements as the function specified in 
#' \code{FuncUcrtMeas} apply here also.
#' Custom uncertainty functions may output any amount of variables/columns as needed, but the variable naming is important. 
#' At least one output column name from the measurement uncertainty function in \code{FuncUcrtMeas} must start with "ucrtMeas", 
#' and any number of output columns beginning with "ucrtMeas" indicate other sources of uncertainty (except FDAS) that should 
#' be added in quadrature to yield the combined individual measurement uncertainty. Any variables in the output data frame(s) of the uncertainty 
#' functions indicated here that begin with 'ucrtMeas' or 'ucrtFdas' (typically output from the FDAS uncertainty function) will be 
#' added in quadrature to represent the combined L0' uncertainty for the indicated term. 
#'
#' @param ucrtCoefFdas (optional). A data frame of FDAS uncertainty coefficients, as produced by 
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. See that function for details. Must be provided if any FDAS uncertainty functions
#' are indicated in \code{FuncUcrt$FuncUcrtFdas}. These coefficients will be added to the uncertainty coefficients found in any calibration
#' files and output to the ucrt_coef folder, as well as input into any uncertainty functions indicated in \code{FuncUcrt$FuncUcrtFdas}.
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
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Directories for calibrated data (data), uncertainty coefficients (uncertainty_coef), valid calibration flags (flags)(if indicated), 
#' uncertainty data (uncertainty_data) (if indicated) and any other additional subfolders specified in DirSubCopy symbolically linked in 
#' directory DirOut, where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains
#' the child directory structure of the input path. By default, the 'calibration' directory of the input path is dropped
#' unless specified in the DirSubCopy argument. Further details on the outputs in each of these directories are as follows: \cr
#' \cr
#' \code{data}: Calibrated L0' data. If the input argument \code{FuncConv} is not NULL, the calibrated output is structured as follows: 
#' In the typical case where the term listed in the \code{FuncConv$var} column matches a L0 term in the input data, the calibrated output 
#' will overwrite the original L0 data (the columns may be relabeled as specified in the output schema provided in SchmDataOutList). 
#' In the case that a term listed in \code{FuncConv$var} does not match a column in the input data, no L0 data will be overwritten by 
#' the function's output and instead a new column will be appended to the end of the output data in the row order it appears in 
#' \code{FuncConv$var}, with the column name defaulting to the term name indicated in \code{FuncConv$var} (but it may also be relabeled 
#' as specified in the output schema provided SchmDataOutList). Any terms existing in the input data but not indicated in \code{FuncConv$var} 
#' will be passed through to the output unmodified and in the same order as the input data. If input argument \code{FuncConv} is not 
#' included or NULL, no calibration conversion will be performed for any L0 data, and the output L0' data will be identical to the 
#' L0 data, aside from any relabeling of the columns as specified in SchmDataOutList. \cr
#' \cr
#' \code{uncertainty_coef}: All uncertainty coefficients in the calibration files and FDAS uncertainty (if applicable) will be 
#' output in the uncertainty_coef folder (json format) for all terms with calibration information supplied in the calibration folder,
#' regardless of whether they are specified in the input arguemnts for output of individual measurement uncertainty data 
#' (output in the uncertainty_data folder).
#' \cr
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
#' FuncUcrt <- data.frame(var='resistance',
#'                        FuncUcrtMeas='def.ucrt.meas.cnst',
#'                        FuncUcrtFdas='def.ucrt.fdas.rstc.poly',
#'                        stringsAsFactors=FALSE)
#' ucrtCoefFdas  <- NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'fdas_calibration_uncertainty_general.json')
#' SchmDataOutList <- NEONprocIS.base::def.schm.avro.pars(FileSchm = 'prt_calibrated.avsc')
#' SchmQf <- base::paste0(base::readLines('flags_calibration.avsc'), collapse = '')
#' 
#' wrap.cal.conv.dp0p(DirIn="~/pfs/hmp155_data_calibration_group",
#'                    DirOutBase="~/pfs/out",
#'                    FuncConv=FuncConv,
#'                    FuncUcrt=FuncUcrt,
#'                    ucrtCoefFdas=ucrtCoefFdas,
#'                    TermQf='resistance',
#'                    NumDayExpiMax=NA,
#'                    SchmDataOutList=SchmDataOutList,
#'                    SchmQf=SchmQf
#' )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-07-21)
#     Convert flow script to wrapper function
#   Mija Choi (2021-09-14)
#     Fix a misplaced parenthesis in if cond,
#   Cove Sturtevant (2022-08-25)
#     Write empty uncertainty_coef json file even if no uncertainty coefs. 
##############################################################################################
wrap.cal.conv.dp0p <- function(DirIn,
                               DirOutBase,
                               FuncConv=NULL,
                               FuncUcrt=NULL,
                               ucrtCoefFdas=NULL,
                               TermQf=NULL,
                               NumDayExpiMax=NA,
                               SchmDataOutList=NULL,
                               SchmQf=NULL,
                               DirSubCopy=NULL,
                               log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Get directory listing of input directory. Expect subdirectories for data and calibration(s)
  dirData <- base::paste0(DirIn, '/data')
  dirCal <- base::paste0(DirIn, '/calibration')
  fileData <- base::dir(dirData)
  varCal <- base::dir(dirCal)
  
  # Make sure NumDayExpiMax is populated for all possible calibrated terms 
  varNumDayExpiMax <- base::unique(c(FuncConv$var,FuncUcrt$var,TermQf,varCal))
  if(base::length(varNumDayExpiMax) == 0){
    NumDayExpiMax <- NULL
    
  } else if(!base::is.data.frame(NumDayExpiMax) && 
            length(NumDayExpiMax) == 1 && 
            (base::is.na(NumDayExpiMax) || 
            base::is.numeric(NumDayExpiMax))){
    
    # Apply single value to all possible calibrated terms
    NumDayExpiMax <-
      NEONprocIS.base::def.vect.pars.pair(
        vect = NumDayExpiMax,
        KeyExpc = varNumDayExpiMax,
        ValuDflt = NA,
        NameCol = c('var', 'NumDayExpiMax'),
        Type = c('character', 'numeric'),
        log = log
      )
  } else if (base::is.data.frame(NumDayExpiMax) && 
             base::ncol(NumDayExpiMax) == 2 && 
             base::names(NumDayExpiMax) %in% c('var','NumDayExpiMax')){
    
    # Fill in the default NA for any missing terms
    termMiss <- varNumDayExpiMax[!(varNumDayExpiMax %in% NumDayExpiMax$var)]
    if(base::length(termMiss) > 0){
      dfAdd <- base::data.frame(var=termMiss,
                                NumDayExpiMax=base::as.numeric(NA),
                                stringsAsFactors = FALSE)
      NumDayExpiMax <- base::rbind(NumDayExpiMax,dfAdd)
    }
    
  } else {
    log$fatal('Input argument NumDayExpiMax is formatted incorrectly. Please check.')
    stop()
  }
  
  # Do some error checking if we're applying calibration conversion
  if(!base::is.null(FuncConv)){
    
    numConv <- base::nrow(FuncConv)
  
    # Check that the data streams we want to calibrate and/or compute uncertainty for have calibration folders. If not, issue a warning.
    exstCal <- base::unique(FuncConv$var,FuncUcrt$var) %in% varCal
    if (numConv > 0 && !base::all(exstCal)) {
      log$warn(
        base::paste0(
          'No calibration folder exists for term(s): ',
          base::paste0(FuncConv$var[!exstCal], collapse = ','),
          ' in datum path ',
          dirCal,
          '. This might be okay if custom cal and/or uncertainty functions are used.'
        )
      )
    }
  } else {
    numConv <- 0
  }
  
  # ------- Create the output directories for data, flags, and uncertainty --------
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  timeBgn <- InfoDirIn$time # start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  dirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  dirOutData <- base::paste0(dirOut, '/data')
  dirOutUcrtCoef <- base::paste0(dirOut, '/uncertainty_coef')
  NEONprocIS.base::def.dir.crea(
    DirBgn = '',
    DirSub = c(dirOutData, dirOutUcrtCoef),
    log = log
  )
  if (!base::is.null(TermQf)) {
    dirOutQf <- base::paste0(dirOut, '/flags')
    NEONprocIS.base::def.dir.crea(DirBgn = '',
                                  DirSub = dirOutQf,
                                  log = log)
  }
  
  if (!base::is.null(FuncUcrt)) {
    dirOutUcrtData <- base::paste0(dirOut, '/uncertainty_data')
    NEONprocIS.base::def.dir.crea(DirBgn = '',
                                  DirSub = dirOutUcrtData,
                                  log = log)
  }
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn, '/', DirSubCopy), dirOut, log =
                                         log)
  }
  
  # --------- Load the data ----------
  # Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
  fileData <- fileData[1]
  data  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirData, '/', fileData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', dirData, '/', fileData, ' is unreadable.'))
    base::stop()
  }
  
  # Validate the data
  valiData <-
    NEONprocIS.base::def.validate.dataframe(dfIn = data,
                                            TestNameCol = base::unique(c(
                                              'readout_time', TermQf
                                            )),
                                            log = log)
  if (!valiData) {
    base::stop()
  }
  
  # Create a mapping between terms in the input data and terms in the output data (if we have an output schema)
  nameVarIn <- base::names(data)
  nameVarAdd <- setdiff(FuncConv$var,nameVarIn)
  if (!base::is.null(SchmDataOutList)) {
    nameVarOut <- SchmDataOutList$var$name
  } else {
    nameVarOut <- c(nameVarIn,nameVarAdd)
  }
  mappNameVar <-
    NEONprocIS.base::def.var.mapp.in.out(
      nameVarIn = c(nameVarIn,nameVarAdd),
      nameVarOut = nameVarOut,
      nameVarDfltSame = varCal,
      log = log
    )
  
  # ------- Select which calibrations apply to this day --------
  log$debug('Selecting calibrations applicable to this day')
  calSlct <- NULL
  calSlct <- NEONprocIS.cal::wrap.cal.slct(
    DirCal = dirCal,
    NameVarExpc = base::unique(c(
      FuncConv$var, TermQf, FuncUcrt$var
    )),
    TimeBgn = timeBgn,
    TimeEnd = timeEnd,
    NumDayExpiMax = NumDayExpiMax,
    log = log
  )
  
  # ------- Apply the calibration function to the selected terms ---------
  log$debug('Applying any calibration conversions.')
  dataConv <- NULL
  dataConv <-
    NEONprocIS.cal::wrap.cal.conv(
      data = data,
      calSlct = calSlct,
      FuncConv = FuncConv,
      log = log
    )
  
  
  # ------- Populate valid calibration and suspect calibration flags for selected variables with cal info ---------
  log$debug('Populating any calibration flags.')
  qfCal <- NULL
  qfCal <-
    NEONprocIS.cal::wrap.qf.cal(
      data = data,
      calSlct = calSlct[TermQf],
      mappNameVar = mappNameVar,
      log = log
    )
  
  # Combine the flag output
  if (!base::is.null(TermQf)) {
    base::names(qfCal$qfExpi) <-
      base::paste0(base::names(qfCal$qfExpi), '_qfExpi')
    base::names(qfCal$qfSusp) <-
      base::paste0(base::names(qfCal$qfSusp), '_qfSusp')
    qfCal <-
      base::list(data['readout_time'], qfCal$qfExpi, qfCal$qfSusp)
    qfCal <- base::do.call(base::cbind, qfCal)
  }
  
  
  # ------- Compile uncertainty coefficients for all variables with cal info ---------
  log$debug('Compiling uncertainty coefficients.')
  ucrtCoef <- NULL
  ucrtCoef <-
    NEONprocIS.cal::wrap.ucrt.coef(
      calSlct = calSlct,
      ucrtCoefFdas = ucrtCoefFdas,
      mappNameVar = mappNameVar,
      log = log
    )
  
  # Simplify & make pretty the uncertainty information
  ucrtCoef <-
    base::Reduce(f = base::rbind, x = ucrtCoef) # merge uncertainty coefs for all terms
  if (!base::is.null(ucrtCoef)) {
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
  }
  
  
  # ------- Apply the uncertainty function to the selected terms ---------
  log$debug('Computing any uncertainty data.')
  ucrtData <- NULL
  ucrtData <-
    NEONprocIS.cal::wrap.ucrt.dp0p(
      data = data,
      FuncUcrt = FuncUcrt,
      ucrtCoefFdas = ucrtCoefFdas,
      calSlct = calSlct,
      mappNameVar = mappNameVar,
      log = log
    )
  
  if (base::length(ucrtData) > 0) {
    # Combine uncertainty data frames for all variables
    base::names(ucrtData) <- NULL # Preserves column names
    ucrtData <- base::do.call(base::cbind, ucrtData)
    ucrtData <-
      cbind(data['readout_time'], ucrtData) # Add timestamps
  }
  
  
  # ------------ Output ---------------
  
  # Replace the original data with the calibrated data
  data[, base::names(dataConv)] <- dataConv
  
  # If no output schema was provided, use the same schema as the input data
  if (base::is.null(SchmDataOutList)) {
    # Use the same schema as the input data to write the output data.
    idxSchmDataOut <- base::attr(data, 'schema')
    
    # If we created new variables, add them to the schema as double
    for(idxVarAdd in nameVarAdd){
      txtEval <- base::paste0('arrow::schema(',idxVarAdd,'=arrow::float32())')
      schmAdd <- base::eval(base::parse(text=txtEval))
      idxSchmDataOut <- arrow::unify_schemas(idxSchmDataOut,schmAdd)
    }
  } else {
    idxSchmDataOut <- SchmDataOutList$schmJson
  }
  
  # Write out the calibrated data
  NameFileOutData <- base::paste0(dirOutData, '/', fileData)
  rptData <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = data,
      NameFile = NameFileOutData,
      NameFileSchm = NULL,
      Schm = idxSchmDataOut
    ),
    silent = FALSE)
  if (base::any(base::class(rptData) == 'try-error')) {
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
  if (!base::is.null(TermQf)) {
    NameFileOutQf <-
      NEONprocIS.base::def.file.name.out(nameFileIn = fileData, 
                                         prfx = base::paste0(dirOutQf, '/'),
                                         sufx = '_flagsCal')
    rptQfCal <-
      base::try(NEONprocIS.base::def.wrte.parq(
        data = qfCal,
        NameFile = NameFileOutQf,
        NameFileSchm = NULL,
        Schm = SchmQf
      ),
      silent = FALSE)
    if (base::any(base::class(rptQfCal) == 'try-error')) {
      log$error(base::paste0(
        'Cannot write calibration flags to ',
        NameFileOutQf,
        '. ',
        attr(rptQfCal, "condition")
      ))
      stop()
    } else {
      log$info(base::paste0(
        'Calibration flags written successfully in ',
        NameFileOutQf
      ))
    }
  }
  
  # Write uncertainty info to json format
  if (!base::is.null(ucrtCoef)) {
    ucrtList <-
      base::split(ucrtCoef, base::seq(base::nrow(ucrtCoef))) # Turn into a list for writing out in json format
    base::names(ucrtList) <- NULL
    ucrtList <- base::lapply(ucrtList, base::as.list)
    ucrtJson <- rjson::toJSON(ucrtList, indent = 3)
  } else {
    ucrtJson <- '[]'
  }
    
  NameFileOutUcrtCoef <-
    NEONprocIS.base::def.file.name.out(nameFileIn = fileData,
                                       sufx = '_uncertaintyCoef',
                                       ext = 'json')
  NameFileOutUcrtCoef <-
    base::paste0(dirOutUcrtCoef, '/', NameFileOutUcrtCoef)
  rptUcrt <-
    base::try(base::write(ucrtJson, file = NameFileOutUcrtCoef),
              silent = FALSE)
  if (base::any(base::class(rptUcrt) == 'try-error')) {
    log$error(
      base::paste0(
        'Cannot write uncertainty coefficients to ',
        NameFileOutUcrtCoef,
        '. ',
        attr(rptUcrt, "condition")
      )
    )
    stop()
  } else {
    log$info(
      base::paste0(
        'Uncertainty coefficients written successfully in ',
        NameFileOutUcrtCoef
      )
    )
  }

  
  # Write out uncertainty data
  if (!base::is.null(FuncUcrt)) {
    NameFileOutUcrtData <-
      NEONprocIS.base::def.file.name.out(nameFileIn = fileData, sufx = '_uncertaintyData')
    NameFileOutUcrtData <-
      base::paste0(dirOutUcrtData, '/', NameFileOutUcrtData)
    rptUcrtData <-
      base::try(NEONprocIS.base::def.wrte.parq(data = ucrtData,
                                               NameFile = NameFileOutUcrtData,
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(rptUcrtData) == 'try-error')) {
      log$error(
        base::paste0(
          'Cannot write uncertainty data to ',
          NameFileOutUcrtData,
          '. ',
          attr(rptUcrtData, "condition")
        )
      )
      stop()
    } else {
      log$info(base::paste0(
        'Uncertainty data written successfully in ',
        NameFileOutUcrtData
      ))
    }
  }
  
}