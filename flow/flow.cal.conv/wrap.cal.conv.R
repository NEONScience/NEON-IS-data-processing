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
#' as if they don't exist. Data points are turned to NA if no valid or expired calibration is found.
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
#' FuncConv <- data.frame(FuncConv='def.cal.conv.poly',
#'                        var='resistance',
#'                        stringsAsFactors=FALSE)
#' FuncUcrt <- data.frame(FuncUcrt=c('def.ucrt.meas.cnst','def.ucrt.fdas.rstc.poly')
#'                        var=c('resistance','resistance'),
#'                        stringsAsFactors=FALSE)
#' Meta <- list() 
#' Meta$ucrtCoefFdas <- NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = 'fdas_calibration_uncertainty_general.json')
#' SchmDataOutList <- NEONprocIS.base::def.schm.avro.pars(FileSchm = 'prt_calibrated.avsc')
#' SchmQf <- base::paste0(base::readLines('flags_calibration.avsc'), collapse = '')
#' 
#' wrap.cal.conv(DirIn="~/pfs/hmp155_data_calibration_group",
#'                    DirOutBase="~/pfs/out",
#'                    FuncConv=FuncConv,
#'                    FuncUcrt=FuncUcrt,
#'                    TermQf='resistance',
#'                    NumDayExpiMax=NA,
#'                    SchmDataOutList=SchmDataOutList,
#'                    SchmQf=SchmQf,
#'                    Meta = Meta
#' )

#' @seealso \link[NEONprocIS.base]{wrap.loc.meta.comb}
#' @seealso \link[NEONprocIS.cal]{def.read.ucrt.coef.fdas}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-07-21)
#     Convert flow script to wrapper function
#   Mija Choi (2021-09-14)
#     Fix a misplaced parenthesis in if cond,
#   Cove Sturtevant (2022-08-25)
#     Write empty uncertainty_coef json file even if no uncertainty coefs. 
#   Nora Catolico (2023-01-26)
#     Update dirSubCopy to allow copying of individual files as opposed to the whole directory
#   Cove Sturtevant (2025-08-10)
#     Read in and pass location metadata to calibration routine if present
#     Refactor to allow greater flexibility in custom functions, like calibrating multiple 
#       variables in a single function call, creating new variables, etc.
#     Remove ucrtCoefFdas input parameter and assume it is included in Meta$ucrtCoefFdas
##############################################################################################
wrap.cal.conv <- function(DirIn,
                          DirOutBase,
                          FuncConv=NULL,
                          FuncUcrt=NULL,
                          TermQf=NULL,
                          NumDayExpiMax=NA,
                          SchmDataOutList=NULL,
                          SchmQf=NULL,
                          Meta=list(PathDatum=DirIn,ucrtCoefFdas=NULL),
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
  dirLoc <- base::paste0(DirIn, '/location')
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
    
    # Check that the data streams we want to calibrate and/or compute uncertainty for have calibration folders. If not, issue a warning.
    varCalExpc <- base::setdiff(base::unique(base::unlist(base::strsplit(c(FuncConv$var,FuncUcrt$var),"|",fixed=TRUE))),NA)
    exstCal <- varCalExpc %in% varCal
    if (base::nrow(FuncConv) > 0 && !base::all(exstCal)) {
      log$warn(
        base::paste0(
          'No calibration folder exists for term(s): ',
          base::paste0(varCalExpc[!exstCal], collapse = ','),
          ' in datum path ',
          dirCal,
          '. This might be okay if custom cal and/or uncertainty functions are used.'
        )
      )
    }
  }
  
  # ------- Create the output directories for data, flags, and uncertainty --------
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  timeBgn <- InfoDirIn$time # start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  dirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  dirOutData <- base::paste0(dirOut, '/data')
  dirOutUcrtCoef <- base::paste0(dirOut, '/uncertainty_coef')
  dirOutQf <- base::paste0(dirOut, '/flags')
  dirOutUcrtData <- base::paste0(dirOut, '/uncertainty_data')
  NEONprocIS.base::def.dir.crea(
    DirBgn = '',
    DirSub = c(dirOutData, dirOutUcrtCoef),
    log = log
  )
  if (!base::is.null(TermQf)) {
    NEONprocIS.base::def.dir.crea(DirBgn = '',
                                  DirSub = dirOutQf,
                                  log = log)
  }
  
  if (!base::is.null(FuncUcrt)) {
    NEONprocIS.base::def.dir.crea(DirBgn = '',
                                  DirSub = dirOutUcrtData,
                                  log = log)
  }
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    if(any(c('data','uncertainty_coef','uncertainty_data','flags') %in% DirSubCopy)){
      LnkSubObj <- TRUE
    } else {
      LnkSubObj <- FALSE
    }
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,'/',DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=LnkSubObj,
                                       log=log)
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
  numData <- base::nrow(data)
  
  
  # ------- Select which calibrations apply to this day --------
  log$debug('Selecting calibrations applicable to this day')
  calSlct <- NULL
  varCalExpc <- NULL
  if(!base::is.null(c(FuncConv$var,FuncUcrt$var,TermQf))){
    varCalExpc <- base::setdiff(base::unique(base::unlist(base::strsplit(c(FuncConv$var,FuncUcrt$var,TermQf),"|",fixed=TRUE))),NA)
  } 
  calSlct <- NEONprocIS.cal::wrap.cal.slct(
    DirCal = dirCal,
    NameVarExpc = varCalExpc,
    TimeBgn = timeBgn,
    TimeEnd = timeEnd,
    NumDayExpiMax = NumDayExpiMax,
    log = log
  )

  # ------- Load location metadata if present --------
  fileLoc <- base::dir(dirLoc)
  numFileLoc <- base::length(fileLoc)
  if (numFileLoc > 0) {
    log$debug(base::paste0('Loading location metadata found in ', dirLoc))
    loc <- NEONprocIS.base::wrap.loc.meta.comb(NameFile=fs::path(dirLoc,fileLoc))
    Meta$Locations <- loc # Add to Meta object to be passed into the calibration functions
  }
  
  
  # ------- Initialize output --------
  dataConv <- NULL
  qfCal <- NULL
  ucrtCoef <- NULL 
  ucrtData <- NULL
  
  
  # ------- Apply the calibration function to the selected terms ---------
  # NOTE: All standard cal funcs just do calibration. BUT, this framework is flexible enough
  #    that they can return a named list that includes more than the calibrated data.  
  #    See the following code block for details.
  log$debug('Applying any calibration conversions.')
  if(base::is.null(FuncConv) || base::nrow(FuncConv) == 0){
    dataConv <- data
  } else {
    dataConv <-
      NEONprocIS.cal::wrap.cal.conv.dp0p(
        data = data,
        calSlct = calSlct,
        FuncConv = FuncConv,
        Meta = Meta,
        log = log
      )
  }
  
  # Did the cal function return a list output rather than a data frame? If so,
  #   it's a cal function that might also produce uncertainty data (list element 
  #   "ucrtData"), uncertainty coefficients (list element "ucrtCoef") and/or 
  #   calibration flags (list element "qfCal").
  #   Make sure these outputs match the format of the outputs from the default
  #   process. See the following functions:
  #     ucrtData: named list of data frames as returned by NEONprocIS.cal::wrap.ucrt.dp0p
  #     ucrtCoef: named list of data frames as returned by NEONprocIS.cal::wrap.ucrt.coef
  #     qfCal: named list of data frames as returned by NEONprocIS.cal::wrap.ucrt.coef
  #
  #   Detect if it has produced any of these
  if(base::is.list(dataConv) && !base::is.data.frame(dataConv)){
    log$info('List output returned from calibration conversion. Looking for any outputs named: "data", "ucrtData", "ucrtCoef", and/or "qfCal". For each found the corresponding default processing will not be run.')
    
    # Look for uncertainty data 
    ucrtData <- dataConv$ucrtData
    if (!base::is.null(ucrtData)){
      log$info('Found uncertainty data, using this instead of computing with any uncertainty functions in FuncUcrt.')
      
      # Create directory if not already done above
      if(base::is.null(FuncUcrt)){
        NEONprocIS.base::def.dir.crea(DirBgn = '',
                                      DirSub = dirOutUcrtData,
                                      log = log)
      }
    }
    
    # Look for calibration flags
    qfCal <- dataConv$qfCal
    if (!base::is.null(qfCal)){
      log$info('Found calibration flags, using these instead of computing with standard code (the unput TermQf will be ignored).')

      # Create directory if not already done above
      if (base::is.null(TermQf)) {
        NEONprocIS.base::def.dir.crea(DirBgn = '',
                                      DirSub = dirOutQf,
                                      log = log)
      }
      
    }
    
    # Look for uncertainty coefficients (data frame)
    ucrtCoef <- dataConv$ucrtCoef
    if (!base::is.null(ucrtCoef)){
      log$info('Found uncertainty coefficients, using these instead of computing with standard code.')
    }
    
    # Look for data (calibrated data)
    dataConv <- dataConv$data
    if (!base::is.null(dataConv)){
      log$info('Found calibrated data (phew!).')
    }
  }
  
  # --------------- Variable mapping to output schema ------------------
  # Create a mapping between terms in the calibrated data frame and the output 
  #   schema (if we have one). This same mapping will be applied to the terms 
  #   coming out of the calibration flags, uncertainty functions and 
  #   uncertainty coefficients.
  nameVarIn <- base::names(dataConv)
  if (!base::is.null(SchmDataOutList)) {
    nameVarOut <- SchmDataOutList$var$name
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
  
  
  # ------- Populate valid calibration and suspect calibration flags for selected variables with cal info ---------
  if(base::is.null(qfCal)){
    log$debug('Populating any calibration flags.')
    qfCal <-
      NEONprocIS.cal::wrap.qf.cal(
        data = data,
        calSlct = calSlct[TermQf],
        log = log
      )
  }
  

  # Combine the flag output and map names to the output schema (if provided)
  numDataQf <- base::unlist(base::lapply(qfCal,base::nrow))
  if (!base::is.null(numDataQf)) {
    
    # Error-check
    if(!base::all(numDataQf == numData)){
      log$error('Quality flags do not have the same number of rows as the data.')
      stop()
    }
    
    # Construct column names for quality flags as a combo of the variable name and flag type.
    #   Also perform any mapping of term names done by the output data schema
    typeQf <- base::names(qfCal)
    for(typeQfIdx in typeQf){

      #  Get output variable names from the schema mapping (if input)
      nameVarIn <- base::names(qfCal[[typeQfIdx]])
      nameVarOut <- mappNameVar$nameVarOut[base::match(nameVarIn,mappNameVar$nameVarOut)]
      setNoMtch <- base::is.na(nameVarOut)
      nameVarOut[setNoMtch] <- nameVarIn[setNoMtch] # Retain names for any mapping not found
      
      # Rename the columns using a combo of the variable name and flag type
      base::names(qfCal[[typeQfIdx]]) <-
        base::paste0(nameVarOut, '_',typeQfIdx)
    }
    
    # Add readout time to the flags output and compile into a single data frame
    base::names(qfCal) <- NULL
    qfCal <- base::append(data['readout_time'],qfCal) 
    qfCal <- base::do.call(base::cbind, qfCal)
  }
  
  
  # ------- Compile uncertainty coefficients for all variables with cal info ---------
  if(base::is.null(ucrtCoef)){
    log$debug('Compiling uncertainty coefficients.')
    ucrtCoef <-
      NEONprocIS.cal::wrap.ucrt.coef(
        calSlct = calSlct,
        ucrtCoefFdas = Meta$ucrtCoefFdas,
        mappNameVar = NULL, # Mapping is done below
        log = log
      )
  }
    
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
  
    
    # Apply the term mappings to uncertainty coefs 
    idxMtchVarOut <- base::match(ucrtCoef$term,mappNameVar$nameVarIn)
    setNotNa <- !is.na(idxMtchVarOut)
    ucrtCoef$term[setNotNa] <- mappNameVar$nameVarOut[idxMtchVarOut[setNotNa]]
    
  }
  
  # ------- Apply the uncertainty function to the selected terms ---------
  if(base::is.null(ucrtData)){
    log$debug('Computing any uncertainty data.')
    ucrtData <-
      NEONprocIS.cal::wrap.ucrt.dp0p(
        data = data,
        FuncUcrt = FuncUcrt,
        calSlct = calSlct,
        Meta = Meta, 
        log = log
      )
  }
  
  # Append the output variable name as a prefix to each uncertainty data column,
  #   apply any mappings to the calibrated output variable transformations,
  #   and combine all uncertainty data frames into a single data frame with 
  #   readout_time included
  if (base::length(ucrtData) > 0) {

    ucrtData <- base::lapply(base::names(ucrtData),FUN=function(varIdx){
      
      # Get output variable name from the mapping 
      nameVarUcrtOut <- mappNameVar$nameVarOut[mappNameVar$nameVarIn==varIdx]
      
      if(base::length(nameVarUcrtOut) == 0){
        nameVarUcrtOut <- varIdx
      } else {
        nameVarUcrtOut <- nameVarUcrtOut[1]
      }
      
      # Append the output variable name as a prefix to each column
      nameColUcrtIdx <- base::names(ucrtData[[varIdx]])
      if(base::length(nameVarUcrtOut) != 0){
        nameColUcrtOut <- base::paste0(nameVarUcrtOut,'_',nameColUcrtIdx)
      } else {
        nameColUcrtOut <- base::paste0(nameVarUcrtOut,'_',varIdx)
      }
      base::names(ucrtData[[varIdx]]) <- nameColUcrtOut
      
      return(ucrtData[[varIdx]])
    })
    
    # Combine uncertainty data frames for all variables
    base::names(ucrtData) <- NULL # Preserves column names
    ucrtData <- base::do.call(base::cbind, ucrtData)
    ucrtData <-
      cbind(data['readout_time'], ucrtData) # Add timestamps
  }
  
  
  # ------------ Output ---------------
  
  # Write out the calibrated data
  NameFileOutData <- base::paste0(dirOutData, '/', fileData)
  rptData <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = dataConv,
      NameFile = NameFileOutData,
      NameFileSchm = NULL,
      Schm = SchmDataOutList$schmJson
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
  if (!base::is.null(numDataQf)) {
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
  if (base::length(ucrtData) > 0) {
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
  
  return()
}