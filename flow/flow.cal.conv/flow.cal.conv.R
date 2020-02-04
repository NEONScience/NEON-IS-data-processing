##############################################################################################
#' @title Calibration conversion module for NEON IS data processing

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} 

#' @description Workflow. Apply polyomial calibration function to L0 data and save applicable 
#' uncertainty coefficients. Optionally compute FDAS (datalogger) uncertainty. Valid date 
#' ranges and certificate numbers in calibration files are used to determine the most relevant 
#' calibration to apply. The most relevant cal follows this choice order (1 chosen first):
#'    1. higher certificate number & data date within valid date range
#'    2. lower certificate number & data date within valid date range 
#'    3. higher certificate number & data date after valid date range 
#'    4. lower certificate number & data date after valid date range 
#' Data points are turned to NA if no valid or expired valibration is found. Expired calibrations 
#' are those in which the valid date range ends prior to the data date. Quality flags are output 
#' indicating whether an expired calibration was used. 
#' 
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Read in FDAS (datalogger) uncertainty coefficients if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy over (by symbolic link) unmodified components 
#'      Read in L0 data
#'     For each L0 data stream with calibration information:
#'        Read in calibration and uncertainty coefficients and valid date ranges from all available calibration files 	
#'        Apply calibration polynomial function to the L0 data using the most valid available calibration coefficients for each data value
#'        Compute FDAS uncertainty for each data value using the uncertainty coefficients
#'        Quality flag any calibrated data values that used expired calibration coefficients
#'     Write out the calibrated data to file
#'     Write out the quality flags to file
#'     Write out the uncertainty information
#'     
#' This script is run at the command line with 6 or 7 arguments. Each argument must be a string in the 
#' format "Para=value", where "Para" is the intended parameter name and "value" is the value of the 
#' parameter. The arguments are: 
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
#' Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter will be assigned from 
#' the system environment variable matching the value string.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "FileSchmData=value", where value is the full path to schema for calibrated data output by this
#' workflow. Use NA to create the schema from the data frame. If the "value" string begins with a $ (e.g. $DIR_IN), 
#' the value of the parameter will be assigned from the system environment variable matching the value string.
#' Note that the column order of the output will ne identical to the column order of the input. Terms/variables
#' not calibrated will still be included in the output, just passed through. Note that any term names that are 
#' changed between the input schema that the data are read in with and the output schema will be applied also
#' to uncertainty information. For example, if the term 'resistance' is changed to 'temperature' in the output 
#' schema, the uncertainty information will be output using the term 'temperature'.
#' 
#' 4. "FileSchmQf=value", where value is the full path to schema for valid calibration quality flags output by this
#' workflow. Use NA to create the schema from the data. If the "value" string begins with a $ (e.g. $DIR_IN), 
#' the value of the parameter will be assigned from the system environment variable matching the value string.
#' Note that the column order of the output will match the column order of the input, with the exception that any
#' terms not indicated for calibration will be omitted.
#' 
#' 5. "Term=value", where value contains any number of terms/variables of the input data to apply calibration, 
#' separated by pipes (|). For example, to apply calibration to the terms "resistance" and "voltage", then enter
#' the argument as "Term=resistance|voltage".
#' 
#' 6. "NumDayExpiMax=value", where value contains the max days since expiration to apply an expired calibration.
#' The calibration will not be applied past the indicated days-since-expiration, returning NA until a new valid 
#' calibration is found. Enter NA for infinity (the last applicable calibration will be used regardless of time
#' since expiration. Value may be a single number, in which case it will apply to all calibrated terms, or 
#' a vector the same length as the Term argument above, in which case each value will apply to the corresponding 
#' term of the Term argument. Separate vector elements with pipes (|). For example, "NumDayExpiMax=10|NA" indicates
#' to apply the calibration up to 10 days past the expiration date for the first term listed in the Term argument, 
#' and to apply the calibration any number of days past expiration for the second term listed in the Term argument. 
#' 
#' 7. "TermUcrtFdas=value" (optional), where value contains any number of terms/variables for which the FDAS (field 
#' data  aquisition system) uncertainty applies. Separate multiple terms with pipes (|) and indicate whether the 
#' resistance or voltage uncertainty applies by adding a parentheses with either R or V, respectively inside. For 
#' example, if terms resistance and voltage are subject to FDAS uncertainty, and their respective L0 units ohms and 
#' volts, respectively, the argument is "TermUcrtFdas=resistance(R)|voltage(V)". The terms listed here must be a 
#' subset of the terms listed in the Term argument above. 
#'  
#' 8. "FileUcrtFdas=value" (optional), where value is the full path to the uncertainty coefficients for the FDAS. 
#' Must be provided if TermUcrtFdas is input.
#'  
#' 9. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at 
#' the same level as the calibration folder in the input path that are to be copied with a symbolic link to the 
#' output path.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.

#' @return Directories for calibrated data, valid calibration flags, uncertainty information, FDAS uncertainty (if 
#' indicated) and any other additional subfolders specified in DirSubCopy symbolically linked in directory DirOut, 
#' where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains
#' the child directory structure of the input path. By default, the 'calibration' directory of the input path is dropped 
#' unless specified in the DirSubCopy argument.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.cal.conv.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_validCal.avsc" "Term=resistance" "NumDayExpiMax=NA"
#' 
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/prt_calibration_filter/prt/2019/01/01')
#' Rscript flow.cal.conv.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "FileSchmData=/avro_schemas/dp0p/prt_calibrated.avsc" "FileSchmQf=/avro_schemas/dp0p/flags_validCal.avsc" "Term=resistance" NumDayExpiMax=NA"
#'
#' Stepping through the code as an example
#' Sys.setenv(DIR_IN='/scratch/pfs/prt_calibration_filter')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "FileSchmData=/scratch/pfs/avro_schemas/dp0p/prt_calibrated.avsc", "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_validCal.avsc", "Term=resistance", "NumDayExpiMax=NA")

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
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","FileSchmData", "FileSchmQf",
   "Term","NumDayExpiMax"),TypePara=base::list(NumDayExpiMax="numeric"),NameParaOptn=c("DirSubCopy",
   "TermUcrtFdas","FileUcrtFdas"),log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve the schema for output data and valid calibration flags
FileSchmData <- Para$FileSchmData
log$debug(base::paste0('Schema for output data: ',FileSchmData))

FileSchmQf <- Para$FileSchmQf
log$debug(base::paste0('Schema for output flags: ',FileSchmQf))

# Turn schema names to NULL if they are NA. This will cause the avro writer the create the schema 
# from the data frames. Otherwise, read in the schemas so we only have to do it once and not every 
# time in the avro writer.
if(FileSchmData == 'NA'){
  SchmDataOut <- NULL
} else {
  # Retrieve and interpret the output data schema 
  SchmDataOutAll <- NEONprocIS.base::def.schm.avro.pars(FileSchm=FileSchmData,log=log)
  SchmDataOut <- SchmDataOutAll$schmJson
  SchmDataOutVar <- SchmDataOutAll$var
}
if(FileSchmQf == 'NA'){
  SchmQf <- NULL
} else {
  SchmQf <- base::paste0(base::readLines(FileSchmQf),collapse='')
}


# Retrieve the streams we are going to calibrate and the max days since expiration to apply an expired 
# calibration. NA for the max days indicates we will accept any previous calibration regardless of time since expired.
Term <- Para$Term
log$debug(base::paste0('Terms to apply calibration: ',base::paste0(Term,collapse=',')))

numVar <- base::length(Term)
NumDayExpiMax=Para$NumDayExpiMax
log$debug(base::paste0('Number of days past expiration to apply calibration to each term: ',base::paste0(NumDayExpiMax,collapse=',')))

# Apply logic for extending NumDayExpiMax to all terms, and error check
if(base::length(NumDayExpiMax)==1 && numVar > 1){
  NumDayExpiMax=base::rep(x=NumDayExpiMax,times=numVar)
} else if(base::length(NumDayExpiMax) != numVar){
  log$fatal(base::paste0('Input argument NumDayMaxExpi must be of length 1 or equal to the length of the Term argument.')) 
  base::stop()
}

# Assemble the calibration paramters
ParaCal <- base::data.frame(var=Term,NumDayExpiMax=NumDayExpiMax,stringsAsFactors=FALSE)

# Retrieve FDAS uncertainty information
if(!base::is.null(Para$TermUcrtFdas)){
  ParaFdas <- base::lapply(Para$TermUcrtFdas,FUN=function(argSplt){
    if(base::grepl(pattern='(R)',x=argSplt)){
      # Terms in which FDAS resistance uncertainty applies
      return(base::data.frame(var=gsub(pattern='[(R)]',replacement='',x=argSplt),typeFdas='R',stringsAsFactors=FALSE))
    } else if (base::grepl(pattern='(V)',x=argSplt)){
      return(base::data.frame(var=gsub(pattern='[(V)]',replacement='',x=argSplt),typeFdas='V',stringsAsFactors=FALSE))
    } else {
      log$fatal(base::paste0('Cannot interpet whether argument TermUcrtFdas=', argSplt, ' applies to voltage (V) or resistance (R) units. Check inputs.'))
      stop()
    }
  })
  ParaFdas <- base::do.call(base::rbind,ParaFdas)
  
  if(base::sum(!(ParaFdas$var %in% ParaCal$var)) > 0){
    log$fatal('Terms in argument TermUcrtFdas must be a subset of those in the Term argument')
    stop()
  } else {
    ParaCal <- base::merge(x=ParaCal,y=ParaFdas,by='var',all.x=TRUE,all.y=FALSE)
  }
  
  # Open FDAS uncertainty file
  if(base::is.null(Para$FileUcrtFdas)){
    log$fatal('Path to FDAS uncertainty file must be input in argument FileUcrtFdas.') 
    stop()
  }
  ucrtFdas  <- base::try(rjson::fromJSON(file=Para$FileUcrtFdas,simplify=TRUE),silent=FALSE)
  if(base::class(ucrtFdas) == 'try-error'){
    # Generate error and stop execution
    log$error(base::paste0('File: ', Para$FileUcrtFdas, ' is unreadable.')) 
    stop()
  }
  
  # Convert to data frame
  ucrtFdas <- base::lapply(ucrtFdas,base::as.data.frame,stringsAsFactors=FALSE)
  ucrtFdas <- base::do.call(base::rbind,ucrtFdas)

  # Columns names for desired uncertainty outputs from calibration function (NEONprocIS.cal::def.cal.conv)
  nameColUcrt <- c('dervCal','ucrtFdas','ucrtComb')
  
} else {
  ParaFdas <- NULL
}
log$debug(base::paste0('Terms for which to compute FDAS uncertainty information: ',base::paste0(Para$TermUcrtFdas,collapse=',')))
numVarUcrtFdas <- base::nrow(ParaFdas)

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,c('data','uncertainty','flags')))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('data','calibration',DirSubCopy))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub)
if(base::length(DirIn) == 0){
  log$warn(base::paste0('No datums found for processing in parent directory ',DirBgn))
} else {
  log$info(base::paste0('Preparing to process ',base::length(DirIn),' datums.'))
}

# Process each datum
for(idxDirIn in DirIn){

  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data and calibration(s)
  idxDirData <- base::paste0(idxDirIn,'/data')
  idxDirCal <- base::paste0(idxDirIn,'/calibration')
  fileData <- base::dir(idxDirData)
  varCal <- base::dir(idxDirCal)
  numVarCal <- base::length(varCal)
  
  # Check that the data streams we want to calibrate have calibration folders. If not, error
  exstCal <- Para$Term %in% varCal
  if (base::sum(!exstCal > 0)){
    log$error(base::paste0('No calibration folder exists for term(s): ', base::paste0(Para$Term[!exstCal],collapse=','), ' in datum path ',idxDirCal))
    stop()
  }
  
  # Create the output directories for data, flags, and uncertainty. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirOutQf <- base::paste0(idxDirOut,'/flags')
  base::dir.create(idxDirOutQf,recursive=TRUE)
  idxDirOutUcrt <- base::paste0(idxDirOut,'/uncertainty')
  base::dir.create(idxDirOutUcrt,recursive=TRUE)
  
  if(!base::is.null(ParaFdas)){
    idxDirOutUcrtFdas <- base::paste0(idxDirOut,'/uncertainty_fdas')
    base::dir.create(idxDirOutUcrtFdas,recursive=TRUE)
  }
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    base::suppressWarnings(NEONprocIS.base::def.copy.dir.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut))
    log$info(base::paste0('Unmodified subdirectories ',base::paste0(DirSubCopy,collapse=','),' of ',idxDirIn, ' copied to ',idxDirOut))
  }  
  
  # Load in data file in AVRO format into data frame 'data'. Grab the first file only, since there should only be one. 
  # Note, this AVRO reader is a developmental version. 
  if(base::length(fileData) > 1){
    log$warn(base::paste0('There is more than one data file. Using only the first: ',fileData[1]))
  }
  data  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=base::paste0(idxDirData,'/',fileData[1]),NameLib='/ravro.so'),silent=FALSE)
  if(base::class(data) == 'try-error'){
    # Generate error and stop execution
    log$error(base::paste0('File ', idxDirData,'/',fileData[1], ' is unreadable.')) 
    base::stop()
  }
  nameVarIn <- base::names(data)
    
  # Create a mapping between terms in the input data and terms in the output data (if we have an output schema)
  if(!base::is.null(SchmDataOut)){
    nameVarOut <- SchmDataOutVar$name
  } else {
    nameVarOut <- nameVarIn
  }
  if(base::length(nameVarIn) != base::length(nameVarOut)){
    log$fatal(base::paste0('File ', idxDirData,'/',fileData[1], ' contains ',base::length(nameVarIn), ' variables but the output schema specifies ',base::length(nameVarOut),
                           '. This cannot be.'))
    stop()
  }
  mappNameVar <- base::data.frame(nameVarIn=nameVarIn,nameVarOut=nameVarOut,stringsAsFactors=FALSE)
  newVar <- varCal[!(varCal %in% mappNameVar$nameVarIn)] # Calibration variables not in the input data (we'll return uncertainty info for these anyway)
  mappNameVar <- base::rbind(mappNameVar,base::data.frame(nameVarIn=newVar,nameVarOut=newVar,stringsAsFactors = FALSE))
  
  # How much data do we have?
  numData <- base::nrow(data)
  
  # Pull out the time variable
  if(!('readout_time' %in% nameVarIn)){
    log$error(base::paste0('Variable "readout_time" is required, but cannot be found in file ', idxDirData,'/',fileData[1])) 
    base::stop()
    
  }
  timeMeas <- data$readout_time
  
  # Make sure all calibrated variables exist in the data
  exstVar <- ParaCal$var %in% nameVarIn
  if(base::sum(!exstVar) > 0){
    log$error(base::paste0('Cannot apply calibration to variable(s): ',base::paste0(ParaCal$var[!exstVar],collapse=', '),
                           '. They do not exist in the data file: ',base::paste0(idxDirData,'/',fileData[1]))) 
    base::stop()
  }
  
  # Intialize the output data as NA. Don't worry about time variable and variables we aren't calibrating. These will be ignored.
  dataCal <- NA*data[ParaCal$var]
  qfExpi <- data[ParaCal$var]; # Initialize flag output
  qfExpi[,] <- 1 
  qfExpi <- base::cbind(data['readout_time'],qfExpi)
  
  # Initialize uncertainty data output
  if(!is.null(ParaFdas)){
    numColUcrtOut <- base::length(nameColUcrt)+1
    dataUcrt <- base::as.data.frame(base::matrix(data=NA,nrow=numData,ncol=numVarUcrtFdas*numColUcrtOut+1))
    
    # Populate readout time and raw resistance or voltage
    dataUcrt[,1] <- data[['readout_time']]
    base::names(dataUcrt)[1] <- 'readout_time'
    dataUcrt[,2+((1:numVarUcrtFdas)-1)*numColUcrtOut] <- data[,ParaFdas$var]
    
    # Append the column names with the name of the output data variable that they pertain to. We won't know what the 
    # original variable names were later in the pipeline
    nameVarFdasOut <- base::unlist(base::lapply(ParaFdas$var,FUN=function(idxVar){mappNameVar$nameVarOut[mappNameVar$nameVarIn==idxVar]}))
    base::names(dataUcrt)[2+((1:numVarUcrtFdas)-1)*numColUcrtOut] <- base::paste0(nameVarFdasOut,'_raw')
    setColUcrt <- base::unlist(base::lapply(2+((1:numVarUcrtFdas)-1)*numColUcrtOut,FUN=function(numIdx){(1:base::length(nameColUcrt))+numIdx}))
    base::names(dataUcrt)[setColUcrt] <- base::unlist(base::lapply(nameVarFdasOut,FUN=function(idxVar){base::paste0(idxVar,'_',nameColUcrt)}))
  }

  # Initialize uncertainty coeficient info
  ucrt <- base::vector(mode = "list", length = numVarCal)
  base::names(ucrt) <- varCal

  # Run through each term that has calibration information supplied. Save its uncertainty info and apply calibration if requested
  for(idxVarCal in varCal){
    
    # Intialize idCal for this term
    idCal <- base::rep(NA,numData)
    
    # Directory listing of cal files for this data stream
    DirCalVar <- base::paste0(idxDirCal,'/',idxVarCal)
    fileCal <- base::dir(DirCalVar)
    numCal <- base::length(fileCal) 
    
    # If no calibration exists, report output data as NA and raise the valid (expired) cal flag (already initialized this way)
    if(numCal == 0){
      if(idxVarCal %in% ParaCal$var){
        log$warn(base::paste0('No calibration files in ',DirCalVar, '. All data will be NA and valid calibration flag will be raised.'))
      } else {
        log$warn(base::paste0('No calibration files in ',DirCalVar, '. No uncertainty information will be available for this term.'))
      }
      next
    }
    
    # Read in each calibration file, saving the valid start/end dates, certificate number, and uncertainty information
    metaCal <- base::vector(mode = "list", length = numCal)
    ucrtCal <- base::vector(mode = "list", length = numCal)
    idxVarCalOut <- mappNameVar$nameVarOut[mappNameVar$nameVarIn==idxVarCal] # Grab the L0' term for this L0 variable
    for(idxFile in base::seq_len(numCal)){
      cal <- NEONprocIS.cal::def.read.cal.xml(NameFile=base::paste0(DirCalVar,'/',fileCal[idxFile]),Vrbs=TRUE)
      metaCal[[idxFile]] <- base::data.frame(file=fileCal[idxFile],timeValiBgn=cal$timeVali$StartTime,timeValiEnd=cal$timeVali$EndTime,
                              id=base::as.numeric(cal$file$StreamCalVal$CertificateNumber),
                              stringsAsFactors=FALSE)
      
      # Add in FDAS uncertainty
      typeFdas <- ParaCal$typeFdas[ParaCal$var==idxVarCal]
      if(base::length(typeFdas)!=0){
        setFdasCoef <- base::switch(typeFdas,
                                  R=base::grepl(pattern='U_CVAL[RF]',ucrtFdas$Name),
                                  V=base::grepl(pattern='U_CVAL[VG]',ucrtFdas$Name),
                                  base::logical(0))
        cal$ucrt <- base::rbind(cal$ucrt,ucrtFdas[setFdasCoef,],stringsAsFactors=FALSE)
      }
      
      
      # Add in cal id and applicable term
      cal$ucrt$id <- cal$file$StreamCalVal$CertificateNumber
      cal$ucrt$term <- idxVarCalOut
      ucrtCal[[idxFile]]<- cal$ucrt
    }
    metaCal <- base::Reduce(f=base::rbind,x=metaCal)
    ucrtCal <- base::Reduce(f=base::rbind,x=ucrtCal)
    
    # Sort by certificate number (higher = more recent)
    setSort <- base::sort(metaCal$id,decreasing=FALSE,index.return=TRUE)$ix
    metaCal <- metaCal[setSort,]
    
    # Are any fully expired? Put those first in the list of cals
    setCalExpi <- base::which(metaCal$timeValiEnd < timeBgn)
    metaCal$expi <- FALSE # Add a field for expired cals
    metaCal$expi[setCalExpi]<- TRUE
    metaCal <- metaCal[c(setCalExpi,base::setdiff(base::seq.int(numCal),setCalExpi)),]
    
    # Apply the calibrations in this order, expired first. Then by increasing certificate order for the valid date range. 
    # Expired cals will be overwritten by non-expired cals, and newer overlapping cals will overwrite older cals. 
    if(idxVarCal %in% ParaCal$var){
      NumDayExpiMax <- ParaCal$NumDayExpiMax[ParaCal$var == idxVarCal] # Max number of days since expiration to apply expired cal
    } else {
      NumDayExpiMax <- NA
    }
    for(idxCal in metaCal$file){
      
      # Read the cal file
      cal <- NEONprocIS.cal::def.read.cal.xml(NameFile=base::paste0(DirCalVar,'/',idxCal),Vrbs=TRUE)
      
     # Apply the cal to data after the valid date until the expiration plus the max time since expired. 
      if(base::is.na(NumDayExpiMax)){
        # Any time since expired
        setCal <- timeMeas >= cal$timeVali$StartTime
      } else {
        # Only time since expired + max days since expiration. 
        setCal <- timeMeas >= cal$timeVali$StartTime & timeMeas <= (cal$timeVali$EndTime+base::as.difftime(NumDayExpiMax,units='days'))
      }
      
      # Apply an expired calibration only until there is another cal available.
      if(base::sum(metaCal$timeValiBgn <= cal$timeVali$EndTime & metaCal$timeValiEnd > cal$timeVali$EndTime) > 0){
        # There is another cal that extends longer than this one without a gap. No expired cal application.
        setCal <- setCal & !(timeMeas > cal$timeVali$EndTime)
      } else if (base::sum(metaCal$timeValiBgn > cal$timeVali$EndTime) > 0){
        # There is a gap between this cal and another cal. Let's not apply an expired cal beyond that gap
        setCal <- setCal & !(timeMeas >= min(metaCal$timeValiBgn[metaCal$timeValiBgn > cal$timeVali$EndTime]))
      }
      
      # Save which ID was used for this data. We'll use it later to pull uncertainty information
      idCal[setCal] <- metaCal$id[metaCal$file==idxCal]
      
      # Calibrate the data if requested
      if(idxVarCal %in% ParaCal$var){
        
        # Which FDAS uncertainty coefs - resistance or voltage?
        if(idxVarCal %in% ParaFdas$var){
          coefUcrtFdasIdx <- base::switch(ParaCal$typeFdas[ParaCal$var == idxVarCal],
            R=ucrtFdas$Value[ucrtFdas$Name=="U_CVALR1"],
            V=ucrtFdas$Value[ucrtFdas$Name=="U_CVALV1"]
          )
          coefUcrtFdasOfstIdx <- base::switch(ParaCal$typeFdas[ParaCal$var == idxVarCal],
            R=ucrtFdas$Value[ucrtFdas$Name=="U_CVALR4"],
            V=ucrtFdas$Value[ucrtFdas$Name=="U_CVALV4"]
          )       
        } else {
          coefUcrtFdasIdx <- NULL
          coefUcrtFdasOfstIdx <- NULL
        }
        
        # Calibrate the data & compute FDAS uncertainty
        dataCalIdxVar <- NEONprocIS.cal::def.cal.conv(data=data[setCal,idxVarCal],cal=cal$cal,
                         coefUcrtMeas=base::as.numeric(cal$ucrt$Value[cal$ucrt$Name=="U_CVALA1"]),
                         coefUcrtFdas=base::as.numeric(coefUcrtFdasIdx),
                         coefUcrtFdasOfst=base::as.numeric(coefUcrtFdasOfstIdx)) 
        
        # Assign outputs
        dataCal[setCal,idxVarCal] <- dataCalIdxVar$data
        if(idxVarCal %in% ParaFdas$var){
          idxColUcrt <- base::which(base::names(dataUcrt)==base::paste0(idxVarCalOut,'_raw'))
          dataUcrt[setCal,(idxColUcrt+1):(idxColUcrt+numColUcrtOut-1)] <- dataCalIdxVar$ucrt[,nameColUcrt]
        }
          
        # Mark which values were calibrated within the valid time range
        setVali <- timeMeas >= cal$timeVali$StartTime & timeMeas <= cal$timeVali$EndTime # Indices of valid calibration
        qfExpi[setVali,idxVarCal] <- 0        
      }
      
    } # End loop around calibration files
    

    # Save the uncertainty info for this term
    idCalUse <- base::unique(idCal) # unique calibration ids applicable to the data 
    setCalBgn <- base::data.frame(idCal=character(0),idxBgn=numeric(0))
    for(idxCalUse in idCalUse){
      
      # Data indices for which this calibration id applies
      setCalUse <- base::which(idCal == idxCalUse) 
      
      # Go through continuous blocks of time, writing the value of the coefficient to json
      brk <- setCalUse[base::which(base::diff(setCalUse) > 1)+1]
      setCalBgn <- base::rbind(setCalBgn,base::data.frame(idCal=rep(idxCalUse,base::length(brk)+1),idxBgn=c(setCalUse[1],brk)))
      
    } # End loop around calibrations used
    setCalBgn <- setCalBgn[base::order(setCalBgn$idxBgn),]
    
    # Set beginning times for the cal periods based on their valid calibration dates
    timeBrkBgn <- base::data.frame(timeBgn=NULL)
    for(idxBrk in base::seq_len(base::nrow(setCalBgn))){
      if(idxBrk == 1){
        # For the first row, it's the max of timeBgn or timeValiBgn for the cal id  
        timeBrkBgn <- base::rbind(timeBrkBgn,base::data.frame(timeBgn=base::max(metaCal$timeValiBgn[metaCal$id==setCalBgn$idCal[idxBrk]],timeBgn)))
      } else {
        # For the remaining rows, it's the max of timeValiBgn for the cal id or timeValiEnd for the previous cal id, so long as it is at/before ...
        # the measurement time at which the cal id is used for this break point
        timeBrkBgnOptn <- base::rbind(base::data.frame(timeBgn=metaCal$timeValiBgn[metaCal$id==setCalBgn$idCal[idxBrk]]),
                                   base::data.frame(timeBgn=metaCal$timeValiEnd[metaCal$id==setCalBgn$idCal[idxBrk-1]]))
        # Select the correct start time to assign to the break
        timeBrkBgn <- base::rbind(timeBrkBgn,base::data.frame(timeBgn=base::max(timeBrkBgnOptn$timeBgn[timeBrkBgnOptn$time <= timeMeas[setCalBgn$idxBgn[idxBrk]]])))
      }
    }
    
    # Assign end times
    timeBrkEnd <- base::subset(timeBrkBgn,subset=base::seq_len(nrow(setCalBgn)) %in% utils::tail(seq_len(nrow(setCalBgn)),n=-1))
    names(timeBrkEnd) <- "timeEnd"
    timeBrkEnd <- base::rbind(timeBrkEnd,base::data.frame(timeEnd=timeBgn + base::as.difftime(1,units='days')))
    setCalBgn <- base::cbind(setCalBgn,timeBrkBgn,timeBrkEnd) # Consolidate

    # Merge the uncertainty coefs and calibration periods
    ucrt[[idxVarCal]] <- base::merge(x=setCalBgn,y=ucrtCal,by.x='idCal',by.y='id')
    
  } # End loop around variables to calibrate
    
  # Replace the original data with the calibrated data
  data[,base::names(dataCal)] <- dataCal
  
  # Write out the calibrated data
  options(digits.secs = 3)
  NameFileOutData <- base::paste0(idxDirOutData,'/',fileData[1])
  rptData <- base::try(NEONprocIS.base::def.wrte.avro.deve(data=data,NameFile=NameFileOutData,NameFileSchm=NULL,Schm=SchmDataOut,NameLib='/ravro.so'),silent=TRUE)
  if(base::class(rptData) == 'try-error'){
    log$error(base::paste0('Cannot write Calibrated data to ', NameFileOutData,'. ',attr(rptData,"condition"))) 
    stop()
  } else {
    log$info(base::paste0('Calibrated data written successfully in ',NameFileOutData))
  }
  
  # Write out the valid calibration flags
  fileDataSplt <- base::strsplit(fileData[1],'[.]')[[1]] # Try to grab the file name without extension
  if(base::length(fileDataSplt) > 1){
    NameFileOutQfExpi <- base::paste0(idxDirOutQf,'/',base::paste0(fileDataSplt[-base::length(fileDataSplt)],collapse='.'),'_validCal.',utils::tail(fileDataSplt,1))
  } else {
    NameFileOutQfExpi <- base::paste0(idxDirOutQf,'/',fileData[1],'_validCal')
  }
  qfExpi[,ParaCal$var] <- base::as.integer(qfExpi[,ParaCal$var])  # Use as.integer in order to write out as integer with the avro schema
  rptQfExpi <- NEONprocIS.base::def.wrte.avro.deve(data=qfExpi,NameFile=NameFileOutQfExpi,NameFileSchm=NULL,Schm=SchmQf,NameLib='/ravro.so')
  if(rptQfExpi == 0){
    log$info(base::paste0('Valid calibration flags written successfully in ',NameFileOutQfExpi))
  }
  
  # Simplify & make pretty the uncertainty information
  ucrt <- base::Reduce(f=base::rbind,x=ucrt) # merge list of data frames into single data frame
  ucrt$timeBgn <- base::format(ucrt$timeBgn,format='%Y-%m-%dT%H:%M:%OSZ') # Convert POSIX to character
  ucrt$timeEnd <- base::format(ucrt$timeEnd,format='%Y-%m-%dT%H:%M:%OSZ')
  ucrt[['source_id']] <- data$source_id[1] # Add source id 
  ucrt <- ucrt[c('source_id','term','timeBgn','timeEnd','Name','Value','.attrs')] # rename columns
  base::names(ucrt) <- c('source_id','term','start_date','end_date','Name','Value','.attrs') # reorganize columns
  
 # Write uncertainty info to json format
  ucrtList <- base::split(ucrt, base::seq(base::nrow(ucrt))) # Turn into a list for writing out in json format
  base::names(ucrtList) <- NULL
  ucrtList <- base::lapply(ucrtList,base::as.list)
  
  if(base::length(fileDataSplt) > 1){
    NameFileOutUcrt <- base::paste0(idxDirOutUcrt,'/',base::paste0(fileDataSplt[-base::length(fileDataSplt)],collapse='.'),'_uncertainty.json')
  } else {
    NameFileOutUcrt <- base::paste0(idxDirOutUcrt,'/',fileData[1],'_uncertainty.json')
  }
  
  rptUcrt <- base::write(rjson::toJSON(ucrtList,indent=3),file=NameFileOutUcrt)
  log$info(base::paste0('Uncertainty coefficients written successfully in ',NameFileOutUcrt))
  
  # Write out FDAS uncertainty data
  if(!base::is.null(ParaFdas)){
    if(base::length(fileDataSplt) > 1){
      NameFileOutUcrtFdas <- base::paste0(idxDirOutUcrtFdas,'/',base::paste0(fileDataSplt[-base::length(fileDataSplt)],collapse='.'),'_FDASUncertainty.',utils::tail(fileDataSplt,1))
    } else {
      NameFileOutUcrtFdas <- base::paste0(idxDirOutUcrtFdas,'/',fileData[1],'_FDASUncertainty')
    }
    rptUcrtFdas <- NEONprocIS.base::def.wrte.avro.deve(data=dataUcrt,NameFile=NameFileOutUcrtFdas,NameLib='/ravro.so')
    if(rptUcrtFdas == 0){
      log$info(base::paste0('FDAS uncertainty data written successfully in ',NameFileOutUcrtFdas))
    }
  }

}
