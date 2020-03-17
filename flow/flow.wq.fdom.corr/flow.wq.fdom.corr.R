
##############################################################################################
#' @title Workflow for correcting fdom for temperature and absorbance

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Apply temperature and absorbance corrections to fdom data for the
#' water quality transition.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /named_location_group/exofdom/SENSOR/data
#'         /named_location_group/exofdom/SENSOR/calibration
#'         /named_location_group/exoconductivity/SENSOR/data
#'         /named_location_group/exoconductivity/SENSOR/uncertainty_data
#'         /named_location_group/SUNA/SENSOR/data
#'         /named_location_group/SUNA/SENSOR/calibration
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 4. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Corrected fdom data and associated flags for temperature and absorbance corrections.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/scratch/pfs/waterQuality_exofdom_correction_group')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=scratch/pfs/out", "FileSchmData=/scratch/pfs/avro_schemas/dp0p/exofdom_corrected.avsc", "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_correction_exofdom.avsc")

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut"),NameParaOptn = c("FileSchmData","FileSchmQf"),log = log)

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Schema for output data: ', Para$FileSchmData))
log$debug(base::paste0('Schema for output flags: ', Para$FileSchmQf))

# String constants for CVAL files
rhoNamefdom <- "CVALA1" #Calibration factor for temperature correction function for fDOM
pathNamefdom <- "CVALB1" #Calibration factor for absorbance correction function for fDOM
calTableNameSUNA <- "CVALTABLEA1" #Calibration table component containing wavelength (independet variable) and reference spectrum (dependent variable)
fdomNameUncrt <- "U_CVALA1" # Combined, standard uncertainty of fluorescent dissolved organic matter; provided by CVAL
tempCorrNameUncrt <- "U_CVALA4" # Combined, standard uncertainty of temperature correction function for fDOM; provided by CVAL
absCorrNameUncrt <- "U_CVALA5" # Combined, standard uncertainty of absorbance correction function for fDOM; provided by CVAL

# Other string constants
ravroLib <- "/ravro.so"
timestampFormat <- "%Y-%m-%d %H:%M"

#Default Values for applying corrections
applyTempCorr <- TRUE
applyAbsCorr <- TRUE

# Default values for rho and pathlength, useful for older data where cal files don't have the info yet
rho_fdom <- -0.01024
uncrt_rho_fdom <- 0.002458
pathlength <- 0.330
uncrt_pathlength <- 0.178

# Read in the schemas so we only have to do it once and not every time in the avro writer.
if (!base::is.null(Para$FileSchmData)) {
  # Retrieve and interpret the output data schema
  SchmDataOutAll <- NEONprocIS.base::def.schm.avro.pars(FileSchm = Para$FileSchmData, log = log)
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

# A datum must have fDOM data and optionally exoconductivity and sunav2
nameDirSub <- c('exofdom')

log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,nameDirSub = nameDirSub,log = log)

if(base::length(DirIn) < 1){
  log$fatal('No datums found to process.')
  stop()
}

# Process each datum
for (idxDirIn in DirIn){
  ##### Logging and initializing #####
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Create the base output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  # Need the source ID for the sensor to write out data
  IdxSensor <- base::dir(base::paste0(idxDirIn,"/exofdom"))
  if(length(IdxSensor) != 1){
    log$fatal('More than one source ID for exofdom discovered.')
    stop()
  }
  # The time frame of the data is one day, and this day is indicated in the directory structure.
  if (base::is.null(InfoDirIn$time)) {
    # Generate error and stop execution
    log$fatal(
      base::paste0(
        'Cannot interpret data date from input directory structure: ',
        InfoDirIn$dirRepo
      )
    )
    stop()
  }
  timeBgn <- InfoDirIn$time # start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut, '/exofdom/data')
  idxDirOutFlags <- base::paste0(idxDirOut, '/exofdom/flags')
  NEONprocIS.base::def.dir.crea(
    DirBgn = '/',
    DirSub = c(idxDirOutData, idxDirOutFlags),
    log = log
  )
  
  ##### Read in fDOM data #####
  fdomDataGlob <- base::file.path(idxDirIn,"exofdom","*","data","*")
  fdomDataPath <- base::Sys.glob(fdomDataGlob)
  if(base::length(fdomDataPath)==1){
    fdomData <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile = base::paste0(fdomDataPath),NameLib = ravroLib,log = log), silent = FALSE)
    log$debug(base::paste0("One datum found, reading in: ",fdomDataPath))
  }else{
    log$debug(base::paste0("Zero or more than one datum found for: ",fdomDataGlob))
    #For some reason I'm getting an error when I use base::next()
    next()
  }
  
  if (base::class(fdomData) == 'try-error') {
    # Generate error and stop execution
    #log$error(base::paste0('File ', idxDirData, '/', fdomData, ' is unreadable.'))
    log$error(base::paste0('File: ', fdomDataGlob, ' is unreadable.'))
    stop()
  }
  
  # Validate the data
  valiData <-
    NEONprocIS.base::def.validate.dataframe(
      dfIn = fdomData,
      TestNameCol = base::unique(c('readout_time', 'fDOM')),
      log = log
    )
  if (!valiData) {
    # Fail if there isn't valid data so that we can troubleshoot the missing data.
    log$fatal(base::paste0('File: ', fdomDataGlob, ' is invalid.'))
    stop()
  }
  
  #Default all flags to -1 then change them as the test can be performed
  fdomData$fDOMTempQF <- -1
  fdomData$fDOMAbsQF<- -1
  fdomData$spectrumCount <- -1
  
  #Default expanded uncertainty to NA until it can be calculated
  fdomData$fDOMExpUncert <- NA
  
  #Default temp and absorbance correction factors
  #fdomData$surfaceWaterTemperature <- NA #This gets merged in from exoconductivity data
  fdomData$tempFactor <- NA
  fdomData$pathlength <- NA
  fdomData$ucrt_pathlength <- NA
  fdomData$Abs_ex <- NA
  fdomData$Abs_em <- NA
  fdomData$absFactor <- NA
  fdomData$uncrt_A1_fdom <- NA
  fdomData$rho_fdom <- NA
  fdomData$uncrt_rho_fdom <- NA
  fdomData$ucrt_A_ex <- NA
  fdomData$ucrt_A_em <- NA
  
  #Populate rawCalibratedfDOM
  fdomData$rawCalibratedfDOM <- fdomData$fDOM
  
  ##### Read in fDOM cal and uncertaitnty information #####
  fdomCalGlob <- base::file.path(idxDirIn,"exofdom","*","calibration","fDOM","*")
  fdomCalPath <- base::Sys.glob(fdomCalGlob)
  
  if(base::length(fdomCalPath)>=1){
    log$debug("One or more fDOM cal file(s) found")
    
    #Pull fDOM A1 uncertainty, rho_fdom, and pathlength from the appropriate cal file
    metaCal <- NEONprocIS.cal::def.cal.meta(fileCal = fdomCalPath,log = log)
    # Determine the calibrations that apply for this day
    calSlct <- NEONprocIS.cal::def.cal.slct(metaCal = metaCal,
                                            TimeBgn = timeBgn,
                                            TimeEnd = timeEnd,
                                            log = log)
    fileCalSlct <- base::setdiff(base::unique(calSlct$file), 'NA')
    numFileCalSlct <- base::length(fileCalSlct)

    # Move on without applying corrections if there isn't a valid fDOM cal file
    if(numFileCalSlct >= 1){
      # Loop through the cal files to pull out the values we need
      for(calIdx in calSlct$file[!is.na(calSlct$file)]){
        calFilefdom <- NEONprocIS.cal::def.read.cal.xml(NameFile = base::Sys.glob(base::file.path(idxDirIn,"exofdom","*","calibration","fDOM",calIdx)),Vrbs = FALSE)
        #CVAL has only one rho_fdom value for all sensors and sites
        fdomRangeToApplyCal <- which(fdomData$readout_time >= calSlct$timeBgn[calSlct$file==calIdx] & fdomData$readout_time < calSlct$timeEnd[calSlct$file==calIdx])
        fdomData$uncrt_A1_fdom[fdomRangeToApplyCal] <- try(base::as.numeric(calFilefdom$ucrt$Value[gsub(" ","",calFilefdom$ucrt$Name) == fdomNameUncrt]))
        fdomData$rho_fdom[fdomRangeToApplyCal] <- try(base::as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == rhoNamefdom]))
        fdomData$uncrt_rho_fdom[fdomRangeToApplyCal] <- try(base::as.numeric(calFilefdom$ucrt$Value[gsub(" ","",calFilefdom$ucrt$Name) == tempCorrNameUncrt]))
        fdomData$pathlength[fdomRangeToApplyCal] <- try(base::as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == pathNamefdom]))
        fdomData$ucrt_pathlength[fdomRangeToApplyCal] <- try(base::as.numeric(calFilefdom$ucrt$Value[gsub(" ","",calFilefdom$ucrt$Name) == absCorrNameUncrt]))
      }
    }else{
      log$fatal("Error: No fDOM cal file(s) found that match selected dates.")
      stop()
    }

  }else{
    log$fatal(base::paste0("Zero fDOM cal files found for: ",fdomDataGlob))
    stop()
  }
  
  ##### Temperature Corrections #####
  # Try to pull prt data if we're at a stream site? Holding on this.
  # Look for and read in exoconductivity data
  exoconductivityDataGlob <- base::file.path(idxDirIn,"exoconductivity","*","data","*")
  exoconductivityDataPath <- base::Sys.glob(exoconductivityDataGlob)
  
  if(base::length(exoconductivityDataPath)==1){
    exoconductivityData <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile = base::paste0(exoconductivityDataPath),NameLib = ravroLib,log = log), silent = FALSE)
    log$debug(base::paste0("One file found, reading in: ",exoconductivityDataPath))
  }else{
    log$debug(base::paste0("Zero or more than one file found for: ",exoconductivityDataGlob))
    applyTempCorr <- FALSE
    fdomData$fDOMTempQF <- 1
  }
  
  if(applyTempCorr && base::class(exoconductivityData) == 'try-error') {
    # Generate error and stop execution
    #log$error(base::paste0('File ', idxDirData, '/', fdomData, ' is unreadable.'))
    log$error(base::paste0('File: ', exoconductivityData, ' is unreadable, temp corrections will not be applied.'))
    applyTempCorr <- FALSE
    fdomData$fDOMTempQF <- 1
  }else if(applyTempCorr){
    # Validate the data
    valiData <-
      NEONprocIS.base::def.validate.dataframe(
        dfIn = exoconductivityData,
        TestNameCol = base::unique(c('readout_time', 'surfaceWaterTemperature')),
        log = log
      )
    if (!valiData) {
      applyTempCorr <- FALSE
      fdomData$fDOMTempQF <- 1
    }
  }
  
  # Only need to read in calibration information for temp corrections if we're able to do them
  if(applyTempCorr){
    
    #Combine fdom and temp
    #fdomData$readout_time_min <- base::format(fdomData$readout_time, format = timestampFormat)
    #exoconductivityData$readout_time_min <- base::format(exoconductivityData$readout_time, format = timestampFormat)
    fdomData <- base::merge(fdomData, exoconductivityData, by = c("readout_time"), all.x = TRUE, suffixes = c(".fdom",".temp"))
    
    #Populate the fdom temperature QF values
    fdomData$fDOMTempQF[!base::is.na(fdomData$surfaceWaterTemperature)] <- 0
    fdomData$fDOMTempQF[base::is.na(fdomData$surfaceWaterTemperature)] <- 1
    
    #Determine fdom temperature correction factor
    fdomData$tempFactor <- 1/(1+fdomData$rho_fdom*(fdomData$surfaceWaterTemperature-20))
    
    #Pull uncertainty information for temperature data
    exoconductivityUcrtGlob <- base::file.path(idxDirIn,"exoconductivity","*","uncertainty_data","*")
    exoconductivityUcrtPath <- base::Sys.glob(exoconductivityUcrtGlob)
    
    exoconductivityUcrtData <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile = base::paste0(exoconductivityUcrtPath),NameLib = ravroLib,log = log), silent = FALSE)
    #exoconductivityUcrtData$readout_time_min <- base::format(exoconductivityUcrtData$readout_time, format = timestampFormat)
    fdomData <- base::merge(fdomData, exoconductivityUcrtData, by = c("readout_time"), all.x = TRUE, suffixes = c(".fdom",".tempUcrt"))
  
  }
  
  ##### Absorbance Correction #####
  # Look for and read in sunav2 data
  sunav2DataGlob <- base::file.path(idxDirIn,"sunav2","*","data","*")
  sunav2DataPath <- base::Sys.glob(sunav2DataGlob)
  
  if(base::length(sunav2DataPath) >= 1){
    log$debug(base::paste0(length(sunav2DataPath)," SUNA file(s) found, reading in: ", sunav2DataPath))
  }else{
    log$debug(base::paste0("Zero SUNA files found for: ",sunav2DataGlob))
    applyAbsCorr <- FALSE
    fdomData$fDOMAbsQF <- 1
  }
  
  #Only need to read in calibration information for abs corrections if we're able to do them
  if(applyAbsCorr){
    #Look for and read in sunav2 calibration data
    sunav2CalDataGlob <- base::file.path(idxDirIn,"sunav2","*","calibration","rawNitrateSingleCompressedStream","*")
    sunav2CalDataPath <- base::Sys.glob(sunav2CalDataGlob)
    
    if(base::length(sunav2CalDataPath) >= 1){
      log$debug(base::paste0(length(sunav2CalDataPath)," SUNA calibration file(s) found, reading in: ", sunav2CalDataPath))
    }else{
      log$debug(base::paste0("Zero SUNA calibration files found for: ",sunav2CalDataGlob))
      applyAbsCorr <- FALSE
      fdomData$fDOMAbsQF <- 1
    }
    
    #Determine fdom absorbance correction factor
    absData <- try(NEONprocIS.wq::def.wq.abs.corr(sunav2Filenames = sunav2DataPath, sunav2CalFilenames = sunav2CalDataPath),silent = TRUE)
    
    # Merge in abs data to the fdomData dataframe and calculate correction factors and flags
    for(idxAbsData in absData$readout_time){
      if(idxAbsData <= base::max(fdomData$readout_time)){
        nextReadout <- base::min(absData$readout_time[absData$readout_time > idxAbsData])
        fdomData$Abs_ex[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$Abs_ex[absData$readout_time == idxAbsData]
        fdomData$Abs_em[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$Abs_em[absData$readout_time == idxAbsData]
        fdomData$ucrt_A_ex[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$ucrt_A_ex[absData$readout_time == idxAbsData]
        fdomData$ucrt_A_em[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$ucrt_A_em[absData$readout_time == idxAbsData]
        fdomData$spectrumCount[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$spectrumCount[absData$readout_time == idxAbsData]
      }else{
        break
      }
      
    }
    
    #Populate the fdom absorbance QF values
    fdomData$fDOMAbsQF[!base::is.na(fdomData$Abs_ex)&!base::is.na(fdomData$Abs_em)] <- 0
    fdomData$fDOMAbsQF[base::is.na(fdomData$Abs_ex)|base::is.na(fdomData$Abs_em)] <- 1
    
    #Determine fdom absorbance correction factor
    fdomData$absFactor <- 10^((fdomData$Abs_ex + fdomData$Abs_em) * fdomData$pathlength)
  }
  
  #Calculate the fdom output with the factors that exist
  fdomData$fdom_out <- fdomData$fDOM * fdomData$tempFactor * fdomData$absFactor
  
  ##### Uncertainty Calculations #####
  #Uncertainty when no corrections can be applied
  noCorrIdx <- base::which(fdomData$fDOMTempQF != 0 & fdomData$fDOMAbsQF != 0)
  fdomData$fDOMExpUncert[noCorrIdx] <- fdomData$uncrt_A1_fdom[noCorrIdx] * fdomData$fDOM[noCorrIdx]
  
  #Uncertainty when temperature corrections, only, are applied
  tempOnlyIdx <- base::which(fdomData$fDOMTempQF == 0 & fdomData$fDOMAbsQF != 0)
  fdomData$fDOMExpUncert[tempOnlyIdx] <- sqrt((fdomData$uncrt_A1_fdom[tempOnlyIdx] * fdomData$fDOM[tempOnlyIdx])^2 + (fdomData$surfaceWaterTemperature_ucrtComb[tempOnlyIdx] * fdomData$fDOM[tempOnlyIdx])^2 + (fdomData$uncrt_rho_fdom[tempOnlyIdx] * fdomData$fDOM[tempOnlyIdx])^2)
  
  #Uncertainty when absorbance corrections, only, are applied
  absOnlyIdx <- base::which(fdomData$fDOMTempQF != 0 & fdomData$fDOMAbsQF == 0)
  fdomData$fDOMExpUncert[absOnlyIdx] <- sqrt((fdomData$uncrt_A1_fdom[absOnlyIdx] * fdomData$fDOM[absOnlyIdx])^2 + (fdomData$ucrt_pathlength[absOnlyIdx] * fdomData$fDOM[absOnlyIdx])^2 + (fdomData$ucrt_A_ex[absOnlyIdx] * fdomData$fDOM[absOnlyIdx])^2 + (fdomData$ucrt_A_em[absOnlyIdx] * fdomData$fDOM[absOnlyIdx])^2)
  
  #Uncertainty when both temperature and absorbance corrections are applied
  tempAnAbsIdx <- base::which(fdomData$fDOMTempQF == 0 & fdomData$fDOMAbsQF == 0)
  fdomData$fDOMExpUncert[tempAnAbsIdx] <- sqrt((fdomData$uncrt_A1_fdom[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2 + (fdomData$surfaceWaterTemperature_ucrtComb[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2 + (fdomData$uncrt_rho_fdom[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2 + (fdomData$ucrt_pathlength[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2 + (fdomData$ucrt_A_ex[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2 + (fdomData$ucrt_A_em[tempAnAbsIdx] * fdomData$fDOM[tempAnAbsIdx])^2)
  
  ##### Writing out data and flag files #####
  #Write an AVRO file for data and uncertainty (which get stats)
  #readout_time, fDOM, fDOMExpUncert, spectrumCount
  dataOutputs <- c("readout_time", "fDOM", "fDOMExpUncert", "spectrumCount")
  dataOut <- fdomData[,which(names(fdomData)%in%dataOutputs)]
  names(dataOut) <- dataOutputs
  NEONprocIS.base::def.wrte.avro.deve(data = dataOut,
                                      NameFile = base::paste0(idxDirOutData,"/exofdom_",IdxSensor,"_",format(timeBgn,format = "%Y-%m-%d"),"_correctedData.avro"),
                                      NameSchm = Para$FileSchmQf,
                                      NameLib = ravroLib)
  
  #Write an AVRO file for the flags (which get metrics)
  #readout_time, fDOMTempQF, fDOMAbsQF
  flagOutputs <- c("readout_time", "fDOMTempQF", "fDOMAbsQF")
  flagsOut <- fdomData[,which(names(fdomData)%in%flagOutputs)]
  names(flagsOut) <- flagOutputs
  NEONprocIS.base::def.wrte.avro.deve(data = flagsOut, 
                                      NameFile = base::paste0(idxDirOutData,"/exofdom_",IdxSensor,"_",format(timeBgn,format = "%Y-%m-%d"),"_correctionFlags.avro"), 
                                      NameSchm = Para$FileSchmData, 
                                      NameLib = ravroLib )
}


