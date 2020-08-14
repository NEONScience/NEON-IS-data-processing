
##############################################################################################
#' @title Workflow for Below Zero Pressure Flag

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. Flags pressure, temperature, and conductivity for the Level Troll 500 and Aqua Troll 200
#' when pressure is below zero.
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
#'         /aquatroll200/yyyy/mm/dd/SENSOR/data
#'         /aquatroll200/yyyy/mm/dd/SENSOR/flags
#'         /aquatroll200/yyyy/mm/dd/SENSOR/uncertainty_coef
#'         /aquatroll200/yyyy/mm/dd/SENSOR/uncertainty_data
#'         /leveltroll500/yyyy/mm/dd/SENSOR/data
#'         /leveltroll500/yyyy/mm/dd/SENSOR/flags
#'         /leveltroll500/yyyy/mm/dd/SENSOR/uncertainty_coef
#'         /leveltroll500/yyyy/mm/dd/SENSOR/uncertainty_data
#'         
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
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. Note that the 
#' 
#' If no output schema is provided for the flags, the output column/variable names will be 
#' readout_time, qfFlow, qfHeat, in that order. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS 
#' MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/scratch/pfs/waterQuality_exofdom_correction_group')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN", "DirOut=/scratch/pfs/out", "FileSchmData=/scratch/pfs/avro_schemas/dp0p/aquatroll200_corrected.avsc", "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_correction_exofdom.avsc")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2020-08-01)
#     original creation
##############################################################################################

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut"),NameParaOptn = c("FileSchmData","FileSchmQf"),log = log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve the sensor and term for pressure
SensTermPressure <- base::list(sens=Para$SensTermPressure[1],term=Para$SensTermPressure[2])
if(base::sum(base::is.na(base::unlist(SensTermPressure)))>0){
  log$fatal('Invalid parameter formulation for SensTermCond. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for pressure: ',base::paste0(SensTermPressure,collapse=', ')))


# Retrieve the sensor and term for temperature
SensTermTemp <- base::list(sens=Para$SensTermTemp[1],term=Para$SensTermTemp[2])
if(base::sum(base::is.na(base::unlist(SensTermTemp)))>0){
  log$fatal('Invalid parameter formulation for SensTermTemp. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for temperature: ',base::paste0(SensTermTemp,collapse=', ')))


# Retrieve the sensor and term for conductivity
SensTermCond <- base::list(sens=Para$SensTermCond[1],term=Para$SensTermCond[2])
if(base::sum(base::is.na(base::unlist(SensTermCond)))>0){
  log$fatal('Invalid parameter formulation for SensTermCond. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for conductivity: ',base::paste0(SensTermCond,collapse=', ')))




# Read in the schemas
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




# Retrieve the sensor and term for turbine speed
SensTermTbne <- base::list(sens=Para$SensTermTbne[1],term=Para$SensTermTbne[2])
if(base::sum(base::is.na(base::unlist(SensTermTbne)))>0){
  log$fatal('Invalid parameter formulation for SensTermTbne. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for turbine speed: ',base::paste0(SensTermTbne,collapse=', ')))
log$debug(base::paste0('Min turbine speed: ', Para$AvelTbneMin, ' rpm'))

# Retrieve the sensors and terms for wind speed 
nameParaSensWind <- base::names(Para)[names(Para) %in% base::paste0("SensTermWind",1:100)]
numParaSensWind <- base::length(nameParaSensWind)

spltSensWind <- Para[base::sort(nameParaSensWind,decreasing=FALSE)]
SensTermWind <- base::lapply(spltSensWind,FUN=function(argSplt){
  if(base::length(argSplt) < 2){
    log$fatal('Invalid parameter formulation for one or more SensTermWindX arguments. Check inputs and documentation.')
    stop()
  }
  base::list(sens=argSplt[1],
             term=argSplt[2:base::length(argSplt)])
})
sensWind <- base::unlist(base::lapply(SensTermWind,FUN=function(idx){idx$sens})) # Pull out the terms to test
names(SensTermWind) <- sensWind
for (idxWind in base::seq_len(base::length(SensTermWind))){
  log$debug(base::paste0('Priority ', idxWind, ' sensor, term(s) for wind speed: ',base::paste0(SensTermWind[[idxWind]],collapse=', ')))
}
log$debug(base::paste0('Min wind speed: ', Para$VeloWindMin, ' m s-1'))

SensTermHeat <- base::list(sens=Para$SensTermHeat[1],term=Para$SensTermHeat[2])
if(base::sum(base::is.na(base::unlist(SensTermHeat)))>0){
  log$fatal('Invalid parameter formulation for SensTermHeat. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for heater state: ',base::paste0(SensTermHeat,collapse=', ')))




















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
  
  #Default Values for applying corrections
  applyTempCorr <- TRUE
  applyAbsCorr <- TRUE
  
  # Create the base output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  # Need the source ID for the sensor to write out data
  cfgLoc <- base::dir(base::paste0(idxDirIn,"/exofdom"))
  if(length(cfgLoc) != 1){
    log$fatal('More than one CFGLOC for exofdom discovered.')
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
  idxDirOutData <- base::paste0(idxDirOut, '/exofdom/', cfgLoc,'/stats')
  idxDirOutFlags <- base::paste0(idxDirOut, '/exofdom/', cfgLoc,'/flags')
  
  ##### Read in fDOM data #####
  fdomDataGlob <- base::file.path(idxDirIn,"exofdom","*","data","*")
  fdomDataPath <- base::Sys.glob(fdomDataGlob)
  if(base::length(fdomDataPath)==1){
    fdomData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(fdomDataPath),log = log), silent = FALSE)
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
  #fdomData$fDOMAbsQF <- -1 #This gets calculated by def.wq.abs.corr.R, if possible
  
  #Default expanded uncertainty to NA and spectrumCount to 0 until they can be calculated
  fdomData$fDOMExpUncert <- NA
  #fdomData$spectrumCount <- 0 #This gets calculated by def.wq.abs.corr.R, if possible
  
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
  
  ##### Read in fDOM cal and uncertainty information #####
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
      log$error("Error: No fDOM cal file(s) found that match selected dates.")
      applyAbsCorr <- FALSE
      applyTempCorr <- FALSE
      fdomData$spectrumCount <- 0
      fdomData$fDOMAbsQF <- -1
    }

  }else{
    log$error(base::paste0("Zero fDOM cal files found for: ",fdomDataGlob))
    applyAbsCorr <- FALSE
    applyTempCorr <- FALSE
    fdomData$spectrumCount <- 0
    fdomData$fDOMAbsQF <- -1
  }
  
  ##### Temperature Corrections #####
  # Try to pull prt data if we're at a stream site? Holding on this.
  # Look for and read in exoconductivity data
  exoconductivityDataGlob <- base::file.path(idxDirIn,"exoconductivity","*","data","*")
  exoconductivityDataPath <- base::Sys.glob(exoconductivityDataGlob)
  
  if(base::length(exoconductivityDataPath)==1){
    log$debug(base::paste0("One file found, reading in: ",exoconductivityDataPath))
    exoconductivityData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(exoconductivityDataPath),log = log), silent = FALSE)
  }else{
    log$debug(base::paste0("Zero or more than one file found for: ",exoconductivityDataGlob))
    applyTempCorr <- FALSE
    fdomData$fDOMTempQF <- 1
  }
  
  if(applyTempCorr && base::class(exoconductivityData) == 'try-error') {
    # Generate error and stop execution
    #log$error(base::paste0('File ', idxDirData, '/', fdomData, ' is unreadable.'))
    log$warn(base::paste0('File: ', exoconductivityData, ' is unreadable, temp corrections will not be applied.'))
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
    
    exoconductivityUcrtData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(exoconductivityUcrtPath),log = log), silent = FALSE)
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
    fdomData$spectrumCount <- 0
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
    absData <- try(NEONprocIS.wq::def.wq.abs.corr(sunav2Filenames = sunav2DataPath, 
                                                  sunav2CalFilenames = sunav2CalDataPath, 
                                                  log = log),silent = FALSE)
    
    # Handle the error when absData is a try-error
    if (base::class(absData) == 'try-error') {
      log$debug(base::paste0('Could not perform absorptance correction.'))
      fdomData$fDOMAbsQF <- 1
      fdomData$spectrumCount <- 0
    }else{
      # Merge in abs data to the fdomData dataframe and calculate correction factors and flags
      for(idxAbsData in absData$readout_time){
        if(idxAbsData <= base::max(fdomData$readout_time)){
          nextReadout <- base::min(absData$readout_time[absData$readout_time > idxAbsData])
          fdomData$Abs_ex[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$Abs_ex[absData$readout_time == idxAbsData]
          fdomData$Abs_em[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$Abs_em[absData$readout_time == idxAbsData]
          fdomData$ucrt_A_ex[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$ucrt_A_ex[absData$readout_time == idxAbsData]
          fdomData$ucrt_A_em[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$ucrt_A_em[absData$readout_time == idxAbsData]
          fdomData$spectrumCount[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$spectrumCount[absData$readout_time == idxAbsData]
          fdomData$fDOMAbsQF[fdomData$readout_time >= idxAbsData & fdomData$readout_time < nextReadout] <- absData$fDOMAbsQF[absData$readout_time == idxAbsData]
        }else{
          break
        }
        
      }
      
      #Determine fdom absorbance correction factor
      fdomData$absFactor <- 10^((fdomData$Abs_ex + fdomData$Abs_em) * fdomData$pathlength)
    }
  }
  
  ##### Uncertainty Calculations #####
  #Uncertainty when no corrections can be applied
  noCorrIdx <- base::which(fdomData$fDOMTempQF != 0 & (fdomData$fDOMAbsQF == 1|fdomData$fDOMAbsQF == 3|fdomData$fDOMAbsQF == -1))
  fdomData$fDOMExpUncert[noCorrIdx] <- 2 * fdomData$uncrt_A1_fdom[noCorrIdx] * fdomData$fDOM[noCorrIdx]
  
  #Uncertainty when temperature corrections, only, are applied
  tempOnlyIdx <- base::which(fdomData$fDOMTempQF == 0 & (fdomData$fDOMAbsQF == 1|fdomData$fDOMAbsQF == 3|fdomData$fDOMAbsQF == -1))
  #Calculate the fdom output with the factors that exist
  fdomData$fDOM[tempOnlyIdx] <- fdomData$fDOM[tempOnlyIdx] * fdomData$tempFactor[tempOnlyIdx]
  fdomData$fDOMExpUncert[tempOnlyIdx] <- 2 * sqrt((fdomData$uncrt_A1_fdom[tempOnlyIdx] * 1/(1+fdomData$rho_fdom[tempOnlyIdx]*(fdomData$surfaceWaterTemperature[tempOnlyIdx]-20)) * fdomData$rawCalibratedfDOM[tempOnlyIdx])^2 + 
                                                (fdomData$surfaceWaterTemperature_ucrtComb[tempOnlyIdx] * fdomData$rawCalibratedfDOM[tempOnlyIdx]*(fdomData$surfaceWaterTemperature[tempOnlyIdx]-20)/((1+fdomData$rho_fdom[tempOnlyIdx]*(fdomData$surfaceWaterTemperature[tempOnlyIdx]-20))^2))^2 + 
                                                (fdomData$uncrt_rho_fdom[tempOnlyIdx] * fdomData$rawCalibratedfDOM[tempOnlyIdx]*fdomData$rho_fdom[tempOnlyIdx]/((1+fdomData$rho_fdom[tempOnlyIdx]*(fdomData$surfaceWaterTemperature[tempOnlyIdx]-20))^2))^2)
  
  #Uncertainty when absorbance corrections, only, are applied
  absOnlyIdx <- base::which(fdomData$fDOMTempQF != 0 & (fdomData$fDOMAbsQF == 0|fdomData$fDOMAbsQF == 2))
  #Calculate the fdom output with the factors that exist
  fdomData$fDOM[absOnlyIdx] <- fdomData$fDOM[absOnlyIdx] * fdomData$absFactor[absOnlyIdx]
  fdomData$fDOMExpUncert[absOnlyIdx] <- 2 * sqrt((fdomData$uncrt_A1_fdom[absOnlyIdx] * 10^(fdomData$pathlength[absOnlyIdx]*(fdomData$Abs_ex[absOnlyIdx]+fdomData$Abs_em[absOnlyIdx])) * fdomData$rawCalibratedfDOM[absOnlyIdx])^2 +
                                               (fdomData$ucrt_A_ex[absOnlyIdx] * fdomData$pathlength[absOnlyIdx] * fdomData$rawCalibratedfDOM[absOnlyIdx] * 10^(fdomData$pathlength[absOnlyIdx]*(fdomData$Abs_ex[absOnlyIdx] + fdomData$Abs_em[absOnlyIdx])))^2 + 
                                               (fdomData$ucrt_A_em[absOnlyIdx] * fdomData$pathlength[absOnlyIdx] * fdomData$rawCalibratedfDOM[absOnlyIdx] * 10^(fdomData$pathlength[absOnlyIdx]*(fdomData$Abs_ex[absOnlyIdx] + fdomData$Abs_em[absOnlyIdx])))^2 +
                                               (fdomData$ucrt_pathlength[absOnlyIdx] * fdomData$rawCalibratedfDOM[absOnlyIdx] * (fdomData$Abs_ex[absOnlyIdx] + fdomData$Abs_em[absOnlyIdx]) * 10 ^ (fdomData$pathlength[absOnlyIdx] * (fdomData$Abs_ex[absOnlyIdx] + fdomData$Abs_em[absOnlyIdx])))^2)
  
  #Uncertainty when both temperature and absorbance corrections are applied
  tempAndAbsIdx <- base::which(fdomData$fDOMTempQF == 0 & (fdomData$fDOMAbsQF == 0|fdomData$fDOMAbsQF == 2))
  fdomData$rawCalibratedfDOM[tempAndAbsIdx] * fdomData$pathlength[tempAndAbsIdx] * base::log2(10) * (fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx])*10^(fdomData$pathlength[tempAndAbsIdx]*(fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx]))/(fdomData$rho_fdom[tempAndAbsIdx]*(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)-1)
  #Calculate the fdom output with the factors that exist
  fdomData$fDOM[tempAndAbsIdx] <- fdomData$fDOM[tempAndAbsIdx] * fdomData$tempFactor[tempAndAbsIdx] * fdomData$absFactor[tempAndAbsIdx]
  fdomData$fDOMExpUncert[tempAndAbsIdx] <- 2 * sqrt((fdomData$uncrt_A1_fdom[tempAndAbsIdx] * fdomData$rawCalibratedfDOM[tempAndAbsIdx] * 10^(fdomData$pathlength[tempAndAbsIdx]*(fdomData$Abs_ex[tempAndAbsIdx]+fdomData$Abs_em[tempAndAbsIdx]))/(1-fdomData$rho_fdom[tempAndAbsIdx]*(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)))^2 + 
                                                  (fdomData$surfaceWaterTemperature_ucrtComb[tempAndAbsIdx] * fdomData$rawCalibratedfDOM[tempAndAbsIdx] * fdomData$rho_fdom[tempAndAbsIdx] * 10^(fdomData$pathlength[tempAndAbsIdx] * (fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx]))/((fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)*fdomData$rho_fdom[tempAndAbsIdx] -1)^2)^2 + 
                                                  (fdomData$uncrt_rho_fdom[tempAndAbsIdx] * fdomData$rawCalibratedfDOM[tempAndAbsIdx]*(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)*10^(fdomData$pathlength[tempAndAbsIdx]*(fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx]))/(1-(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)*fdomData$rho_fdom[tempAndAbsIdx])^2)^2 + 
                                                  (fdomData$ucrt_A_ex[tempAndAbsIdx] * fdomData$rawCalibratedfDOM[tempAndAbsIdx] * fdomData$pathlength[tempAndAbsIdx] * base::log2(10)  * (fdomData$pathlength[tempAndAbsIdx]*(fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx]))/(fdomData$rho_fdom[tempAndAbsIdx]*(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)-1))^2 + 
                                                  (fdomData$ucrt_A_em[tempAndAbsIdx] * fdomData$rawCalibratedfDOM[tempAndAbsIdx] * fdomData$pathlength[tempAndAbsIdx] * base::log2(10)  * (fdomData$pathlength[tempAndAbsIdx]*(fdomData$Abs_ex[tempAndAbsIdx] + fdomData$Abs_em[tempAndAbsIdx]))/(fdomData$rho_fdom[tempAndAbsIdx]*(fdomData$surfaceWaterTemperature[tempAndAbsIdx]-20)-1))^2 + 
                                                  (fdomData$ucrt_pathlength[tempAndAbsIdx] * fdomData$fDOM[tempAndAbsIdx])^2)
  
  ##### Writing out data and flag files #####
  #Create output directories
  NEONprocIS.base::def.dir.crea(
    DirBgn = '/',
    DirSub = c(idxDirOutData, idxDirOutFlags),
    log = log
  )
  
  #Write data and uncertainty (which get stats)
  #readout_time, fDOM, fDOMExpUncert, spectrumCount
  dataOutputs <- c("readout_time", "readout_time", "fDOM", "rawCalibratedfDOM", "fDOMExpUncert", "spectrumCount")
  dataOut <- fdomData[,dataOutputs]
  #Don't really need to change the names, but just to avoid confusion it doesn't seem like a bad idea
  names(dataOut) <- c("startDateTime", "endDateTime", "fDOM", "rawCalibratedfDOM", "fDOMExpUncert", "spectrumCount")
  
  #Turn necessary outputs to integer
  colInt <- "spectrumCount"
  dataOut[colInt] <- base::lapply(dataOut[colInt],base::as.integer) # Turn spectrumCount to integer
  
  rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                      NameFile = base::paste0(idxDirOutData,"/exofdom_",cfgLoc,"_",format(timeBgn,format = "%Y-%m-%d"),"_basicStats_100.parquet"),
                                      Schm = SchmDataOut),silent=FALSE)
  if(class(rptDataOut) == 'try-error'){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Basic stats written out.")
  }
  
  #Write the flags (which get metrics)
  #readout_time, fDOMTempQF, fDOMAbsQF
  flagOutputs <- c("readout_time", "fDOMTempQF", "fDOMAbsQF")
  flagsOut <- fdomData[,flagOutputs]
  
  #Turn necessary outputs to integer
  colInt <- c("fDOMTempQF", "fDOMAbsQF") 
  flagsOut[colInt] <- base::lapply(flagsOut[colInt],base::as.integer) # Turn flags to integer
  
  rptQfOut <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut, 
                                      NameFile = base::paste0(idxDirOutFlags,"/exofdom_",cfgLoc,"_",format(timeBgn,format = "%Y-%m-%d"),"_flagsCorrection.parquet"), 
                                      Schm = SchmQf),silent=FALSE)
  if(class(rptQfOut) == 'try-error'){
    log$error(base::paste0('Writing the output flags failed: ',attr(rptQfOut,"condition")))
    stop()
  } else {
    log$info("Flags written out.")
  }
}


