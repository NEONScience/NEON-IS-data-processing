##############################################################################################
#' @title Sensor-specific QA/QC for Single/Triple-Aspirated Air Temperature data products.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Sensor-specific QA/QC for Single/Triple-Aspirated Air Temperature data products. 
#' Evaluates heater status and adequate flow through the aspirated shield considering the turbine speed of 
#' the fan in the aspirated shield and the ambient wind speed. 
#' 
#' This script is run at the command line with 6+ arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are (at a minimum) folders with the same name as the the sensors indicated in 
#' input arguments SensTermTemp, SensTermTurb, SensTermHeat, and SensTermWindX. For example, if the 
#' temperature sensor is called 'prt', the heater called 'heater', two wind speed sensors, 'csat3' and 
#' 'windobserverii', are respectively indicated in SensTermWind1 and SensTermWind2, and the turbine is called 
#' 'dualfan', then the following subdirectories will be present in DirIn (or a child of DirIn):
#'         /prt
#'         /heater
#'         /dualfan
#'         /csat3
#'         /windobserverii
#'         
#' Within each of these folders must be a subdirectory for the location id of the sensor, and within that 
#' folder will be a subdirectory called 'data', in which a single data file resides. The only sensor folder
#' within which may exist multiple location ID folders or may exist multiple data files within a single 
#' location ID folder is the temperature sensor. In that case, each will be quality-checked against the 
#' single locations available for each the turbine, wind, and heater sensors. 
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "SensTermTemp=value", where the value is the sensor name and term representing the calibrated
#' temperature measurement. The sensor name and term are separated by a colon, without spaces 
#' (e.g. "SensTermTemp=prt:temp")
#' 
#' 4. "SensTermTbne=value", where the value is the sensor name and term representing the speed of the turbine
#' within the aspirated shield. The sensor name and term are separated by a colon, without spaces 
#' (e.g. "SensTermTbne=dualfan:turbineSpeed")
#' 
#' 5. "AvelTbneMin=value" (optional), where the value is the minimum turbine speed (RPM) at and above which 
#' indicates adequate aspiration of the shield. If not provided, the default value is 300 RPM.
#' 
#' 6. "SensTermWindX=value", where the value is the sensor name and term(s) representing the nearest wind 
#' speed measurement. The sensor name and terms are separated by colons (:), without spaces 
#' (e.g. "SensTermWind1=windobserverii:uVectorComponent:vVectorComponent"). X is an integer indicating the 
#' priority order of the wind measurement to be used. In priority order (1 being first priority), the sensor 
#' and associated term(s) will be checked for availability. If unavailable, the windspeed from the next 
#' available sensor-term set will be used. If only one term is provided, it will be taken as the net 
#' horizontal wind speed. If two or more terms are provided, the will be treated as orthogonal vector 
#' components of the windspeed, for which their vector sum will be computed. At least SensTermWind1 is 
#' required, and there is a maximum of SensTermWind100. 
#' 
#' 7. "VeloWindMin=value" (optional), where the value is the minimum wind speed (m s-1) at and above which 
#' the aspiration in the shield is considered adequate even if the turbine speed is below the minimum. If 
#' not provided, the default value is 12 m s-1.
#' 
#' 8. "SensTermHeat=value", where the value is the sensor name and term representing the status of the heater
#' within the aspirated shield. The sensor name and term are separated by a colon, without spaces 
#' (e.g. "SensTermHeat=heater:heaterFlag"). 
#' 
#' 9. "RmvFlow=value" (optional), where the value is logical TRUE or FALSE (e.g. "RmvFlow=TRUE") indicating whether to 
#' remove (turn to NA) data during which there is inadequate flow throught the aspirated shield (failure of 
#' the flow test. Default is "RmvFlow=FALSE".
#' 
#' 10. "RmvHeat=value" (optional), where the value is logical TRUE or FALSE (e.g. "RmvHeat=TRUE") indicating whether to 
#' remove (turn to NA) data during which the heater is active. Default is "RmvHeat=TRUE".
#' 
#' 11.  "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN 
#' ORDER OF THE INPUT DATA.
#'  
#' 12. "FileSchmQf=value" (optional), where value is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE FOLLOWING ORDER OF THE OUTPUT 
#' DATA FRAME GENERATED BY THIS CODE. See output information below for details. 
#' 
#' 13. "DirSubCopySens=value" (optional), where value is the names of sensor subfolders, separated by 
#' pipes that are to be copied with a symbolic link to the output path. By default, only the temperature 
#' sensor subfolder indicated in the SensTermTemp argument is retained.
#' 
#' 14. "DirSubCopyTemp=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the data folder in the temperature sensor subfolder that are to be copied 
#' with a symbolic link to the output path. By default, only the data subfolder is retained.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. Note that the 
#' 
#' If no output schema is provided for the flags, the output column/variable names will be 
#' readout_time, qfFlow, qfHeat, in that order. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS 
#' MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.
#' 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000646 Revision E: ALGORITHM THEORETICAL BASIS DOCUMENT (ATBD) SINGLE ASPIRATED AIR TEMPERATURE
#' NEON.DOC.000302 Revision B: NEON SENSOR COMMAND, CONTROL, AND CONFIGURATION: SINGLE ASPIRATED AIR TEMPERATURE


#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.temp.air.aspi.R "DirIn=/scratch/pfs/tempAirSingle_qaqc_specific_group_test" "DirOut=/scratch/pfs/out" "SensTermTemp=prt:temp","SensTermTbne=dualfan:turbineSpeed" "AvelTbneMin=300" "SensTermWind1=windobserverii:uVectorComponent:vVectorComponent" "VeloWindMin=12" "SensTermHeat=heater:state"

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-12-18)
#     initial creation
#   Cove Sturtevant (2020-04-28)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

TimeCool <- base::as.difftime(224,units='secs') # Cooling time after heater turns off. See NEON.DOC.000646 & NEON.DOC.000302
  
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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","SensTermTemp","SensTermHeat",
                                                             "SensTermTbne","SensTermWind1"),
                                      NameParaOptn=c("AvelTbneMin","VeloWindMin","FileSchmQf","FileSchmQf",
                                                     "DirSubCopySens","DirSubCopyTemp",
                                                     base::paste0("SensTermWind",2:100),
                                                     "RmvFlow","RmvHeat"),
                                      ValuParaOptn=base::list(AvelTbneMin=300,VeloWindMin=12,RmvFlow=FALSE,
                                                              RmvHeat=TRUE),
                                      TypePara = base::list(AvelTbneMin='numeric',VeloWindMin='numeric',
                                                            RmvFlow='logical',RmvHeat='logical'),
                                      log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve the sensor and term for temperature
SensTermTemp <- base::list(sens=Para$SensTermTemp[1],term=Para$SensTermTemp[2])
if(base::sum(base::is.na(base::unlist(SensTermTemp)))>0){
  log$fatal('Invalid parameter formulation for SensTermTemp. Check inputs and documentation.')
  stop()
}
log$debug(base::paste0('Sensor, term for temperature: ',base::paste0(SensTermTemp,collapse=', ')))

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

# Echo the data removal options
log$debug(base::paste0('Data removal for failure of flow test set to : ', Para$RmvFlow))
log$debug(base::paste0('Data removal for heater active periods set to : ', Para$RmvHeat))

# Retrieve output schema for data
FileSchmDataOut <- Para$FileSchmData
log$debug(base::paste0('Output schema for data: ',base::paste0(FileSchmDataOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NA
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}

# Retrieve output schema for flags
FileSchmQfOut <- Para$FileSchmQf
log$debug(base::paste0('Output schema for flags: ',base::paste0(FileSchmQfOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmQfOut) || FileSchmQfOut == 'NA'){
  SchmQfOut <- NULL
} else {
  SchmQfOut <- base::paste0(base::readLines(FileSchmQfOut),collapse='')
}

# Retrieve optional sensor subdirectories to copy over
DirSubCopySens <- base::unique(base::setdiff(Para$DirSubCopySens,SensTermTemp$sens))
log$debug(base::paste0('Additional sensor subdirectories to copy: ',base::paste0(DirSubCopySens,collapse=',')))

DirSubCopyTemp <- base::unique(base::setdiff(Para$DirSubCopyTemp,'data'))
log$debug(base::paste0('Additional temperature sensor subdirectories to copy: ',base::paste0(DirSubCopyTemp,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSubSens <- base::as.list(SensTermTemp$sens)
log$debug(base::paste0('Minimum expected sensor subdirectories of each datum path: ',nameDirSubSens))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSubSens,log=log)

# Create the binning for each 30-second interval evaluated for adequate aspiration
timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=base::as.difftime(30,units="secs"),WndwTime=base::as.difftime(1,units='days'))

# Create dummy vectors for evaluating 30-second average flow rates for each day
flagFailSec30 <- base::rep(1,2880) # initialize flag to 1 (fail)
naSec30 <- NA*flagFailSec30

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  base::dir.create(idxDirOut,recursive=TRUE)

  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirSubCopySens) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopySens),idxDirOut,log=log)
  }  
  
  # Flesh out sensor directories (except for wind, that's below)
  dirTemp <- base::paste0(idxDirIn,'/',SensTermTemp$sens)
  dirTbne <- base::paste0(idxDirIn,'/',SensTermTbne$sens)
  dirHeat <- base::paste0(idxDirIn,'/',SensTermHeat$sens)
  
  
  # Load in turbine data
  dataTbne <- NULL
  dirLocTbne <- base::dir(dirTbne,full.names=TRUE)
  if(base::length(dirLocTbne) < 1){
    log$debug(base::paste0('No turbine sensor directory: ',dirTbne))
  } else {
    # Choose the first sensor location
    dirDataTbne <- base::paste0(dirLocTbne[1],'/data')
    fileTbne <- base::dir(dirDataTbne,full.names=TRUE)
    
    # load the first data file
    numFileTbne <- base::length(fileTbne)
    if(numFileTbne < 1){
      log$debug(base::paste0('No turbine sensor data file in ',dirDataTbne))
    } else if (numFileTbne > 1){
      log$warn(base::paste0('More than one turbine sensor data file in ',dirDataTbne, '. Using only the first.'))
    } else {
      dataTbne  <- base::try(NEONprocIS.base::def.read.parq(NameFile=fileTbne[1],log=log),silent=FALSE)
      if(base::class(data) == 'try-error'){
        log$error(base::paste0('File ', fileTbne[1],' is unreadable.')) 
        stop()
      }

      # Check that the turbine speed variable is present
      if(base::sum(!c('readout_time',SensTermTbne$term) %in% base::names(dataTbne)) > 0){
        log$warn(base::paste0('Variables "readout_time" and "',SensTermTbne$term,'" are required in turbine data, but at least one cannot be found in file: ',fileTbne[1])) 
        dataTbne <- NULL
      } else {
        log$debug(base::paste0('Turbine data found for sensor: ',SensTermTbne$sens))
      }
    }
  }
  
  # Load in wind speed data, by indicated priority
  dataWind <- NULL
  for(idxSensWind in sensWind){
    dirWind <- base::paste0(idxDirIn,'/',idxSensWind)
    
    dirLocWind <- base::dir(dirWind,full.names=TRUE)
    if(base::length(dirLocWind) < 1){
      log$debug(base::paste0('No wind sensor directory: ',dirWind,'. Will try for less preferable wind sensor (if indicated).'))
    } else {
      # Choose the first sensor location
      dirDataWind <- base::paste0(dirLocWind[1],'/data')
      fileWind <- base::dir(dirDataWind,full.names=TRUE)
      
      # load the first data file
      numFileWind <- base::length(fileWind)
      if(numFileWind < 1){
        log$debug(base::paste0('No wind sensor data file in ',dirDataWind))
      } else if (numFileWind > 1){
        log$warn(base::paste0('More than one wind sensor data file in ',dirDataWind, '. Using only the first.'))
      } else {
        dataWind  <- base::try(NEONprocIS.base::def.read.parq(NameFile=fileWind[1],log=log),silent=FALSE)
        if(base::class(data) == 'try-error'){
          log$error(base::paste0('File ', fileWind[1],' is unreadable.')) 
          stop()
        }
        
        # Check that the wind speed variables are present
        if(base::sum(!c('readout_time',SensTermWind[[idxSensWind]]$term) %in% base::names(dataWind)) > 0){
          log$warn(base::paste0('Variables "readout_time" and "',base::paste0(SensTermWind[[idxSensWind]]$term,collapse=','),'" are required in wind data, but at least one cannot be found in file: ',fileWind[1])) 
          dataWind <- NULL
        } else {
          # We have a workable data file. Don't look for any lower priority wind sensors
          log$debug(base::paste0('Wind data found for sensor: ',idxSensWind))
          break
        }
      }
    }
  }
  
  # Create 30-second time breaks for this day, including the end time
  timeBrk <- timeBgn + c(timeBinDiff$timeBgnDiff,tail(timeBinDiff$timeEndDiff,1))
  dataFlow <- base::data.frame(avelTbne=naSec30,veloWind=naSec30)
  
  # Take a 30-second bin average of turbine speed
  binTbne <- .bincode(x=dataTbne$readout_time,breaks=timeBrk,right=FALSE)
  setBin <- base::sort(base::unique(binTbne))
  dataFlow$avelTbne[setBin] <- base::unlist(base::lapply(setBin,FUN=function(idxBin){
    base::mean(dataTbne[[SensTermTbne$term]][binTbne==idxBin],na.rm=TRUE)}))
  
  # Take a 30-second bin average of wind speed - this is slightly different from ATBD, but much more robust
  dataFlow$veloWind <- NA
  if(!base::is.null(dataWind)){
    veloVectWind <- base::subset(dataWind,select=SensTermWind[[idxSensWind]]$term)
    veloWind <- base::sqrt(base::rowSums(x=veloVectWind^2,na.rm=TRUE)) # Vector sum of wind speed components
    binWind <- .bincode(x=dataWind$readout_time,breaks=timeBrk,right=FALSE)
    setBin <- base::sort(base::unique(binWind))
    dataFlow$veloWind[setBin] <- base::unlist(base::lapply(setBin,FUN=function(idxBin){
      base::mean(veloWind[binWind==idxBin],na.rm=TRUE)}))
  }
 
  
  # Create 30-second flow rate flags
  qfFlowSec30 <- flagFailSec30
  qfFlowSec30[dataFlow$avelTbne >= Para$AvelTbneMin] <- 0 # Pass flow test if turbine speed at/above minimum 
  qfFlowSec30[dataFlow$veloWind >= Para$VeloWindMin] <- 0 # Pass flow test if wind speed at/above minimum 
  qfFlowSec30[base::is.na(dataFlow$veloWind) & base::is.na(dataFlow$avelTbne)] <- -1 # Test indeterminate if no wind speed or turbine data 
  qfFlowSec30[base::is.na(dataFlow$veloWind) & dataFlow$avelTbne < Para$AvelTbneMin] <- -1 # Test indeterminate if turbine speed below min but no wind speed 
  qfFlowSec30[base::is.na(dataFlow$avelTbne) & dataFlow$veloWind < Para$VeloWindMin] <- -1 # Test indeterminate if wind speed below min but no turbine 
  
  
  # Load in heater data
  dataHeat <- NULL
  dirLocHeat <- base::dir(dirHeat,full.names=TRUE)
  if(base::length(dirLocHeat) < 1){
    log$debug(base::paste0('No heater directory: ',dirHeat))
  } else {
    # Choose the first sensor location
    dirDataHeat <- base::paste0(dirLocHeat[1],'/data')
    fileHeat <- base::dir(dirDataHeat,full.names=TRUE)
    
    # load the first data file
    numFileHeat <- base::length(fileHeat)
    if(numFileHeat < 1){
      log$debug(base::paste0('No heater data file in ',dirDataHeat))
    } else if (numFileHeat > 1){
      log$warn(base::paste0('More than one heater data file in ',dirDataHeat, '. Using only the first.'))
    } else {
      
      # Read heater events into data frame
      dataHeat <- base::try(NEONprocIS.base::def.read.evnt.json(NameFile=fileHeat[1]),silent=FALSE)
      if(base::class(data) == 'try-error'){
        log$error(base::paste0('File ', fileHeat[1],' is unreadable.')) 
        stop()
      }

      # Check that the heater variables are present
      if(base::sum(!c('timestamp',SensTermHeat$term) %in% base::names(dataHeat)) > 0){
        log$warn(base::paste0('Variables "timestamp" and "',SensTermHeat$term,'" are required in heater events, but at least one cannot be found in file: ',fileHeat[1])) 
        dataHeat <- NULL
      } else {
        log$debug(base::paste0('Heater status found for sensor: ',SensTermHeat$sens))
      }
    }
  }
  
  # Create start and end times for heater ON periods
  if(!base::is.null(dataHeat)){
    timeHeatEvnt <- NEONprocIS.qaqc::def.time.heat.on(dataHeat=dataHeat,TimeOffAuto=base::as.difftime(10,units="secs"))
  }
  
  
  
  # Create flags and write output for each temp sensor location directory
  dirLocTemp <- base::dir(dirTemp,full.names=TRUE)
  for(idxDirLocTemp in dirLocTemp){
    
    # Gather info about the input directory (including date) and create the output directories. 
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirLocTemp)
    idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
    idxDirOutData <- base::paste0(idxDirOut,'/data')
    base::dir.create(idxDirOutData,recursive=TRUE)
    idxDirOutQf <- base::paste0(idxDirOut,'/flags')
    base::dir.create(idxDirOutQf,recursive=TRUE)
    
    # Copy with a symbolic link the desired subfolders 
    if(base::length(DirSubCopyTemp) > 0){
      NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirLocTemp,'/',DirSubCopyTemp),idxDirOut,log=log)
    }  
    
    
    # Where's the temperature data?
    idxDirDataTemp <- base::paste0(idxDirLocTemp,'/data')
    fileDataTemp <- base::dir(idxDirDataTemp)    
   
    # Read in each data file and create/apply flags
    for (idxFileDataTemp in fileDataTemp){
      
      # Load in data
      dataTemp  <- base::try(NEONprocIS.base::def.read.parq(NameFile=base::paste0(idxDirDataTemp,'/',idxFileDataTemp),log=log),silent=FALSE)
      if(base::class(dataTemp) == 'try-error'){
        log$error(base::paste0('File ', base::paste0(idxDirDataTemp,'/',idxFileDataTemp),' is unreadable.')) 
        stop()
      }
      
      # Check that the temperature variables are present
      if(base::sum(!c('readout_time',SensTermTemp$term) %in% base::names(dataTemp)) > 0){
        log$error(base::paste0('Variables "readout_time" and "',base::paste0(SensTermTemp$term,collapse=','),'" are required in temperature data, but at least one cannot be found in file: ',base::paste0(idxDirDataTemp,'/',idxFileDataTemp))) 
        stop()
      }
      
      # Initialize quality flag output
      qf <- base::subset(dataTemp,select='readout_time')
      qf$qfFlow <- -1
      qf$qfHeat <- -1
      
      # Apply each 30-second flow rate flag to corresponding temperature readout times
      binTemp <- .bincode(x=dataTemp$readout_time,breaks=timeBrk,right=FALSE)
      setBin <- base::unique(binTemp)
      qfFlow <- qf$qfFlow
      for(idxBin in setBin){
        qfFlow[binTemp==idxBin] <- qfFlowSec30[idxBin]
      }
      qf$qfFlow <- qfFlow
  
      # Apply the heater flag
      if(!base::is.null(dataHeat)){
        qfHeat <- 0*qf$qfHeat 

        # Run through each period the heater was on
        for(idxHeatEvnt in base::seq_len(base::nrow(timeHeatEvnt))){
          if(base::is.na(timeHeatEvnt$timeOn[idxHeatEvnt])){
            # No heater-on time resolved. Do nothing
            next
            # Assume the heater was on until the off time
            # idxHeatOn <- 1
            # idxHeatOff <- utils::tail(base::which(dataTemp$readout_time < (timeHeatEvnt$timeOff[idxHeatEvnt]+TimeCool)),n=1)
            # No heater-on time recorded
          } else if (base::is.na(timeHeatEvnt$timeOff[idxHeatEvnt])){
            # No heater-off time resolved, assume on until end of data file (this should not occur if there is an auto-timeout applied above)
            idxHeatOn <- utils::head(base::which(dataTemp$readout_time >= timeHeatEvnt$timeOn[idxHeatEvnt]),n=1)
            idxHeatOff <- base::nrow(dataTemp)
          } else {
            # On-off times resolved
            idxHeatOn <- utils::head(base::which(dataTemp$readout_time >= timeHeatEvnt$timeOn[idxHeatEvnt]),n=1)
            idxHeatOff <- utils::tail(base::which(dataTemp$readout_time < (timeHeatEvnt$timeOff[idxHeatEvnt]+TimeCool)),n=1)
          }
          
          # Apply the flag
          if(base::length(idxHeatOn) > 0 && base::length(idxHeatOff) > 0 && idxHeatOff >= idxHeatOn)
          qfHeat[idxHeatOn:idxHeatOff] <- 1
        }
        
        qf$qfHeat <- qfHeat
      }
      
      # Are we doing data removal? 
      if(Para$RmvFlow == TRUE){
        dataTemp[[SensTermTemp$term]][qf$qfFlow==1] <- NA
      }
      if(Para$RmvHeat == TRUE){
        dataTemp[[SensTermTemp$term]][qf$qfHeat==1] <- NA
      }
      
      
      # Use as.integer in order to write out as integer with the avro schema
      qf[,2:base::ncol(qf)] <- base::apply(X=base::subset(x=qf,select=2:base::ncol(qf)),MARGIN=2,FUN=base::as.integer)
      
      
      # If no schema was provided for the data, use the same schema as the input data
      if(base::is.na(SchmDataOut)){
        
        # Use the same schema as the input data to write the output data. 
        idxSchmDataOut <- base::attr(dataTemp,'schema')
        
      } else {
        idxSchmDataOut <- SchmDataOut
      }
      

      # Construct file names
      NameFileOutData <- NEONprocIS.base::def.file.name.out(nameFileIn = idxFileDataTemp, prfx=base::paste0(idxDirOutData,'/'), sufx = '_specificQc')
      NameFileOutQf <- NEONprocIS.base::def.file.name.out(nameFileIn = idxFileDataTemp, prfx=base::paste0(idxDirOutQf,'/'), sufx = '_flagsSpecificQc')
      
      
      # Write the data
      rptData <- base::try(NEONprocIS.base::def.wrte.parq(data=dataTemp,NameFile=NameFileOutData,NameFileSchm=NULL,Schm=idxSchmDataOut),silent=TRUE)
      if(base::class(rptData) == 'try-error'){
        log$error(base::paste0('Cannot write Quality controlled data in file ', NameFileOutData,'. ',attr(rptData,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Quality controlled data written successfully in ',NameFileOutData))
      }
      
      # Write out the flags 
      rptQf <- base::try(NEONprocIS.base::def.wrte.parq(data=qf,NameFile=NameFileOutQf,NameFileSchm=NULL,Schm=SchmQfOut),silent=TRUE)
      if(base::class(rptQf) == 'try-error'){
        log$error(base::paste0('Cannot write sensor-specific QC flags  in file ', NameFileOutQf,'. ',attr(rptQf,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Sensor-specific QC flags written successfully in ',NameFileOutQf))
      }

      
    } # End loop around data files
    
  } # End loop around temp sensor location directories

  return()
} # End loop around datum paths
