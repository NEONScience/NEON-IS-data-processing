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
                                                             "SensTermTbne","SensTermWind1", "DirErr"),
                                      NameParaOptn=c("AvelTbneMin","VeloWindMin","FileSchmData","FileSchmQf",
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
  SchmDataOut <- NULL
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

# Retrieve optional sensor subdirectories to copy overs
DirSubCopySens <- base::unique(base::setdiff(Para$DirSubCopySens,SensTermTemp$sens))
log$debug(base::paste0('Additional sensor subdirectories to copy: ',base::paste0(DirSubCopySens,collapse=',')))

DirSubCopyTemp <- base::unique(base::setdiff(Para$DirSubCopyTemp,'data'))
log$debug(base::paste0('Additional temperature sensor subdirectories to copy: ',base::paste0(DirSubCopyTemp,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSubSens <- base::as.list(SensTermTemp$sens)
log$debug(base::paste0('Minimum expected sensor subdirectories of each datum path: ',nameDirSubSens))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSubSens,log=log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Run the wrapper function for each datum
  wrap.qaqc.temp.air.aspi(DirIn=idxDirIn,
                          DirOut=DirOut,
                          SensTermTemp=SensTermTemp,
                          SensTermTbne=SensTermTbne,
                          AvelTbneMin=Para$AvelTbneMin,
                          SensTermWind=SensTermWind,
                          sensWind=sensWind,
                          VeloWindMin=Para$VeloWindMin,
                          SensTermHeat=SensTermHeat,
                          DirSubCopyTemp=DirSubCopyTemp,
                          DirSubCopySens=DirSubCopySens,
                          log=log,
                          RmvFlow=Para$RmvFlow,
                          RmvHeat=Para$RmvHeat,
                          SchmDataOut=SchmDataOut,
                          SchmQfOut=SchmQfOut
  )
  
  return()
  
} # End loop around datum paths
