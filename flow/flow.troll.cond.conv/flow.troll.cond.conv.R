##############################################################################################
#' @title Workflow for Missing Temp Flag and Conductivity Conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#' 
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

#' @return Corrected conductivity data and associated flags for missing temperature data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' If no output schema is provided for the flags, the output column/variable names will be 
#' readout_time, qfFlow, qfHeat, in that order. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS 
#' MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
 Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/aquatroll200_flags_specific')
 log <- NEONprocIS.base::def.log.init(Lvl = "debug")
 arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","FileSchmData=~/R/NEON-IS-avro-schemas/dp0p/aquatroll200_calibrated.avsc","FileSchmQf=~/R/NEON-IS-avro-schemas/dp0p/flags_troll_specific_temp.avsc")
#' rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2020-08-01)
#     original creation
#   Cove Sturtevant (2020-09-22)
#     placed output flags in existing flags directory
#     symbolically linked any files already in the flags directory to the output
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

# Retrieve output schema for data
FileSchmDataOut <- Para$FileSchmData
log$debug(base::paste0('Output schema for data: ',base::paste0(FileSchmDataOut,collapse=',')))

# Retrieve output schema for flags; no need for new data schema
FileSchmQfOut <- Para$FileSchmQf
log$debug(base::paste0('Output schema for flags: ',base::paste0(FileSchmQfOut,collapse=',')))

# Read in the schemas 
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}
if(base::is.null(FileSchmQfOut) || FileSchmQfOut == 'NA'){
  SchmQfOut <- NULL
} else {
  SchmQfOut <- base::paste0(base::readLines(FileSchmQfOut),collapse='')
}

# Retrieve optional sensor subdirectories to copy over
nameDirSubCopy <- c('flags','uncertainty_coef','uncertainty_data')
DirFlagsCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))
DirUncertCoefCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[2]))
DirUncertDataCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[3]))


#what are the expected subdirectories of each input path
nameDirSub <- c('data')

log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

if(base::length(DirIn) < 1){
  log$fatal('No datums found to process.')
  stop()
}



# Process each datum
for (idxDirIn in DirIn){
  ##### Logging and initializing #####
  idxDirIn<-DirIn[20] #for testing
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirInFlags <- base::paste0(idxDirIn,'/flags')
  idxDirOutFlags <- base::paste0(idxDirOut,'/flags')
  base::dir.create(idxDirOutFlags,recursive=TRUE)
  
  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirUncertCoefCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/uncertainty_coef'),idxDirOut,log=log)
  } 
  if(base::length(DirUncertDataCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/uncertainty_data'),idxDirOut,log=log)
  } 

  # The flags folder is already populated from the calibration module. Let's copy over any existing files
  fileCopy <- base::list.files(idxDirInFlags,recursive=TRUE) # Files to copy over
  
  # Symbolically link each file
  for(idxFileCopy in fileCopy){
    cmdCopy <- base::paste0('ln -s ',base::paste0(idxDirInFlags,'/',idxFileCopy),' ',base::paste0(idxDirOutFlags,'/',idxFileCopy))
    rptCopy <- base::system(cmdCopy)
  }
  
  
  ##### Read in troll data #####
  trollData <- NULL
  dirTroll <- base::paste0(idxDirIn,'/data')
  dirLocTroll <- base::dir(dirTroll,full.names=TRUE)
  
  if(base::length(dirLocTroll)<1){
    log$debug(base::paste0('No troll sensor data file in ',dirTroll))
  } else if (base::length(dirLocTroll)>1){
    log$warn(base::paste0('More than one troll sensor data file in ',dirLocTroll, '. Using only the first.'))
  } else{
    trollData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirLocTroll),log = log), silent = FALSE)
    log$debug(base::paste0("One datum found, reading in: ",dirLocTroll))
  }
  
  #create missing temperature flag; default all flags to -1 then change them as the test can be performed
  trollData$missingTempQF <- -1
  trollData$missingTempQF[!is.na(trollData$temperature) && trollData$temperature!="NA" && trollData$temperature!="NaN"]<-0
  trollData$missingTempQF[is.na(trollData$temperature)|trollData$temperature=="NA"|trollData$temperature=="NaN"]<-1
  source_id<-trollData$source_id[1]
  
  #convert acutal conductivity to specific conductance
  trollData$specCond <- NA
  trollData$specCond <- trollData$conductivity/(1+0.0191*(trollData$temperature-25))
  trollData$specCond[trollData$missingTempQF>0]<-NA #If no temp stream, then do not output specific conductance. Could potentially report acutal conductivity in future. 
  
  #Create dataframe for output data
  dataOut <- trollData
  dataOut$actual_conductivity <- dataOut$conductivity #need to keep for later calculations
  dataOut$conductivity <- dataOut$specCond #replace actual conductivity with specific conductance
  dataCol <- c("source_id","site_id","readout_time","pressure","pressure_data_quality","temperature","temperature_data_quality","actual_conductivity","conductivity_data_quality","conductivity","internal_battery")
  dataOut <- dataOut[,dataCol]
  
  #Create dataframe for just flags
  QFCol <- c("readout_time", "missingTempQF")
  flagsOut <- trollData[,QFCol]
  
  #Turn necessary outputs to integer
  colInt <- c("missingTempQF") 
  flagsOut[colInt] <- base::lapply(flagsOut[colInt],base::as.integer) # Turn flags to integer
  
  
  #Write out data
  rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                 NameFile = base::paste0(idxDirOutData,"/aquatroll200_",source_id,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet"), 
                                                 Schm = SchmDataOut),silent=FALSE)
  if(class(rptDataOut) == 'try-error'){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
  
  #Write out flags
  rptQfOut <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut, 
                                                 NameFile = base::paste0(idxDirOutFlags,"/aquatroll200_",source_id,"_",format(timeBgn,format = "%Y-%m-%d"),"_flagsSpecificQc_Temp.parquet"), 
                                                 Schm = SchmQfOut),silent=FALSE)
  if(class(rptQfOut) == 'try-error'){
    log$error(base::paste0('Writing the output flags failed: ',attr(rptQfOut,"condition")))
    stop()
  } else {
    log$info("Flags written out.")
  }
}
