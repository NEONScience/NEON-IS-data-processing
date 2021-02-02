##############################################################################################
#' @title Workflow for Level Troll 500 and Aqua Troll 200 Science Computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Computes temperature corrected density and water table elevation for surface and groundwater elevation data products.
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
#'         /sensor/yyyy/mm/dd/namedLocation/data
#'         /sensor/yyyy/mm/dd/namedLocation/location
#'         /sensor/yyyy/mm/dd/namedLocation/uncertainty_coef
#'         /sensor/yyyy/mm/dd/namedLocation/uncertainty_data
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
#' 4. Terms? 
#'  
#'    
#' 5. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the flags folder in the input path that are to be copied with a 
#' symbolic link to the output path. 
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
Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/leveltroll500_qaqc_data_group')
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out")
rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2020-12-18)
#     original creation
##############################################################################################

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut"),NameParaOptn = c("FileSchmData"),log = log)

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

# Read in the schemas
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}



# Retrieve optional sensor subdirectories to copy over
nameDirSubCopy <- c('uncertainty_coef','uncertainty_data')
DirUncertCoefCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))
DirUncertDataCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[2]))

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
  idxDirIn<-DirIn[6]
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,'/pre_statistics',InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirOutLocation <- base::paste0(idxDirOut,'/location')
  base::dir.create(idxDirOutLocation,recursive=TRUE)
  
  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirUncertCoefCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/uncertainty_coef'),idxDirOut,log=log)
  } 
  if(base::length(DirUncertDataCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/uncertainty_data'),idxDirOut,log=log)
  } 
  
  
  ##### Read in troll data #####
  trollData <- NULL
  dirTroll <- base::paste0(idxDirIn,'/data')
  dirLocTroll <- base::dir(dirTroll,full.names=TRUE)
  
  if(base::length(dirLocTroll)<1){
    log$debug(base::paste0('No troll sensor data file in ',dirDataTbne))
  } else if (base::length(dirLocTroll)>1){
    log$warn(base::paste0('More than one troll sensor data file in ',dirLocTroll, '. Using only the first.'))
  } else{
    trollData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirLocTroll),log = log), silent = FALSE)
    log$debug(base::paste0("One datum found, reading in: ",dirLocTroll))
  }
  
  ##### Read in location data #####
  LocationData <- NULL
  dirLocation <- base::paste0(idxDirIn,'/location')
  dirLocLocation <- base::dir(dirLocation,full.names=TRUE)
  
  if(base::length(dirLocLocation)<1){
    log$debug(base::paste0('No troll sensor data file in ',dirDataTbne))
  } else if (base::length(dirLocLocation)>1){
    log$warn(base::paste0('More than one troll sensor data file in ',dirLocLocation, '. Using only the first.'))
    # Choose the second location file
    LocationData <- base::paste0(dirLocLocation[2])
  } else{
    LocationData <- base::paste0(dirLocLocation)
    log$debug(base::paste0("One datum found, reading in: ",dirLocLocation))
  }
  
  
  
  
  
  ##############################
  #need to figure out how to pull elevation and z-offsets from json...
  
  
  
  
  
  
  ###### Compute water table elevation. Function of calibrated pressure, gravity, and density of water
  density <- 999   #future mod: temperature corrected density; conductivity correct density
  gravity <- 9.81  #future mod: site specific gravity
  
  #incorporate locaiton data
  trollData$elevation  #need to figure out how to extract this from json
  trollData$zOffset    #need to figure out how to extract this from json
  
  #calculate water table elevation
  trollData$elev_H2O<-NA
  trollData$elev_H2O<-trollData$elevation+trollData$zOffset+(1000*trollData$data/(density*gravity))
  
  
  #Create dataframe for output data
  dataOut <- trollData
  
  #include conductivity based on sensor type
  if(grepl("aquatroll",idxDirIn)){
    dataCol <- c("readout_time","pressure","pressure_data_quality","temperature","temperature_data_quality","conductivity","conductivity_data_quality","elev_H2O")
    sensor<-"aquatroll200"
  }else{
    dataCol <- c("readout_time","pressure","pressure_data_quality","temperature","temperature_data_quality","elev_H2O") 
    sensor<-"leveltroll500"
  }
  dataOut <- dataOut[,dataCol]
  
  #Write out data
  fileOutSplt <- base::strsplit(idxDirIn,'[/]')[[1]] # Separate underscore-delimited components of the file name
  CFGLOC<-tail(x=fileOutSplt,n=1)
  rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                   NameFile = base::paste0(idxDirOutData,"/",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet"), 
                                                   Schm = SchmDataOut),silent=FALSE)
  if(class(rptDataOut) == 'try-error'){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
  
  #need to write out location data as well
  
}
