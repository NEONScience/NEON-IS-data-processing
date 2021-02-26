##############################################################################################
#' @title Workflow for Level Troll 500 and Aqua Troll 200 Science Computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Uncertainty module for surface and groundwater troll data products.
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
#' 4. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the flags folder in the input path that are to be copied with a 
#' symbolic link to the output path. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return water table elevation calculated from calibrated pressure, density of water, gravity, and sensor elevation.
#' Data and uncertainty values will be output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Any other folders specified in argument DirSubCopy will be copied over unmodified with a symbolic link. 
#'  
#' If no output schema is provided for the data, the output column/variable names will be determined by the 
#' sensor type (leveltroll500 or aquatroll200). Output column/variable names for the leveltroll500 will be
#' readout_time, pressure, pressure_data_quality, temperature, temperature_data quality, elev_H2O, in that order. 
#' Output column/variable names for the aquatroll200 will be readout_time, pressure, pressure_data_quality, 
#' temperature, temperature_data quality, conductivity, conductivity_data_quality, elev_H2O, in that order.
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/groundwaterPhysical_qaqc_data_group')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out")
#rm(list=setdiff(ls(),c('arg','log')))

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2021-02-02)
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
# Retrieve output schema; no need for new data schema

# Read in the schemas
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}


# Retrieve optional sensor subdirectories to copy over
nameDirSubCopy <- c('data','location','uncertainty_coef','uncertainty_data')
DirLocationCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))
DirUncertCoefCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[2]))
DirUncertDataCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[3]))


#what are the expected subdirectories of each input path
nameDirSub <- c('data','location','uncertainty_coef','uncertainty_data')
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
  #idxDirIn<-DirIn[20] #for testing
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,'/sci_uncertainty',InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)

  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirLocationCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/location'),idxDirOut,log=log)
  }
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
    log$debug(base::paste0('No troll sensor data file in ',dirTroll))
  } else{
    trollData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirLocTroll),log = log), silent = FALSE)
    log$debug(base::paste0("Reading in: ",dirLocTroll))
  }
  
  
  
  ##### Read in location data #####
  LocationData <- NULL
  dirLocation <- base::paste0(idxDirIn,'/location')
  dirLocLocation <- base::dir(dirLocation,full.names=TRUE)
  
  #####if no "_locations" file then check that the data file is all NA's. If there is data, STOP. If NA's, move on.
  if(base::length(dirLocLocation)<1){
    #case where there are no files
    log$debug(base::paste0('No troll location data files in ',dirLocation))
  } else if(any(grepl("locations",dirLocLocation))){
    # Choose the _locations.json file
    LocationData <- base::paste0(dirLocLocation[grep("locations",dirLocLocation)])
    log$debug(base::paste0("One datum found, reading in: ",LocationData))
    LocationHist <- NEONprocIS.base::def.loc.geo.hist(LocationData, log = NULL)
  } else { 
    #case where there is only a CFGLOC file
    if(length(unique(trollData$pressure))<=1 & is.na(unique(trollData$pressure)[1])){
      #check that the data file is indeed empty then move on to next datum
      log$debug(base::paste0('Troll data file is empty in',dirTroll, '. Moving on to next datum.'))
      LocationHist <-NULL
    }else{
      log$debug(base::paste0('Data exists in troll data file. Location file is missing for ',dirTroll))
      stop()
    }
  }
  
  
  ###### Compute water table elevation. Function of calibrated pressure, gravity, and density of water
  density <- 999  #m/s2 #future mod: temperature corrected density; conductivity correct density
  gravity <- 9.81  #kg/m3 #future mod: site specific gravity
  
  #incorporate location data
  fileOutSplt <- base::strsplit(idxDirIn,'[/]')[[1]] # Separate underscore-delimited components of the file name
  CFGLOC<-tail(x=fileOutSplt,n=1)
  elevation<- LocationHist$CFGLOC[[1]]$geometry$coordinates[3]
  z_offset<- LocationHist$CFGLOC[[1]]$z_offset
  survey_uncert<- LocationHist$CFGLOC[[1]]$`Survey vertical uncertainty` #includes survey uncertainty and hand measurements
  
  ######
  ###### NEED TO ADD CODE FOR WHEN THERE ARE MULTIPLE LOCATIONS IN A SINGLE DAY
  ######
  
  #calculate water table elevation
  trollData$elev_H2O<-NA
  if(length(LocationHist)>0){
    trollData$elev_H2O<-elevation+z_offset+(1000*trollData$pressure/(density*gravity))
  }
  

  ##### Read in uncertainty data #####
  uncertaintyData <- NULL
  dirUncertainty <- base::paste0(idxDirIn,'/uncertainty_data')
  dirUncertaintyLocation <- base::dir(dirUncertainty,full.names=TRUE)
  if(base::length(dirUncertaintyLocation)<1){
    log$debug(base::paste0('No troll uncertainty data file in ',dirUncertainty))
  } else{
    uncertaintyData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirUncertaintyLocation),log = log), silent = FALSE)
    log$debug(base::paste0("Reading in: ",dirUncertaintyLocation))
  }
  
  ##### Read in uncertainty coef #####
  uncertaintyCoef <- NULL
  dirUncertaintyCoef <- base::paste0(idxDirIn,'/uncertainty_coef')
  dirUncertaintyCoefLocation <- base::dir(dirUncertaintyCoef,full.names=TRUE)
    if(base::length(dirUncertaintyCoefLocation)<1){
    log$debug(base::paste0('No troll uncertainty data file in ',dirUncertaintyCoef))
  } else{
    uncertaintyCoef <- rjson::fromJSON(file=dirUncertaintyCoefLocation,simplify=TRUE)
    log$debug(base::paste0("Reading in: ",dirUncertaintyCoefLocation))
  }
  
  #--------left off here -----------
  
  #Uncert for instantaneous 5-min aquatroll data
  uncertaintyData$UTemp_inst_meas<-uncertaintyData$temperature_ucrtMeas
  uncertaintyData$UTemp_inst_expn<-uncertaintyData$temperature_ucrtExpn
  
  uncertaintyData$UCond_inst_meas<-uncertaintyData$conductivity_ucrtMeas
  uncertaintyData$UCond_inst_expn<-uncertaintyData$conductivity_ucrtExpn
  
  uncertaintyData$USpC_inst_meas<-((((1/(1+0.0191*(Temp-25)))^2)*U_CVALA1_cond^2)+(((1+0.0191*RawCond)/((1+0.0191*(Temp-25))^2))^2)*U_CVALA1_temp^2)
  uncertaintyData$USpC_inst_expn<-2*USpC_inst_meas
  
  uncertaintyData$UPressure_inst_meas<-uncertaintyData$pressure_ucrtMeas
  uncertaintyData$UPressure_inst_expn<-uncertaintyData$pressure_ucrtExpn
  
  uncertaintyData$UElev_inst_meas<-(1*survey_uncert^2+((1000/(density*gravity))^2)*U_CVALA1_pressure^2)^0.5
  uncertaintyData$UElev_inst_expn<-2*UElev_inst_meas
  
  
  #Uncert for L1 mean DP (5 min level troll, 30 min level and aqua)
  
  UNat_temp
  UNat_cond
  UNat_SpC
  UNat_pressue
  
  uncertaintyData$UTemp_L1_comb<- (UNat_temp^2+U_CVALA3_temp^2)^0.5
  uncertaintyData$UTemp_L1_expn<-2*UTemp_L1_comb
  
  uncertaintyData$UCond_L1_comb<-(UNat_cond^2+U_CVALA3_cond^2)^0.5
  uncertaintyData$UCond_L1_expn<-2*UCond_L1_comb
  
  uncertaintyData$USpC_L1_comb<-(UNat_SpC^2+(((1/(1+0.0191*(Temp-25)))^2)*U_CVALA3_cond^2)+(((1+0.0191*RawCond)/((1+0.0191*(Temp-25))^2))^2)*U_CVALA3_temp^2)^0.5
  uncertaintyData$USpC_L1_expn<-2*USpC_L1_comb
  
  uncertaintyData$ UPressure_L1_comb<-(UNat_pressue^2+U_CVALA3_pressure^2)^0.5
  uncertaintyData$UPressure_L1_expn<-2*UPressure_L1_comb
  
  uncertaintyData$UElev_L1_comb<-(1*survey_uncert^2+((1000/(density*gravity))^2)*UPressure_L1_comb^2)^0.5
  uncertaintyData$UElev_L1_expn<-2*UElev_L1_comb
  
  
  
  
  
  
  
  infoCal <- list(ucrt = data.frame(Name='U_CVALA1',Value='Value',varUcrt='pressure',stringsAsFactors=FALSE))
  NEONprocIS.cal::def.ucrt.meas.cnst(data=trollData$pressure,infoCal=infoCal)
  
  
  
  
  
  
  
  
  #### Uncertainty Calculations ####
  #the repeatability and reproducibility of the sensor and  uncertainty of the calibration procedures and coefficients including uncertainty in the standard
  U_CVALA1 <- 1 #Combined, standard calibration uncertainty of the measurement by the sensor (uS,C,kPa)
  U_CVALA2 <- 1
  U_CVALA3 <- 1 #Combined, relative uncertainty (truth and trueness only) of the measurement by the sensor (%)
  U_CVALD1 <- 1
  U_CVALD2 <- 1
  U_CVALD3 <- 1

  
  #Temperature Uncertainty
  #combined uncertainty of temperature is equal to the standard uncertainty values provided by CVAL
  theta_T <- U_CVALA2_T  #experimental standard deviation of individual observations for a defined time period
  n <-1  #number of observations made during the defined period
  Unat_T <- ((theta_T^2)/n)^(0.5) #standard error of the mean (natural variation)
  Uc_TGW <- ((Unat_T^2)+(U_CVALA3_T^2))^(0.5)  #combined uncertainty
  #expanded is two times the combined uncertainty
  U_Spc_exp<-2*Uc_TGW
  
  #Conductivity Uncertainty
  #combined uncertainty of actual conductivity (not published) is equal to the standard uncertainty values provided by CVAL
  theta_C <- U_CVALA1_C
  n <-1  #number of observations made during the defined period
  Unat_C <- ((theta_C^2)/n)^(0.5) #standard error of the mean (natural variation)
  #uncertainty of individual specific conductivity measurements takes both conductivity and temp into account
  Uc_SpcGWi <- (((1/(1+0.0191*(trollData$temperature-25)))^2)*(U_CVALA1_C^2)+((0.0191*(trollData$conductivity)/((1+0.0191*(trollData$temperature-25))^2))^2)*(U_CVALA1_T^2))^(0.5)
  
  Uc_CGW <- ((Unat_C^2)+(U_CVALA3_C^2))^(0.5)  #combined uncertainty for individual conductivity
  #### combined uncertainty for specific conductivity
  Uc_SpcGW
  #expanded is two times the combined uncertainty
  U_Spc_exp<-2*Uc_SpcGW
  
  #Elevation Uncertainty
  #survey_uncert is the uncertainty of the sensor elevation relative to other aquatic instruments at the NEON site. 
  #survey_uncert includes the total station survey uncertainty and the uncertainty of hand measurements between the sensor and survey point.
  theta_P <- U_CVALA2_P  #experimental standard deviation of individual observations for a defined time period
  n <-1  #number of observations made during the defined period
  Unat_P <- ((theta^2)/n)^(0.5) #standard error of the mean (natural variation)
  Uc_EGWi <- ((survey_uncert^2)+((1000/(density*gravity))^2)*(U_CVALA1_P^2))^(0.5) #uncertainty of individual groundwater elevation measurements
  Uc_PGW <- ((Unat_P^2)+(U_CVALA3_P^2))^(0.5) #combined uncertainty  for pressure
  Uc_EGW <- ((survey_uncert^2)+((1000/(density*gravity))^2)*(Uc_PGW^2))^(0.5)  #combined uncertainty for groundwater surface elevation includes the combined uncertainties for sensor depth and ground surface
  U_elev_exp <- 2*Uc_EGW  #expanded uncertainty
  

  #Create dataframe for output data
  dataOut <- trollData
  
  #include conductivity based on sensor type
  if(grepl("aquatroll",idxDirIn)){
    dataCol <- c("readout_time","pressure","temperature","conductivity","elev_H2O")
    sensor<-"aquatroll200"
  }else{
    dataCol <- c("readout_time","pressure","temperature","elev_H2O") 
    sensor<-"leveltroll500"
  }
  dataOut <- dataOut[,dataCol]
  
  
  #Write out data
  rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                   NameFile = base::paste0(idxDirOutData,"/",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet"), 
                                                   Schm = SchmDataOut),silent=FALSE)
  if(any(grepl('try-error',class(rptDataOut)))){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
}
