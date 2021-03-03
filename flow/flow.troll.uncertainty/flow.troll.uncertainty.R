##############################################################################################
#' @title Workflow for Level Troll 500 and Aqua Troll 200 Science Computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Workflow. Calculate elevation and derive uncertainty  for surface and groundwater troll data products.
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
#' 4. "FileSchmUcrt=value" (optional), where values is the full path to the avro schema for the output uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 5. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
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
#' readout_time, pressure, pressure_data_quality, temperature, temperature_data quality, elevation, in that order. 
#' Output column/variable names for the aquatroll200 will be readout_time, pressure, pressure_data_quality, 
#' temperature, temperature_data quality, conductivity, conductivity_data_quality, elevation, in that order.
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/groundwaterPhysical_qaqc_data_group')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out")
#' rm(list=setdiff(ls(),c('arg','log')))

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
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut"),NameParaOptn = c("FileSchmData","FileSchmUcrt"),log = log)

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
# Retrieve output schema for data
FileSchmUcrtOut <- Para$FileUcrtUcrt
log$debug(base::paste0('Output schema for uncertainty: ',base::paste0(FileSchmUcrtOut,collapse=',')))


# Read in the schemas
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NULL
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}
if(base::is.null(FileSchmUcrtOut) || FileSchmUcrtOut == 'NA'){
  SchmUcrtOut <- NULL
} else {
  SchmUcrtOut <- base::paste0(base::readLines(FileSchmUcrtOut),collapse='')
}


# Retrieve optional sensor subdirectories to copy over
nameDirSubCopy <- c('location')
DirLocationCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))


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
  #idxDirIn<-DirIn[1] #for testing
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirOutUcrt <- base::paste0(idxDirOut,'/uncertainty')
  base::dir.create(idxDirOutUcrt,recursive=TRUE)

  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirLocationCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/location'),idxDirOut,log=log)
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
  
  # Which location history matches each readout_time
  troll_all<-NULL
  if(length(LocationHist$CFGLOC)>0){
    for(i in 1:length(LocationHist$CFGLOC)){
      startDate<-LocationHist$CFGLOC[[i]]$start_date
      endDate<-LocationHist$CFGLOC[[i]]$end_date
      troll_sub<-trollData[trollData$readout_time>=startDate && trollData$readout_time<endDate,]
      troll_sub$elevation<- LocationHist$CFGLOC[[i]]$geometry$coordinates[3]
      troll_sub$z_offset<- LocationHist$CFGLOC[[i]]$z_offset
      troll_sub$survey_uncert <- LocationHist$CFGLOC[[i]]$`Survey vertical uncertainty` #includes survey uncertainty and hand measurements
      troll_sub$real_world_uncert <-LocationHist$CFGLOC[[i]]$`Real world coordinate uncertainty`  
      if(i==1){
        troll_all<-troll_sub
      }else{
        troll_all<-rbind(troll_all,troll_sub)
      }
    }
    trollData<-troll_all
  }
  
  #calculate water table elevation
  trollData$elevation<-NA
  if(length(LocationHist)>0){
    trollData$elevation<-trollData$elevation+trollData$z_offset+(1000*trollData$pressure/(density*gravity))
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
  
  
  #Define troll type and context
  #include conductivity based on sensor type
  if(grepl("aquatroll",idxDirIn)){
    sensor<-"aquatroll200"
  }else{
    sensor<-"leveltroll500"
  }
  if(grepl("groundwater",idxDirIn)){
    context<-"GW"
  }else{ 
    context<-"SW"
  }

  
  ######## Uncert for instantaneous 5-min groundwater aqua troll data #######
  #surface water does not have instantaneous L1 output
  if(context=="GW"){
    #temp and pressure uncert calculated earlier in pipeline
    #existing conductivity uncertainty is for raw values, need additional columns for specific conductivity
    uncertaintyData$rawConductivity_ucrtMeas<-uncertaintyData$conductivity_ucrtMeas
    uncertaintyData$conductivity_ucrtMeas<-NA
    uncertaintyData$rawConductivity_ucrtComb<-uncertaintyData$conductivity_ucrtComb
    uncertaintyData$conductivity_ucrtComb<-NA
    uncertaintyData$rawConductivity_ucrtExpn<-uncertaintyData$conductivity_ucrtExpn
    uncertaintyData$conductivity_ucrtExpn<-NA
    #check that data frames are equal length
    if(length(uncertaintyData$conductivity_ucrtMeas)!=length(trollData$raw_conductivity)){
      log$warn("Conductivity data and uncertainty data frames are not of equal length")
      stop()
    }
    if(length(uncertaintyData$temperature_ucrtMeas)!=length(trollData$temperature)){
      log$warn("Temperature data and uncertainty data frames are not of equal length")
      stop()
    }
    #calculate uncertainty for specific conductivity
    for(i in 1:length(uncertaintyData$rawConductivity_ucrtMeas)){
      U_CVALA1_cond<-uncertaintyData$rawConductivity_ucrtMeas[i]
      U_CVALA1_temp<-uncertaintyData$temperature_ucrtMeas[i]
      uncertaintyData$conductivity_ucrtMeas[i]<-((((1/(1+0.0191*(trollData$temperature[i]-25)))^2)*U_CVALA1_cond^2)+((((0.0191*trollData$raw_conductivity[i])/((1+0.0191*(trollData$temperature[i]-25))^2))^2)*U_CVALA1_temp^2))^0.5
    }
    uncertaintyData$conductivity_ucrtComb<-uncertaintyData$conductivity_ucrtMeas
    uncertaintyData$conductivity_ucrtExpn<-2*uncertaintyData$conductivity_ucrtMeas
    
    #calculate instantaneous elevation uncert
    uncertaintyData$elevation_ucrtMeas<-NA
    if(length(LocationHist)>0){
      for(i in 1:length(uncertaintyData$pressure_ucrtMeas)){
        U_CVALA1_pressure<-uncertaintyData$pressure_ucrtMeas[i]
        uncertaintyData$elevation_ucrtMeas[i]<-(1*trollData$survey_uncert[i]^2+((1000/(density*gravity))^2)*U_CVALA1_pressure^2)^0.5
      }
    }else{
      uncertaintyData$elevation_ucrtMeas<-NA
    }
    uncertaintyData$elevation_ucrtComb<-uncertaintyData$elevation_ucrtMeas
    uncertaintyData$elevation_ucrtExpn<-2*uncertaintyData$elevation_ucrtMeas
  }
  
  
  
  ######## Uncertainty for L1 mean 5 and 30 minute outputs ########
  #the repeatability and reproducibility of the sensor and  uncertainty of the calibration procedures and coefficients including uncertainty in the standard
  if(length(uncertaintyCoef)>0){
    #Temperature Uncertainty
    #combined uncertainty of temperature is equal to the standard uncertainty values provided by CVAL
    #UTemp_L1_Expn<-2*(UNat_temp^2+U_CVALA3_temp^2)^0.5
    uncertaintyData$UTemp_L1_Expn<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=trollData,VarUcrt='temperature',ucrtCoef=uncertaintyCoef)
    
    #Pressure Uncertainty
    #combined uncertainty of pressure is equal to the standard uncertainty values provided by CVAL
    #UPressure_L1_Expn<-2*(UNat_pressure^2+U_CVALA3_pressure^2)^0.5
    uncertaintyData$UPressure_L1_Expn<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=trollData,VarUcrt='pressure',ucrtCoef=uncertaintyCoef)
    uncertaintyData$UPressure_L1_Comb<-uncertaintyData$UPressure_L1_Expn/2
    
    #Elevation Uncertainty
    #survey_uncert is the uncertainty of the sensor elevation relative to other aquatic instruments at the NEON site. 
    #survey_uncert includes the total station survey uncertainty and the uncertainty of hand measurements between the sensor and survey point.
    uncertaintyData$UElev_L1_Expn<-2*((1*trollData$survey_uncert^2+((1000/(density*gravity))^2)*uncertaintyData$UPressure_L1_Comb^2)^0.5)
    
    if(sensor=='aquatroll200'){
      #Raw Conductivity Uncertainty
      #combined uncertainty of actual conductivity (not published) is equal to the standard uncertainty values provided by CVAL
      #UCond_L1_Expn<-2*(UNat_cond^2+U_CVALA3_cond^2)^0.5
      uncertaintyData$UCond_L1_Expn<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=trollData,VarUcrt='conductivity',ucrtCoef=uncertaintyCoef)
      
      #Specific Conductivity Uncertainty
      #grab U_CVALA3 values
      U_CVALA3_cond<-NULL
      ucrtCoef<-NULL
      for(i in 1:length(uncertaintyCoef)){
        if(uncertaintyCoef[[i]]$term =='conductivity' & uncertaintyCoef[[i]]$Name == 'U_CVALA3'){
          ucrtCoef<-as.double(uncertaintyCoef[[i]]$Value)
          U_CVALA3_cond<-append(U_CVALA3_cond,ucrtCoef)
        }
      }
      if(length(U_CVALA3_cond)>1){
        log$warn("Multiple conductivity U_CVALA3 coefficients")
        stop()
      }
      U_CVALA3_temp<-NULL
      for(i in 1:length(uncertaintyCoef)){
        if(uncertaintyCoef[[i]]$term =='temperature' & uncertaintyCoef[[i]]$Name == 'U_CVALA3'){
          ucrtCoef<-as.double(uncertaintyCoef[[i]]$Value)
          U_CVALA3_temp<-append(U_CVALA3_temp,ucrtCoef)
        }
      }
      if(length(U_CVALA3_temp)>1){
        log$warn("Multiple temperature U_CVALA3 coefficients")
        stop()
      }
      
      # Compute uncertainty of the mean due to natural variation, represented by the standard error of the mean
      #log$debug(base::paste0('Computing L1 uncertainty due to natural variation (standard error)'))
      dataComp<-trollData$conductivity
      numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
      se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
      
      for(i in 1:length(uncertaintyData$conductivity_ucrtMeas)){
        uncertaintyData$USpC_L1_Comb[i]<-((se^2)*(((1/(1+0.0191*(trollData$temperature[i]-25)))^2)*U_CVALA3_cond^2)+((((0.0191*trollData$raw_conductivity[i])/((1+0.0191*(trollData$temperature[i]-25))^2))^2)*U_CVALA3_temp^2))^0.5
      }
      uncertaintyData$USpC_L1_Expn<-2*uncertaintyData$USpC_L1_Comb
    }
  }else{
    uncertaintyData$UTemp_L1_Expn<-NA
    uncertaintyData$UPressure_L1_Expn<-NA
    uncertaintyData$UElev_L1_Expn<-NA
    uncertaintyData$USpC_L1_Expn<-NA
  }
  


  #Create dataframe for output data
  dataOut <- trollData
  if(sensor=="aquatroll200"){
    dataCol <- c("readout_time","pressure","temperature","conductivity","elevation")
  }else{
    dataCol <- c("readout_time","pressure","temperature","elevation") 
  }
  dataOut <- dataOut[,dataCol]
  
  #Create dataframe for output uncertainty
  ucrtOut <- uncertaintyData
  if(context=="GW"){
    ucrtCol_05min <- c("readout_time","pressure_ucrtExpn","temperature_ucrtExpn","conductivity_ucrtExpn","elevation_ucrtExpn")
    ucrtCol_30min <- c("readout_time","UTemp_L1_Expn","UPressure_L1_Expn","UElev_L1_Expn","USpC_L1_Expn")
  }else if(sensor=="aquatroll200"){
    ucrtCol_05min <- c("readout_time","UTemp_L1_Expn","UPressure_L1_Expn","UElev_L1_Expn","USpC_L1_Expn")
    ucrtCol_30min <- c("readout_time","UTemp_L1_Expn","UPressure_L1_Expn","UElev_L1_Expn","USpC_L1_Expn")
  }else{
    ucrtCol_05min <- c("readout_time","UTemp_L1_Expn","UPressure_L1_Expn","UElev_L1_Expn")
    ucrtCol_30min <- c("readout_time","UTemp_L1_Expn","UPressure_L1_Expn","UElev_L1_Expn")
  }
  ucrtOut_05min <- uncertaintyData[,ucrtCol_05min]
  ucrtOut_30min <- uncertaintyData[,ucrtCol_30min]
  
  
  #Write out data
  rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                   NameFile = base::paste0(idxDirOutData,"/",context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet"), 
                                                   Schm = SchmDataOut),silent=FALSE)
  if(any(grepl('try-error',class(rptDataOut)))){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
  
  #Write out uncertainty data
  rptUcrtOut_05min <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_05min, 
                                                   NameFile = base::paste0(idxDirOutUcrt,"/",context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_05minUcrt.parquet"), 
                                                   Schm = SchmUcrtOut),silent=FALSE)
  rptUcrtOut_30min <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_30min, 
                                                   NameFile = base::paste0(idxDirOutUcrt,"/",context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_30minUcrt.parquet"), 
                                                   Schm = SchmUcrtOut),silent=FALSE)
  if(any(grepl('try-error',class(rptUcrtOut_05min)))){
    log$error(base::paste0('Writing the output data failed: ',attr(rptUcrtOut_05min,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
  if(any(grepl('try-error',class(rptUcrtOut_30min)))){
    log$error(base::paste0('Writing the output data failed: ',attr(rptUcrtOut_30min,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
}
