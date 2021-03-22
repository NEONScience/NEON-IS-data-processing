Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/groundwaterPhysical_qaqc_data_group')
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","WndwInst=005","WndwAgr=030")

#' 4. "WndwAgr=value", where value is the aggregation interval for which to compute statistics. It is formatted 
#' as a 3 character sequence, typically representing the number of minutes over which to compute statistics. 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals.
#' 
#' 5. "WndwInst=value"

################################################

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut"),NameParaOptn = c("DirSubCopy","FileSchmData","FileSchmUcrt","WndwInst","WndwAgr"),log = log)

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

# Retrieve instantaneous and aggregation intervals
if(base::is.null(Para$WndwInst) || Para$WndwInst == 'NA'){
  WndwInst <- NULL
}else{
  WndwInst <- base::as.difftime(base::as.numeric(Para$WndwInst),units="mins")
  log$debug(base::paste0('Instantaneous interval(s), in minutes: ',base::paste0(WndwInst,collapse=',')))
}

if(base::is.null(Para$WndwAgr) || Para$WndwAgr == 'NA'){
  WndwAgr <- NULL
}else{
  WndwAgr <- base::as.difftime(base::as.numeric(Para$WndwAgr),units="mins")
  log$debug(base::paste0('Aggregation interval(s), in minutes: ',base::paste0(WndwInst,collapse=',')))
}

# Retrieve optional sensor subdirectories to copy over
nameDirSubCopy <- c('location','uncertainty_coef')
DirLocationCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))
DirUncertCoefCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[2]))



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

# Create the binning for each aggregation interval
timeBgnDiff <- list()
timeEndDiff <- list()
for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
  timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
} # End loop around aggregation intervals

# Process each datum
for (idxDirIn in DirIn){
  ##### Logging and initializing #####
  #idxDirIn<-DirIn[1] #for testing
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirOutUcrt <- base::paste0(idxDirOut,'/uncertainty_data')
  base::dir.create(idxDirOutUcrt,recursive=TRUE)
  
  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirLocationCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/location'),idxDirOut,log=log)
  }
  if(base::length(DirUncertCoefCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/uncertainty_coef'),idxDirOut,log=log)
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
      troll_sub$sensorElevation<- LocationHist$CFGLOC[[i]]$geometry$coordinates[3]
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
    trollData$elevation<-trollData$sensorElevation+trollData$z_offset+(1000*trollData$pressure/(density*gravity))
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
  
  #Create dataframe for output data
  dataOut <- trollData
  if(sensor=="aquatroll200"){
    dataCol <- c("readout_time","pressure","temperature","conductivity","elevation")
  }else{
    dataCol <- c("readout_time","pressure","temperature","elevation") 
  }
  dataOut <- dataOut[,dataCol]
  
  
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
    uncertaintyCoef <- base::try(rjson::fromJSON(file=dirUncertaintyCoefLocation,simplify=TRUE),silent=FALSE)
    if(base::class(uncertaintyCoef) == 'try-error'){
      # Generate error and stop execution
      log$error(base::paste0('File: ', dirUncertaintyCoefLocation, ' is unreadable.')) 
      stop()
    }
    # Turn times to POSIX
    uncertaintyCoef <- base::lapply(uncertaintyCoef,FUN=function(idxUcrt){
      idxUcrt$start_date <- base::strptime(idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
      idxUcrt$end_date <- base::strptime(idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
      return(idxUcrt)
    })
    log$debug(base::paste0("Reading in: ",dirUncertaintyCoefLocation))
  }
  
  
  
  
  ######## Uncert for instantaneous 5-min groundwater aqua troll data #######
  #surface water does not have instantaneous L1 output
  if(length(WndwInst)>0){
    if(sensor=="aquatroll200"){
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
    }
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
    
    #Create dataframes for output uncertainties
    ucrtCol_inst <- c("readout_time","temperature_ucrtExpn","pressure_ucrtExpn","elevation_ucrtExpn","conductivity_ucrtExpn")
    ucrtOut_inst <- uncertaintyData[,ucrtCol_inst]
    #standardize naming
    names(ucrtOut_inst)<- c("readout_time","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
  }
  
  
  
  if(length(WndwAgr)>0){
    ######## Uncertainty for L1 mean 5 and 30 minute outputs ########
    #the repeatability and reproducibility of the sensor and  uncertainty of the calibration procedures and coefficients including uncertainty in the standard
    
    #determine averaging window
    timeMeas <- base::as.POSIXlt(trollData$readout_time)# Pull out time variable
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      #idxWndwAgr<-1 #for testing
      log$debug(base::paste0('Computing stats for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      
      # Allocate data points to aggregation windows
      setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      
      # Allocate uncertainty data points to aggregation windows
      if(!base::is.null(uncertaintyData)){
        setTimeUcrt <- base::.bincode(base::as.numeric(base::as.POSIXlt(uncertaintyData$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      } else {
        setTimeUcrt <- base::numeric(0)
      }
      
      # Run through the time bins
      for(idxWndwTime in base::unique(setTime)){
        #idxWndwTime<-1 #for testing
        # Rows to pull
        dataWndwTime <- base::subset(trollData,subset=setTime==idxWndwTime) 
        ucrtDataWndwTime <- base::subset(uncertaintyData,subset=setTime==idxWndwTime)  
        
        # Compute L1 uncertainty 
        if(length(uncertaintyCoef)>0){
          #Temperature Uncertainty
          #combined uncertainty of temperature is equal to the standard uncertainty values provided by CVAL
          #TemperatureExpUncert<-2*(UNat_temp^2+U_CVALA3_temp^2)^0.5
          ucrtDataWndwTime$temperature_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=dataWndwTime,VarUcrt='temperature',ucrtCoef=uncertaintyCoef)
          
          #Pressure Uncertainty
          #combined uncertainty of pressure is equal to the standard uncertainty values provided by CVAL
          #pressure_ucrtExpn_L1<-2*(UNat_pressure^2+U_CVALA3_pressure^2)^0.5
          ucrtDataWndwTime$pressure_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=dataWndwTime,VarUcrt='pressure',ucrtCoef=uncertaintyCoef)
          ucrtDataWndwTime$pressure_ucrtComb_L1<-ucrtDataWndwTime$pressure_ucrtExpn_L1/2
          
          #Elevation Uncertainty
          #survey_uncert is the uncertainty of the sensor elevation relative to other aquatic instruments at the NEON site. 
          #survey_uncert includes the total station survey uncertainty and the uncertainty of hand measurements between the sensor and survey point.
          ucrtDataWndwTime$elevation_ucrtExpn_L1<-2*((1*dataWndwTime$survey_uncert^2+((1000/(density*gravity))^2)*ucrtDataWndwTime$pressure_ucrtComb_L1^2)^0.5)
          
          if(sensor=='aquatroll200'){
            #Raw Conductivity Uncertainty
            #combined uncertainty of actual conductivity (not published) is equal to the standard uncertainty values provided by CVAL
            #rawConductivity_ucrtExpn_L1<-2*(UNat_cond^2+U_CVALA3_cond^2)^0.5
            ucrtDataWndwTime$rawConductivity_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.mult(data=dataWndwTime,VarUcrt='conductivity',ucrtCoef=uncertaintyCoef)
            
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
            dataComp<-dataWndwTime$conductivity
            ucrtDataWndwTime$conductivity_ucrtComb_L1<-NA
            numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
            se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
            # Compute combined uncertainty for L1 specific conductivity
            for(i in 1:length(ucrtDataWndwTime$conductivity_ucrtMeas)){
              ucrtDataWndwTime$conductivity_ucrtComb_L1[i]<-((se^2)*(((1/(1+0.0191*(dataWndwTime$temperature[i]-25)))^2)*U_CVALA3_cond^2)+((((0.0191*dataWndwTime$raw_conductivity[i])/((1+0.0191*(dataWndwTime$temperature[i]-25))^2))^2)*U_CVALA3_temp^2))^0.5
            }
            # Compute expanded uncertainty for L1 specific conductivity
            ucrtDataWndwTime$conductivity_ucrtExpn_L1<-2*ucrtDataWndwTime$conductivity_ucrtComb_L1
          }
        }else{
          ucrtDataWndwTime$temperature_ucrtExpn_L1<-NA
          ucrtDataWndwTime$pressure_ucrtExpn_L1<-NA
          ucrtDataWndwTime$elevation_ucrtExpn_L1<-NA
          ucrtDataWndwTime$conductivity_ucrtExpn_L1<-NA
        }
        #combine data frames
        if(idxWndwTime==1){
          allUcrtData<-ucrtDataWndwTime
        }else{
          allUcrtData<-rbind(allUcrtData,ucrtDataWndwTime)
        }
      } # End loop through time windows
      uncertaintyData<-allUcrtData
      
      #Create dataframes for output uncertainties
      if(sensor=="aquatroll200"){
        #Conductivity included for aqua troll.
        ucrtCol_agr <- c("readout_time","temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1","conductivity_ucrtExpn_L1")
      }else{
        #Conductivity not included for level troll.
        ucrtCol_agr <- c("readout_time","temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1")
      }
      ucrtOut_agr <- uncertaintyData[,ucrtCol_agr]
      #standardize column names
      if(context=="GW"){
        names(ucrtOut_agr)<- c("readout_time","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
      }else if(sensor=="aquatroll200"){
        names(ucrtOut_agr)<- c("readout_time","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert","surfacewaterCondExpUncert")
      }else{
        names(ucrtOut_agr)<- c("readout_time","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert")
      }
    }
  }
  
  
  
  
  
  
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
  if(length(WndwInst)>0){
    rptUcrtOut_Inst <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_inst, 
                                                          NameFile = base::paste0(idxDirOutUcrt,"/",context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_",WndwInst[1],"_minUcrt.parquet"), 
                                                          Schm = SchmUcrtOut),silent=FALSE)
    
    if(any(grepl('try-error',class(ucrtOut_inst)))){
      log$error(base::paste0('Writing the output data failed: ',attr(ucrtOut_inst,"condition")))
      stop()
    } else {
      log$info("Data written out.")
    }
  }
  
  if(length(WndwAgr)>0){
    rptUcrtOut_Agr <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_agr, 
                                                         NameFile = base::paste0(idxDirOutUcrt,"/",context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_",WndwAgr[idxWndwAgr],"_minUcrt.parquet"), 
                                                         Schm = SchmUcrtOut),silent=FALSE)
    
    if(any(grepl('try-error',class(rptUcrtOut_Agr)))){
      log$error(base::paste0('Writing the output data failed: ',attr(rptUcrtOut_Agr,"condition")))
      stop()
    } else {
      log$info("Data written out.")
    }
  }
}
