
wrap.qaqc.temp.air.aspi <- function(DirIn, 
                                    DirOut,
                                    SensTermTemp,
                                    SensTermTbne,
                                    AvelTbneMin,
                                    SensTermWind,
                                    sensWind,
                                    VeloWindMin,
                                    SensTermHeat,
                                    DirSubCopyTemp,
                                    DirSubCopySens,
                                    log, 
                                    RmvFlow,
                                    RmvHeat,
                                    SchmDataOut,
                                    SchmQfOut
){ 
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  TimeCool <- base::as.difftime(224,units='secs') # Cooling time after heater turns off. See NEON.DOC.000646 & NEON.DOC.000302
  
  # Create the binning for each 30-second interval evaluated for adequate aspiration
  timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=base::as.difftime(30,units="secs"),WndwTime=base::as.difftime(1,units='days'))
  
  # Create dummy vectors for evaluating 30-second average flow rates for each day
  flagFailSec30 <- base::rep(1,2880) # initialize flag to 1 (fail)
  naSec30 <- NA*flagFailSec30
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  base::dir.create(idxDirOut,recursive=TRUE)
  
  # Copy with a symbolic link the desired sensor subfolders 
  if(base::length(DirSubCopySens) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn,'/',DirSubCopySens),idxDirOut,log=log)
  }  
  
  # Flesh out sensor directories (except for wind, that's below)
  dirTemp <- base::paste0(DirIn,'/',SensTermTemp$sens)
  dirTbne <- base::paste0(DirIn,'/',SensTermTbne$sens)
  dirHeat <- base::paste0(DirIn,'/',SensTermHeat$sens)
  
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
    dirWind <- base::paste0(DirIn,'/',idxSensWind)
    
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
  qfFlowSec30[dataFlow$avelTbne >= AvelTbneMin] <- 0 # Pass flow test if turbine speed at/above minimum 
  qfFlowSec30[dataFlow$veloWind >= VeloWindMin] <- 0 # Pass flow test if wind speed at/above minimum 
  qfFlowSec30[base::is.na(dataFlow$veloWind) & base::is.na(dataFlow$avelTbne)] <- -1 # Test indeterminate if no wind speed or turbine data 
  qfFlowSec30[base::is.na(dataFlow$veloWind) & dataFlow$avelTbne < AvelTbneMin] <- -1 # Test indeterminate if turbine speed below min but no wind speed 
  qfFlowSec30[base::is.na(dataFlow$avelTbne) & dataFlow$veloWind < VeloWindMin] <- -1 # Test indeterminate if wind speed below min but no turbine 
  
  
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
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirLocTemp,log=log)
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
      if(RmvFlow == TRUE){
        dataTemp[[SensTermTemp$term]][qf$qfFlow==1] <- NA
      }
      if(RmvHeat == TRUE){
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
}
