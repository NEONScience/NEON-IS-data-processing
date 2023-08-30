##############################################################################################
#' @title Workflow for Level Troll 500 and Aqua Troll 200 Science Computations
#' flow.troll.uncertainty.R
#' 
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Workflow. Calculate elevation and derive uncertainty  for surface and groundwater troll data products.
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
#' 3. "Context=value", where the value must be designated as either "surfacewater" or "groundwater".
#' 
#' 4. "WndwAgr=value", (optional) where value is the aggregation interval for which to compute unceratainty. It is 
#' formatted as a 3 character sequence, typically representing the number of minutes over which to compute unceratainty 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals. No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' 5. "WndwInst=TRUE", (optional) set to TRUE to include instantaneous uncertainty data output. The defualt value is FALSE. 
#' No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' 6. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' 7. "FileSchmUcrt=value" (optional), where values is the full path to the avro schema for the output uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' 8. "FileSchmSciStats=value" (optional), where values is the full path to the avro schema for the output science statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#' 
#' 9. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
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
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA MATCHES THIS ORDER. Otherwise, they will be labeled incorrectly.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' @keywords Currently none
#' 
#' @examples
#' Stepping through the code in Rstudio 
#' Sys.setenv(DIR_IN='/home/NEON/ncatolico/pfs/surfacewaterPhysical_analyze_pad_and_qaqc_plau/2020/01/02')
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' arg <- c("DirIn=$DIR_IN","DirOut=~/pfs/out","Context=surfacewater","WndwInst=TRUE","WndwAgr=005|030")
#' rm(list=setdiff(ls(),c('arg','log')))
#' 
#' @seealso None currently
#' changelog and author contributions / copyrights
#'   Nora Catolico (2021-02-02)
#'     original creation
#'   Nora Catolico (2023-03-03)
#'     updated for no troll data use case
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg = arg,NameParaReqd = c("DirIn", "DirOut","Context"),NameParaOptn = c("DirSubCopy","FileSchmData","FileSchmUcrt","FileSchmSciStats","WndwInst","WndwAgr"),log = log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve output schema for data
FileSchmDataOut <- Para$FileSchmData
log$debug(base::paste0('Output schema for data: ',base::paste0(FileSchmDataOut,collapse=',')))
# Retrieve output schema for uncertainty
FileSchmUcrtOut <- Para$FileSchmUcrt
log$debug(base::paste0('Output schema for uncertainty: ',base::paste0(FileSchmUcrtOut,collapse=',')))
# Retrieve output schema for science stats
FileSchmSciStatsOut <- Para$FileSchmSciStats
log$debug(base::paste0('Output schema for science stats: ',base::paste0(FileSchmSciStatsOut,collapse=',')))


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
if(base::is.null(FileSchmSciStatsOut) || FileSchmSciStatsOut == 'NA'){
  SchmSciStatsOut <- NULL
} else {
  SchmSciStatsOut <- base::paste0(base::readLines(FileSchmSciStatsOut),collapse='')
}



# Retrieve context
if(Para$Context=="groundwater"){
  Context<-"GW"
}else if(Para$Context=="surfacewater"){
  Context<-"SW"
}else{
  log$fatal('Context must equal groundwater or surfacewater.')
  stop()
}
log$debug(base::paste0('Outputs will be calculated for ',base::paste0(Para$Context,collapse=','),' data products.'))

# Retrieve instantaneous and aggregation intervals
if(base::is.null(Para$WndwInst) || Para$WndwInst == 'NA'|| Para$WndwInst == "FALSE"){
  WndwInst <- FALSE
}else{
  WndwInst <- TRUE
  log$debug(base::paste0('Instantaneous uncertainty data will be included in the output.'))
}

if(base::is.null(Para$WndwAgr) || Para$WndwAgr == 'NA'){
  WndwAgr <- NULL
}else{
  WndwAgr <- base::as.difftime(base::as.numeric(Para$WndwAgr),units="mins")
  log$debug(base::paste0('Aggregation interval(s), in minutes: ',base::paste0(WndwAgr,collapse=',')))
}

# Retrieve optional sensor subdirectories to copy over
if(base::is.null(Para$DirSubCopy) || Para$DirSubCopy == 'NA'){
  nameDirSubCopy <- NULL
}else{
  nameDirSubCopy <- Para$DirSubCopy
  DirUncertCoefCopy <- base::unique(base::setdiff(Para$DirIn,nameDirSubCopy[1]))
}


#what are the expected subdirectories of each input path
nameDirSub <- c('data','flags','location')
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
if(length(WndwAgr)>0){
  timeBgnDiff <- list()
  timeEndDiff <- list()
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
    timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  } # End loop around aggregation intervals
}


# Process each datum
for (idxDirIn in DirIn){
  ##### Logging and initializing #####
  #idxDirIn<-DirIn[3] #for testing
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  if(Context=="GW"){
    idxDirOutData <- base::paste0(idxDirOut,'/data')
    base::dir.create(idxDirOutData,recursive=TRUE)
  }
  idxDirOutSciStats <- base::paste0(idxDirOut,'/sci_stats')
  base::dir.create(idxDirOutSciStats,recursive=TRUE)
  idxDirOutUcrt <- base::paste0(idxDirOut,'/uncertainty_data')
  base::dir.create(idxDirOutUcrt,recursive=TRUE)
  
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
  if(length(trollData)>0){
    troll_all<-NULL
    if(length(LocationHist$CFGLOC)>0){
      for(i in 1:length(LocationHist$CFGLOC)){
        startDate<-LocationHist$CFGLOC[[i]]$start_date
        endDate<-LocationHist$CFGLOC[[i]]$end_date
        troll_sub<-trollData[trollData$readout_time>=startDate & trollData$readout_time<endDate,]
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
    trollData$startDateTime<-as.POSIXct(troll_all$readout_time)
    trollData$endDateTime<-as.POSIXct(troll_all$readout_time)
    
    #calculate water table elevation
    trollData$elevation<-NA
    if(length(LocationHist)>0){
      trollData$elevation<-trollData$sensorElevation+trollData$z_offset+(1000*trollData$pressure/(density*gravity))
    }
    
    #Define troll type
    #include conductivity based on sensor type
    if(grepl("aquatroll",idxDirIn)){
      sensor<-"aquatroll200"
    }else{
      sensor<-"leveltroll500"
    }
    
    #Create dataframe for output data
    dataOut <- trollData
    if(sensor=="aquatroll200"){
      dataCol <- c("startDateTime","endDateTime","pressure","temperature","conductivity","elevation")
    }else{
      dataCol <- c("startDateTime","endDateTime","pressure","temperature","elevation") 
    }
    dataOut <- dataOut[,dataCol]
  }
  
  
  #Write out instantaneous data for groundwater only
  #dataOut[,-1] <-round(dataOut[,-1],2)
  
    rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                     NameFile = base::paste0(idxDirOutData,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_005.parquet"), 
                                                     Schm = SchmDataOut),silent=FALSE)
    if(any(grepl('try-error',class(rptDataOut)))){
      log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
      stop()
    } else {
      log$info("Data written out.")
    }
  
  
  
  
  
  ### Read in flags
  flags <- NULL
  dirFlags <- base::paste0(idxDirIn,'/flags')
  dirFlagsLocation <- base::dir(dirFlags,full.names=TRUE)
  if(base::length(dirFlagsLocation)<1){
    log$debug(base::paste0('No flag data file in ',dirFlags))
  } else{
    flags <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirFlagsLocation),log = log), silent = FALSE)
    log$debug(base::paste0("Reading in: ",dirFlagsLocation))
  }
  #add flags to troll data
  trollData<-merge(trollData,flags,by="readout_time", all.x=TRUE)
  flagDataCol<-c("readout_time","pressure","elevation","pressureSpikeQF")
  flagData<-trollData[,flagDataCol]
  
  #####Calculate 30-min average troll data mean while excluding points that fail the spike test. Output in Sci_stats.
  if(length(WndwAgr)>0){
    #determine averaging window
    timeMeas <- base::as.POSIXlt(flagData$readout_time)# Pull out time variable
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      #idxWndwAgr<-1 #for testing
      log$debug(base::paste0('Computing mean pressure and elevation for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      
      # Allocate data points to aggregation windows
      setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      
      # Allocate data points to aggregation windows
      if(!base::is.null(flagData)){
        setTimeSciStats <- base::.bincode(base::as.numeric(base::as.POSIXlt(flagData$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      } else {
        setTimeSciStats <- base::numeric(0)
      }
      
      # Intialize the output
      rptSciStats <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      nameSciStatsTerm <- c("groundwaterPressureMean","groundwaterPressureMin","groundwaterPressureMax","groundwaterPressureVariance",
                            "groundwaterPressureNumPts","groundwaterPressureStdErMean","groundwaterElevMean","groundwaterElevMin",
                            "groundwaterElevMax","groundwaterElevVariance","groundwaterElevNumPts","groundwaterElevStdErMean")
      rptSciStats[,3:(base::length(nameSciStatsTerm)+2)] <- base::as.numeric(NA)
      base::names(rptSciStats)[3:(base::length(nameSciStatsTerm)+2)] <- nameSciStatsTerm
      
      # Run through the time bins
      for(idxWndwTime in base::unique(setTime)){
        #idxWndwTime<-1 #for testing
        # Rows to pull
        flagDataWndwTime <- base::subset(flagData,subset=setTime==idxWndwTime)  
        #only use data that does not fail spike test
        flagDataWndwTime<-flagDataWndwTime[!is.na(flagDataWndwTime$pressure)&flagDataWndwTime$pressureSpikeQF==0,]
        # Compute stats excluding flagged data
        if(length(flagDataWndwTime$pressure)>0){
          groundwaterPressureMean<-mean(flagDataWndwTime$pressure)
          groundwaterPressureMin<-min(flagDataWndwTime$pressure)
          groundwaterPressureMax<-max(flagDataWndwTime$pressure)
          groundwaterPressureVariance<-var(flagDataWndwTime$pressure)
          groundwaterPressureStdErMean<-sd(flagDataWndwTime$pressure)/base::sqrt(nrow(flagDataWndwTime))
          groundwaterPressureNumPts<-base::as.integer(nrow(flagDataWndwTime))
          groundwaterElevMean<-mean(flagDataWndwTime$elevation)
          groundwaterElevMin<-min(flagDataWndwTime$elevation)
          groundwaterElevMax<-max(flagDataWndwTime$elevation)
          groundwaterElevVariance<-var(flagDataWndwTime$elevation)
          groundwaterElevStdErMean<-sd(flagDataWndwTime$elevation)/base::sqrt(nrow(flagDataWndwTime))
          groundwaterElevNumPts<-base::as.integer(nrow(flagDataWndwTime))
          
          #copy info to output dataframe
          rptSciStats$groundwaterPressureMean[idxWndwTime] <- groundwaterPressureMean
          rptSciStats$groundwaterPressureMin[idxWndwTime] <- groundwaterPressureMin
          rptSciStats$groundwaterPressureMax[idxWndwTime] <- groundwaterPressureMax
          rptSciStats$groundwaterPressureVariance[idxWndwTime] <- groundwaterPressureVariance
          rptSciStats$groundwaterPressureNumPts[idxWndwTime] <- groundwaterPressureNumPts
          rptSciStats$groundwaterPressureStdErMean[idxWndwTime] <- groundwaterPressureStdErMean
          rptSciStats$groundwaterElevMean[idxWndwTime] <- groundwaterElevMean
          rptSciStats$groundwaterElevMin[idxWndwTime] <- groundwaterElevMin
          rptSciStats$groundwaterElevMax[idxWndwTime] <- groundwaterElevMax
          rptSciStats$groundwaterElevVariance[idxWndwTime] <- groundwaterElevVariance
          rptSciStats$groundwaterElevNumPts[idxWndwTime] <- groundwaterElevNumPts
          rptSciStats$groundwaterElevStdErMean[idxWndwTime] <- groundwaterElevStdErMean
        }
      } # End loop through time windows
      
      #Write out aggregate uncertainty data
      #rptSciStats[,-c(1:2)] <-round(rptSciStats[,-c(1:2)],2)
      if(WndwAgr[idxWndwAgr]==5){
        window<-"005"
      }else if(WndwAgr[idxWndwAgr]==30){
        window<-"030"
      }else{
        window<-NA
      }
      rptSciStatsOut <- try(NEONprocIS.base::def.wrte.parq(data = rptSciStats, 
                                                           NameFile = base::paste0(idxDirOutSciStats,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_sciStats_",window,".parquet"), 
                                                           Schm = SchmSciStatsOut),silent=FALSE)
      
      if(any(grepl('try-error',class(rptSciStatsOut)))){
        log$error(base::paste0('Writing the output data failed: ',attr(rptSciStatsOut,"condition")))
        stop()
      } else {
        log$info("Stats written out.")
      }
    }
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
  if(WndwInst==TRUE){
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
    uncertaintyData$startDateTime<-uncertaintyData$readout_time
    timeDiff<-uncertaintyData$startDateTime[2]-uncertaintyData$startDateTime[1]
    uncertaintyData$endDateTime<-uncertaintyData$readout_time+timeDiff
    ucrtCol_inst <- c("startDateTime","endDateTime","temperature_ucrtExpn","pressure_ucrtExpn","elevation_ucrtExpn","conductivity_ucrtExpn")
    ucrtOut_inst <- uncertaintyData[,ucrtCol_inst]
    #standardize naming
    names(ucrtOut_inst)<- c("startDateTime","endDateTime","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
    
    #write out instantaneous uncertainty data
    #ucrtOut_inst[,-c(1:2)] <-round(ucrtOut_inst[,-c(1:2)],2)
    ucrtOut_inst$startDateTime<-as.POSIXct(ucrtOut_inst$startDateTime)
    ucrtOut_inst$endDateTime<-as.POSIXct(ucrtOut_inst$endDateTime)
    rptUcrtOut_Inst <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_inst, 
                                                          NameFile = base::paste0(idxDirOutUcrt,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_ucrt_005.parquet"), 
                                                          Schm = SchmUcrtOut),silent=FALSE)
    
    if(any(grepl('try-error',class(ucrtOut_inst)))){
      log$error(base::paste0('Writing the output data failed: ',attr(ucrtOut_inst,"condition")))
      stop()
    } else {
      log$info("Instantaneous uncertainty data written out.")
    }
  }
  
  
  ######## Uncertainty for L1 mean 5 and 30 minute outputs ########
  #the repeatability and reproducibility of the sensor and  uncertainty of the calibration procedures and coefficients including uncertainty in the standard
  if(length(WndwAgr)>0){
    #determine averaging window
    timeMeas <- base::as.POSIXlt(uncertaintyData$readout_time)# Pull out time variable
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      #idxWndwAgr<-1 #for testing
      log$debug(base::paste0('Computing uncertainty for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
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
      
      # Intialize the output
      rptUcrt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      if(sensor=="aquatroll200"){
        #Conductivity included for aqua troll.
        nameTerm <- c("temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1","conductivity_ucrtExpn_L1")
      }else{
        #Conductivity not included for level troll.
        nameTerm <- c("temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1")
      }
      rptUcrt[,3:(base::length(nameTerm)+2)] <- base::as.numeric(NA)
      base::names(rptUcrt)[3:(base::length(nameTerm)+2)] <- nameTerm
      
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
          #numPts <- base::sum(x=!base::is.na(dataWndwTime$temperature),na.rm=FALSE)
          #se <- stats::sd(dataWndwTime$temperature,na.rm=TRUE)/base::sqrt(numPts)
          #TemperatureExpUncert<-2*(se^2+U_CVALA3_temp^2)^0.5
          temperature_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='temperature',ucrtCoef=uncertaintyCoef)
          
          #Pressure Uncertainty
          #combined uncertainty of pressure is equal to the standard uncertainty values provided by CVAL
          #numPts <- base::sum(x=!base::is.na(dataWndwTime$pressure),na.rm=FALSE)
          #se <- stats::sd(dataWndwTime$pressure,na.rm=TRUE)/base::sqrt(numPts)
          #pressure_ucrtExpn_L1<-2*(se^2+U_CVALA3_pressure^2)^0.5
          pressure_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='pressure',ucrtCoef=uncertaintyCoef)
          pressure_ucrtComb_L1<-pressure_ucrtExpn_L1/2
          
          #Elevation Uncertainty
          #survey_uncert is the uncertainty of the sensor elevation relative to other aquatic instruments at the NEON site. 
          #survey_uncert includes the total station survey uncertainty and the uncertainty of hand measurements between the sensor and survey point.
          survey_uncert<-mean(dataWndwTime$survey_uncert)
          elevation_ucrtExpn_L1<-2*((1*survey_uncert^2+((1000/(density*gravity))^2)*pressure_ucrtComb_L1^2)^0.5)
          
          if(sensor=='aquatroll200'){
            #Raw Conductivity Uncertainty
            #combined uncertainty of actual conductivity (not published) is equal to the standard uncertainty values provided by CVAL
            #numPts <- base::sum(x=!base::is.na(dataWndwTime$raw_conductivity),na.rm=FALSE)
            #se <- stats::sd(dataWndwTime$raw_conductivity,na.rm=TRUE)/base::sqrt(numPts)
            #rawConductivity_ucrtExpn_L1<-2*(se^2+U_CVALA3_cond^2)^0.5
            rawConductivity_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='conductivity',ucrtCoef=uncertaintyCoef)
            
            #Specific Conductivity Uncertainty
            #grab U_CVALA3 values
            U_CVALA3_cond<-NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=uncertaintyCoef,
                                                                   NameCoef='U_CVALA3',
                                                                   VarUcrt='conductivity',
                                                                   TimeAgrBgn=dataWndwTime$readout_time[1],
                                                                   TimeAgrEnd=dataWndwTime$readout_time[base::nrow(dataWndwTime)]+as.difftime(.001,units='secs'))
            U_CVALA3_temp<-NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=uncertaintyCoef,
                                                                   NameCoef='U_CVALA3',
                                                                   VarUcrt='temperature',
                                                                   TimeAgrBgn=dataWndwTime$readout_time[1],
                                                                   TimeAgrEnd=dataWndwTime$readout_time[base::nrow(dataWndwTime)]+as.difftime(.001,units='secs'))
            
            # Compute uncertainty of the mean due to natural variation, represented by the standard error of the mean
            #log$debug(base::paste0('Computing L1 uncertainty due to natural variation (standard error)'))
            dataComp<-dataWndwTime$conductivity
            conductivity_ucrtComb_L1<-NA
            numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
            se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
            # Compute combined uncertainty for L1 specific conductivity
            for(i in 1:length(dataWndwTime$conductivity_ucrtMeas)){
              dataWndwTime$conductivity_ucrtComb_L1[i]<-((se^2)*(((1/(1+0.0191*(dataWndwTime$temperature[i]-25)))^2)*U_CVALA3_cond^2)+((((0.0191*dataWndwTime$raw_conductivity[i])/((1+0.0191*(dataWndwTime$temperature[i]-25))^2))^2)*U_CVALA3_temp^2))^0.5
            }
            # Compute expanded uncertainty for L1 specific conductivity
            conductivity_ucrtExpn_L1<-2*mean(dataWndwTime$conductivity_ucrtComb_L1)
          }
        }else{
          temperature_ucrtExpn_L1<-NA
          pressure_ucrtExpn_L1<-NA
          elevation_ucrtExpn_L1<-NA
          conductivity_ucrtExpn_L1<-NA
        }
        #copy info to output dataframe
        rptUcrt$temperature_ucrtExpn_L1[idxWndwTime] <- temperature_ucrtExpn_L1
        rptUcrt$pressure_ucrtExpn_L1[idxWndwTime] <- pressure_ucrtExpn_L1
        rptUcrt$elevation_ucrtExpn_L1[idxWndwTime] <- elevation_ucrtExpn_L1
        if(sensor=="aquatroll200"){
          rptUcrt$conductivity_ucrtExpn_L1[idxWndwTime] <- conductivity_ucrtExpn_L1
        }
      } # End loop through time windows
      
      
      #standardize column names
      if(Context=="GW"){
        names(rptUcrt)<- c("startDateTime","endDateTime","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
      }else if(sensor=="aquatroll200"){
        names(rptUcrt)<- c("startDateTime","endDateTime","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert","surfacewaterCondExpUncert")
      }else{
        names(rptUcrt)<- c("startDateTime","endDateTime","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert")
      }
      
      #Write out aggregate uncertainty data
      #rptUcrt[,-c(1:2)] <-round(rptUcrt[,-c(1:2)],2)
      if(WndwAgr[idxWndwAgr]==5){
        window<-"005"
      }else if(WndwAgr[idxWndwAgr]==30){
        window<-"030"
      }else{
        window<-NA
      }
      rptUcrtOut_Agr <- try(NEONprocIS.base::def.wrte.parq(data = rptUcrt, 
                                                           NameFile = base::paste0(idxDirOutUcrt,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_","ucrt_",window,".parquet"), 
                                                           Schm = SchmUcrtOut),silent=FALSE)
      
      if(any(grepl('try-error',class(rptUcrtOut_Agr)))){
        log$error(base::paste0('Writing the output data failed: ',attr(rptUcrtOut_Agr,"condition")))
        stop()
      } else {
        log$info("Averaged uncertainty data written out.")
      }
    }
  }
}






