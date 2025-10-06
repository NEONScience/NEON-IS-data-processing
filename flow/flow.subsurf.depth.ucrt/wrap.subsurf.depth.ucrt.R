##############################################################################################
#' @title Subsurface moored thermistor depth and uncertainty science computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr

#' @description Wrapper function. Calculate sensor depth and uncertainty for the Subsurface moored temperature chain
#'
#'         
#' @param DirIn 
#' Character value. The input path to the data from a single group ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/group/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day.
#' 
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /flags
#'         /uncertainty_data
#'         
#' The data folder holds 1 data file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD.parquet
#' 
#' The location folder holds 2 location json files with the naming formats:
#' SOURCETYPE_SOURCEID_locations.json
#' CFGLOC.json
#' 
#' 
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional) A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param SchmUcrtOut (optional) A json-formatted character string containing the schema for the output uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the merged and filtered Kafka output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples 
#' # NOT RUN
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# DirIn <-"~/pfs/subsurfMoorTempCond_baro_conv/2022/06/12/subsurf-moor-temp-cond_PRPO103100"
#'wrap.subsurf.depth.ucrt <- function(DirIn=DirIn,
#'                               DirOutBase="~/pfs/out",
#'                               log=log)
#'                               
#' @seealso Currently none
#' 
# changelog and author contributions / copyrights
#   Nora Catolico (2025-10-03)
#     Initial creation
##############################################################################################
wrap.subsurf.depth.ucrt <- function(DirIn,
                                   DirOutBase,
                                   SchmDataOut=NULL,
                                   SchmUcrtOut=NULL,
                                   log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  

  # Gather info about the input directory (including date), and create base output directory

  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  
  DirInTroll <- fs::path(DirIn,'pressure')
  DirInTrollCFGLOC <-
    NEONprocIS.base::def.dir.in(DirBgn = DirInTroll,
                                nameDirSub = 'data',
                                log = log)
  DirInTrollData <- fs::path(DirInTrollCFGLOC,'data')
  DirInTrollFlags <- fs::path(DirInTrollCFGLOC,'flags')
  DirInTrollLoc <- fs::path(DirInTrollCFGLOC,'location')
  DirInTrollUcrt <- fs::path(DirInTrollCFGLOC,'uncertainty_data')

  
  
  # --------- get troll depth ----------
  
  # Take stock of our data files. 
  fileData <- base::list.files(DirInTrollData,full.names=FALSE)
  
  # Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
  fileData <- fileData[1]
  trollData  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInTrollData, '/', fileData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', DirInTrollData, '/', fileData, ' is unreadable.'))
    base::stop()
  }
  
  ###### values for computing water depth. Function of calibrated pressure, gravity, and density of water
  density <- 999  #m/s2 
  gravity <- 9.81  #kg/m3 
  
  #calculate water column height
  trollData$waterColumn<-1000*trollData$pressure/(density*gravity)
  
  #Create dataframe for output instantaneous data
  waterCol <- c("readout_time","waterColumn") 
  waterColumn <- trollData[,waterCol]
  
  
  # --------- get troll depth uncertainty ----------
  
  # Take stock of our files. 
  fileUcrtPressure <- base::list.files(DirInTrollUcrt,full.names=FALSE)
  
  # Load in file in parquet format into data frame
  fileUcrtTroll <- fileUcrtPressure[!grepl("baro",fileUcrtPressure)]
  fileUcrtBaro <- fileUcrtPressure[grepl("baro",fileUcrtPressure)]
  trollUcrt  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInTrollUcrt, '/', fileUcrtTroll),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', DirInTrollUcrt, '/', fileUcrtTroll, ' is unreadable.'))
    base::stop()
  }
  baroUcrt  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInTrollUcrt, '/', fileUcrtBaro),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', DirInTrollUcrt, '/', fileUcrtBaro, ' is unreadable.'))
    base::stop()
  }
  
  depthUcrt<-merge(trollUcrt,baroUcrt,by='readout_time')
  depthUcrt$startDateTime<-as.POSIXct(depthUcrt$readout_time)
  depthUcrt$endDateTime<-as.POSIXct(depthUcrt$readout_time)
  depthUcrt$depth_ucrtMeas <-NA
  depthUcrt$depth_ucrtExpn <-NA
  for (n in 1:length(depthUcrt$readout_time)){
    #equation 16
    baroPresExpUncert <- depthUcrt$baroPresExpUncert[n] 
    baroPressure_ucrtMeas <- baroPresExpUncert/2 #uA1air
    pressure_ucrtMeas <- depthUcrt$pressure_ucrtMeas[n]#uA1wat
    uESensor <- 0.1 #kPa ~0.01m
    #uncertainty of individual sensor depth measurements
    uDSensor <- (((1000/(density*gravity))^2 * pressure_ucrtMeas^2) + ((-1000/(density*gravity))^2 * baroPressure_ucrtMeas^2) + uESensor^2)^0.5
    depthUcrt$depth_ucrtMeas[n] <- uDSensor
    depthUcrt$depth_ucrtExpn[n] <- 2*uDSensor 
  }
  ucrtOutDepth<-depthUcrt[,c("readout_time","depth_ucrtExpn")]
  
  
  
  # ----------- now loop through hobos -----------
  DirInHobo <- fs::path(DirIn,'hobou24')
  
  DirInHoboCFGLOC <-
    NEONprocIS.base::def.dir.in(DirBgn = DirInHobo,
                                nameDirSub = 'data',
                                log = log)
  
  doParallel::registerDoParallel(numCoreUse)
  foreach::foreach(hoboDir = DirInHoboCFGLOC) %dopar% {
    log$info(base::paste0('Processing path to datum: ', hoboDir))
    
    DirInHoboData <- fs::path(hoboDir,'data')
    DirInHoboFlags <- fs::path(hoboDir,'flags')
    DirInHoboLoc <- fs::path(hoboDir,'location')
    DirInHoboUcrt <- fs::path(hoboDir,'uncertainty_data')
    
    
    # --------- determine thermistor depth ------------
    
    # Take stock of our data files. 
    fileDataHobo <- base::list.files(DirInHoboData,full.names=FALSE)
    
    # Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
    fileDataHobo <- fileDataHobo[1]
    hoboData  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInHoboData, '/', fileDataHobo),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', DirInHoboData, '/', fileDataHobo, ' is unreadable.'))
      base::stop()
    }
    
    #add in columns that will be calculated
    hoboData$startDateTime<-as.POSIXct(hoboData$readout_time)
    hoboData$endDateTime<-as.POSIXct(hoboData$readout_time)
    hoboData$z_offset<-NA
    
    #incorporate location data
    fileOutSplt <- base::strsplit(hoboDir,'[/]')[[1]] # Separate underscore-delimited components of the file name
    CFGLOC<-tail(x=fileOutSplt,n=1)
    
    ##### Read in location data #####
    LocationData <- NULL
    dirLocLocation <- base::dir(DirInHoboLoc,full.names=TRUE)
    
    #Could be multiple source IDs in a day. Account for all.
    sources<-unique(hoboData$source_id[!is.na(hoboData$source_id)])
    
    if(length(sources)>0){
      for(n in 1:length(sources)){
        source_n<-sources[n]
        hoboData_n<-hoboData[!is.na(hoboData$source_id) & hoboData$source_id==source_n,]
        #####if no "_locations" file then check that the data file is all NA's. If there is data, STOP. If NA's, move on.
        if(!is.null(dirLocLocation) & base::length(dirLocLocation)<1){
          #case where there are no files
          log$debug(base::paste0('No hobo location data files in ',dirLocation, 'for source id ',source_n))
          LocationHist <-NULL
        } else if(!is.null(dirLocLocation) & any(grepl(source_n,dirLocLocation))){
          # Choose the _locations.json file
          LocationData <- base::paste0(dirLocLocation[grep(source_n,dirLocLocation)])
          log$debug(base::paste0("location datum(s) found, reading in: ",LocationData))
          LocationHist <- NEONprocIS.base::def.loc.geo.hist(LocationData, log = NULL)
        } else { 
          #case where there is only a CFGLOC file
          LocationHist <-NULL
          log$debug(base::paste0('Location file is missing for ',dirHobo))
          stop()
        }
        
        # Which location history matches each readout_time
        if(length(hoboData_n$readout_time)>0){
          hoboData_all_n<-NULL
          if(!is.null(LocationHist) & length(LocationHist$CFGLOC)>0){
            for(i in 1:length(LocationHist$CFGLOC)){
              startDate<-LocationHist$CFGLOC[[i]]$start_date
              endDate<-LocationHist$CFGLOC[[i]]$end_date
              hoboData_subset<-hoboData_n[hoboData_n$readout_time>=startDate & hoboData_n$readout_time<endDate,]
              if(is.null(LocationHist$CFGLOC[[i]]$z_offset)|is.na(LocationHist$CFGLOC[[i]]$z_offset)){
                hoboData_subset$z_offset<-0
              }else{
                hoboData_subset$z_offset<- LocationHist$CFGLOC[[i]]$z_offset
              }
              if(i==1){
                hoboData_all_n<-hoboData_subset
              }else{
                hoboData_all_n<-rbind(hoboData_all_n,hoboData_subset)
              }
            }
            hoboData_n<-hoboData_all_n
          }
        }
        if(n==1){
          hoboData_all<-hoboData_n
        }else{
          hoboData_all<-rbind(hoboData_all,hoboData_n)
        }
      }
      
      #add back in NA data
      hoboNA<-hoboData[is.na(hoboData$source_id),]
      hoboData<-rbind(hoboData_all,hoboNA)
      hoboData<-hoboData[order(hoboData$readout_time),]
    }else{
      hoboNA <-NULL
      hoboData_all <- NULL
      LocationHist <-NULL
    }
    
    
    # merge in troll data
    thisHobo <- merge(hoboData,waterColumn,by='readout_time')
    thisHobo$thermistorDepth <- thisHobo$waterColumn - thisHobo$z_offset
    
    
    
    # --------- determine conductivity and temperature uncertainty ------------
    
    # Take stock of our files. 
    fileUcrtHobo <- base::list.files(DirInHoboUcrt,full.names=FALSE)
    hoboUcrt  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInHoboUcrt, '/', fileUcrtHobo),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', DirInHoboUcrt, '/', fileUcrtHobo, ' is unreadable.'))
      base::stop()
    }
    
    #### calculate conductivity uncertainty
    high_or_low<-thisHobo[,c("readout_time","high_or_low")]
    hoboUcrt<-merge(hoboUcrt,high_or_low, by="readout_time")
    
    #choose high or low
    hoboUcrt$rawConductivity_ucrtMeas<-NA
    hoboUcrt$rawConductivity_ucrtMeas[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']<-hoboUcrt$conductivity_low_ucrtMeas[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']
    hoboUcrt$rawConductivity_ucrtMeas[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']<-hoboUcrt$conductivity_high_ucrtMeas[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']
    hoboUcrt$rawConductivity_ucrtComb<-NA
    hoboUcrt$rawConductivity_ucrtComb[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']<-hoboUcrt$conductivity_low_ucrtComb[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']
    hoboUcrt$rawConductivity_ucrtComb[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']<-hoboUcrt$conductivity_high_ucrtComb[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']
    hoboUcrt$rawConductivity_ucrtExpn<-NA
    hoboUcrt$rawConductivity_ucrtExpn[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']<-hoboUcrt$conductivity_low_ucrtExpn[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'low']
    hoboUcrt$rawConductivity_ucrtExpn[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']<-hoboUcrt$conductivity_high_ucrtExpn[!is.na(hoboUcrt$high_or_low) & hoboUcrt$high_or_low == 'high']
    hoboUcrt$specCond_ucrtMeas-NA
    hoboUcrt$specCond_ucrtComb<-NA
    hoboUcrt$specCond_ucrtExpn<-NA
    
    #check that data frames are equal length
    if(length(hoboUcrt$specCond_ucrtMeas)!=length(thisHobo$raw_conductivity)){
      log$warn("Conductivity data and uncertainty data frames are not of equal length")
      stop()
    }
    if(length(hoboUcrt$temperature_ucrtMeas)!=length(thisHobo$temperature)){
      log$warn("Temperature data and uncertainty data frames are not of equal length")
      stop()
    }
    #calculate uncertainty for specific conductivity
    for(i in 1:length(hoboUcrt$rawConductivity_ucrtMeas)){
      U_CVALA1_cond<-hoboUcrt$rawConductivity_ucrtMeas[i]
      U_CVALA1_temp<-hoboUcrt$temperature_ucrtMeas[i]
      denominator <- 1+0.0191*(thisHobo$temperature[i]-25)
      uA1C2<-(U_CVALA1_cond*thisHobo$raw_conductivity[i])^2
      uA1T2<-U_CVALA1_temp^2
      #uncertainty for individual specific conductivity measurements eq. 11
      hoboUcrt$specCond_ucrtMeas[i]<-(
        ((1/denominator)^2)*(uA1C2)+
          ((((0.0191*thisHobo$raw_conductivity[i])/(denominator^2))^2)*uA1T2)
      )^0.5
    }
    hoboUcrt$specCond_ucrtComb<-hoboUcrt$specCond_ucrtMeas
    hoboUcrt$specCond_ucrtExpn<-2*hoboUcrt$specCond_ucrtMeas
    ucrtOut<-hoboUcrt[,c("readout_time","temperature_ucrtExpn","specCond_ucrtExpn")]
    
    
    # create output directories
    DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
    DirOutHobo <- base::paste0(DirOut,'/hobou24/',CFGLOC)
    base::dir.create(DirOutHobo,recursive=TRUE)
    DirOutData <- base::paste0(DirOutHobo,'/data')
    base::dir.create(DirOutData,recursive=TRUE)
    DirOutUcrt <- base::paste0(DirOutHobo,'/uncertainty_data')
    base::dir.create(DirOutUcrt,recursive=TRUE)
    
    #Create dataframe for output instantaneous data
    dataOut <- thisHobo
    dataCol <- c("startDateTime","endDateTime","temperature","conductivity","thermistorDepth")
    dataOut <- dataOut[,dataCol]
    
    #Write out instantaneous data
    NameFile <- base::paste0(DirOutData,"/",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_001.parquet")
    rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                     NameFile = NameFile, 
                                                     Schm = SchmDataOut),silent=TRUE)
    if(any(grepl('try-error',class(rptDataOut)))){
      log$error(base::paste0('Writing the output data failed for: ',NameFile,". ErrorCode: ",attr(rptDataOut,"condition")))
      stop()
    } else {
      log$info(base::paste0("Data written out for ",NameFile))
    }
    
    #Create dataframe for output instantaneous uncertainty data
    ucrtOut <- merge(ucrtOut,ucrtOutDepth,by='readout_time')
    ucrtOut$startDateTime<-as.POSIXct(ucrtOut$readout_time)
    ucrtOut$endDateTime<-as.POSIXct(ucrtOut$readout_time)
    ucrtCol <- c("startDateTime","endDateTime","depth_ucrtExpn","temperature_ucrtExpn","specCond_ucrtExpn")
    ucrtOut <- ucrtOut[,ucrtCol]
    
    
    #Write out instantaneous Ucrt
    NameFileUcrt <- base::paste0(DirOutUcrt,"/",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_ucrt_001.parquet")
    rptUcrtOut <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut, 
                                                     NameFile = NameFileUcrt, 
                                                     Schm = SchmUcrtOut),silent=TRUE)
    if(any(grepl('try-error',class(rptUcrtOut)))){
      log$error(base::paste0('Writing the output failed for: ',NameFile,". ErrorCode: ",attr(rptUcrtOut,"condition")))
      stop()
    } else {
      log$info(base::paste0("Ucrt file written out for ",NameFileUcrt))
    }

    
  }
  
} # End loop around datum paths
  

  

  

