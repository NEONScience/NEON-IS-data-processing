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
#' @param SchmStatsOut (optional) A json-formatted character string containing the schema for the output stats 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param SchmFlagsOut (optional) A json-formatted character string containing the schema for the output flags 
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
# DirIn <-"~/pfs/subs_test/2022/06/16/subsurf-moor-temp-cond_PRPO103501"
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
                                   SchmStatsOut=NULL,
                                   SchmFlagsOut=NULL,
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

  
  
  # --------- get water column ----------
  
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
  
  
  
  # --------- get troll location ----------
  
  ##### Read in location data #####
  trollLoc <- NULL
  trollFileLoc <- base::dir(DirInTrollLoc,full.names=TRUE)
  
  #Could be multiple source IDs in a day. Account for all.
  trollSources<-unique(trollData$source_id[!is.na(trollData$source_id)])
  
  #add troll height column
  trollData$trollHeight<-NA
  
  if(length(trollSources)>0){
    for(j in 1:length(trollSources)){
      trollSource_j<-trollSources[j]
      trollData_j<-trollData[!is.na(trollData$source_id) & trollData$source_id==trollSource_j,]
      #####if no "_locations" file then check that the data file is all NA's. If there is data, STOP. If NA's, move on.
      if(!is.null(trollFileLoc) & base::length(trollFileLoc)<1){
        #case where there are no files
        log$debug(base::paste0('No troll location data files in ',trollFileLoc, 'for source id ',trollSource_j))
        trollLocationHist <-NULL
      } else if(!is.null(trollFileLoc) & any(grepl(trollSource_j,trollFileLoc))){
        # Choose the _locations.json file
        trollLocationData <- base::paste0(trollFileLoc[grep(trollSource_j,trollFileLoc)])
        log$debug(base::paste0("location datum(s) found, reading in: ",trollLocationData))
        trollLocationHist <- NEONprocIS.base::def.loc.geo.hist(trollLocationData, log = NULL)
      } else { 
        #case where there is only a CFGLOC file
        trollLocationHist <-NULL
        log$debug(base::paste0('Location file is missing for ',DirInTroll))
        stop()
      }
      
      # Which location history matches each readout_time
      if(length(trollData_j$readout_time)>0){
        trollData_all_j<-NULL
        if(!is.null(trollLocationHist) & length(trollLocationHist$CFGLOC)>0){
          for(k in 1:length(trollLocationHist$CFGLOC)){
            startDate<-trollLocationHist$CFGLOC[[k]]$start_date
            endDate<-trollLocationHist$CFGLOC[[k]]$end_date
            trollData_subset<-trollData_j[trollData_j$readout_time>=startDate & trollData_j$readout_time<endDate,]
            if(is.null(trollLocationHist$CFGLOC[[k]]$z_offset)|is.na(trollLocationHist$CFGLOC[[k]]$z_offset)){
              trollData_subset$trollHeight<-0
            }else{
              trollData_subset$trollHeight<- trollLocationHist$CFGLOC[[k]]$z_offset
            }
            if(k==1){
              trollData_all_j<-trollData_subset
            }else{
              trollData_all_j<-rbind(trollData_all_j,trollData_subset)
            }
          }
          trollData_j<-trollData_all_j
        }
      }
      if(j==1){
        trollData_all<-trollData_j
      }else{
        trollData_all<-rbind(trollData_all,trollData_j)
      }
    }
    
    #add back in NA data
    trollNA<-trollData[is.na(trollData$source_id),]
    trollData<-rbind(trollData_all,trollNA)
    trollData<-trollData[order(trollData$readout_time),]
  }else{
    trollNA <-NULL
    trollData_all <- NULL
    trollLocationHist <-NULL
  }
  
  trollData$thermistorHeightQF <- 0
  trollData$thermistorHeightQF[trollData$trollHeight==0] <- 1
  trollData$thermistorHeightFinalQF <- trollData$thermistorHeightQF
  
  #Create dataframe for output instantaneous data
  waterCol <- c("readout_time","waterColumn","trollHeight","thermistorHeightQF","thermistorHeightFinalQF") 
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
    uESensor <- 0.01 #m
    #uncertainty of individual sensor depth measurements
    uDSensor <- (((1000/(density*gravity))^2 * pressure_ucrtMeas^2) + ((-1000/(density*gravity))^2 * baroPressure_ucrtMeas^2) + uESensor^2)^0.5
    depthUcrt$depth_ucrtMeas[n] <- uDSensor
    depthUcrt$depth_ucrtExpn[n] <- 2*uDSensor 
  }
  depthUcrt$thermistorHeight_ucrtExpn <- (uESensor^2 + uESensor^2)^0.5
  depthUcrt<-merge(depthUcrt,waterColumn,by='readout_time')
  depthUcrt$thermistorHeight_ucrtExpn[depthUcrt$thermistorHeightQF==1] <- NA
  ucrtOutDepth<-depthUcrt[,c("readout_time","depth_ucrtExpn","thermistorHeight_ucrtExpn")]
  
  
  
  
  # ----------- now loop through hobos -----------
  DirInHobo <- fs::path(DirIn,'hobou24')
  
  DirInHoboCFGLOC <-
    NEONprocIS.base::def.dir.in(DirBgn = DirInHobo,
                                nameDirSub = 'data',
                                log = log)
  
  doParallel::registerDoParallel(numCoreUse)
  foreach::foreach(hoboDir = DirInHoboCFGLOC) %dopar% {
    log$info(base::paste0('Processing path to hobo datum: ', hoboDir))
    
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
    thisHobo$thermistorHeightFromAnchor <- thisHobo$trollHeight + thisHobo$z_offset
    thisHobo$thermistorHeightFromAnchor[thisHobo$thermistorHeightQF==1] <- NA 
    
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
    hoboUcrt$specCond_ucrtMeas<-NA
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
    hoboUcrt<-merge(hoboUcrt,thisHobo,by='readout_time')
    hoboUcrt$specCond_ucrtExpn[is.na(hoboUcrt$conductivity)]<-NA
    hoboUcrt$temperature_ucrtExpn[is.na(hoboUcrt$temperature)]<-NA
    
    # create output directories
    DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
    DirOutHobo <- base::paste0(DirOut,'/hobou24/',CFGLOC)
    base::dir.create(DirOutHobo,recursive=TRUE)
    DirOutStats <- base::paste0(DirOutHobo,'/stats')
    base::dir.create(DirOutStats,recursive=TRUE)
    DirOutFlags <- base::paste0(DirOutHobo,'/flags')
    base::dir.create(DirOutFlags,recursive=TRUE)
    
    # Copy with a symbolic link the desired subfolders 
    DirSubCopy <- c('location')
    if(base::length(DirSubCopy) > 0){
      
      NEONprocIS.base::def.dir.copy.symb(DirSrc=DirInHoboLoc,
                                         DirDest=DirOutHobo,
                                         LnkSubObj=FALSE,
                                         log=log)
    } 
    
    #Create dataframe for output instantaneous data
    dataOut <- thisHobo
    dataCol <- c("startDateTime","endDateTime","temperature","conductivity","thermistorDepth","thermistorHeightFromAnchor")
    dataOut <- dataOut[,dataCol]
    dataOut$temperature<-round(dataOut$temperature,5)
    dataOut$conductivity<-round(dataOut$conductivity,5)
    dataOut$thermistorDepth<-round(dataOut$thermistorDepth,5)
    dataOut$thermistorHeightFromAnchor<-round(dataOut$thermistorHeightFromAnchor,5)
    
    
    #Create dataframe for output instantaneous flags
    flagsOut <- thisHobo
    flagsCol <- c("readout_time","thermistorHeightQF","thermistorHeightFinalQF")
    flagsOut <- flagsOut[,flagsCol]
    
    #Create dataframe for output instantaneous uncertainty data
    ucrtOut<-hoboUcrt[,c("readout_time","temperature_ucrtExpn","specCond_ucrtExpn")]
    ucrtOut <- merge(ucrtOut,ucrtOutDepth,by='readout_time')
    ucrtOut$startDateTime<-as.POSIXct(ucrtOut$readout_time)
    ucrtCol <- c("startDateTime","temperature_ucrtExpn","specCond_ucrtExpn","depth_ucrtExpn","thermistorHeight_ucrtExpn")
    ucrtOut <- ucrtOut[,ucrtCol]
    ucrtOut$temperature_ucrtExpn<-round(ucrtOut$temperature_ucrtExpn,5)
    ucrtOut$specCond_ucrtExpn<-round(ucrtOut$specCond_ucrtExpn,5)
    ucrtOut$depth_ucrtExpn<-round(ucrtOut$depth_ucrtExpn,5)
    ucrtOut$thermistorHeight_ucrtExpn<-round(ucrtOut$thermistorHeight_ucrtExpn,5)
    
    #merge data frames
    statsOut <- merge(dataOut,ucrtOut,by='startDateTime')
    statsOut$depth_ucrtExpn[is.na(statsOut$thermistorDepth)]<-NA
    statsOut$thermistorHeight_ucrtExpn[is.na(statsOut$thermistorHeightFromAnchor)]<-NA
    
    #Write out instantaneous stats
    NameFile <- base::paste0(DirOutStats,"/",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_030.parquet")
    rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = statsOut, 
                                                     NameFile = NameFile, 
                                                     Schm = SchmStatsOut),silent=TRUE)
    if(any(grepl('try-error',class(rptDataOut)))){
      log$error(base::paste0('Writing the output data failed for: ',NameFile,". ErrorCode: ",attr(rptDataOut,"condition")))
      stop()
    } else {
      log$info(base::paste0("Data written out for ",NameFile))
    }
    
    #Write out instantaneous flags
    NameFile <- base::paste0(DirOutFlags,"/",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_thermistorHeightQF.parquet")
    rptFlagsOut <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut, 
                                                     NameFile = NameFile, 
                                                     Schm = SchmFlagsOut),silent=TRUE)
    if(any(grepl('try-error',class(rptFlagsOut)))){
      log$error(base::paste0('Writing the output flags failed for: ',NameFile,". ErrorCode: ",attr(rptFlagsOut,"condition")))
      stop()
    } else {
      log$info(base::paste0("Flags written out for ",NameFile))
    }
    
  }
  
} # End loop around datum paths
  

  

  

