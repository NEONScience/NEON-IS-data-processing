##############################################################################################
#' @title Level Troll 500 and Aqua Troll 200 Science Computations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr

#' @description Wrapper function. Calculate elevation and derive uncertainty for surface and groundwater troll data products.
#'

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#' 
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' The data folder holds 1 data file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD.parquet
#' 
#' The location folder holds 2 location json files with the naming formats:
#' SOURCETYPE_SOURCEID_locations.json
#' CFGLOC.json
#' 
#' The uncertainty_coef folder holds 1 file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyData.parquet
#' 
#' The uncertainty_data folder holds 1 file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyCoef.json
#' 
#' 
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param Context String. Required. The value must be designated as either "SW" or "GW" for surface or groundwater.
#' 
#' @param WndwAgr (optional) where value is the aggregation interval for which to compute uncertainty. 
#' Formatted as a 3 character sequence, typically representing the number of minutes over which to compute uncertainty 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals. No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param WndwInst (optional) set to TRUE to include instantaneous uncertainty data output. The defualt value is FALSE. 
#' No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param SchmDataOut String. Optional. Full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmUcrtOut String. Optional. Full path to the avro schema for the output uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmSciStatsOut String. Optional. Full path to the avro schema for the output science statistics
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

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.flags <- function(DirIn="~/pfs/groundwaterPhysical_analyze_pad_and_qaqc_plau/2020/02/02",
#'                               DirOutBase="~/pfs/out",
#'                               Context='GW',
#'                               WndwInst=TRUE,
#'                               WndwAgr='030',
#'                               SchmDataOut=NULL,
#'                               SchmUcrtOut=NULL,
#'                               SchmSciStatsOut=NULL,
#'                               log=log)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Nora Catolico (2023-10-03)
#     Initial creation
##############################################################################################
wrap.troll.uncertainty <- function(DirIn,
                                   DirOutBase,
                                   Context,
                                   WndwInst,
                                   WndwAgr,
                                   SchmDataOut=NULL,
                                   SchmUcrtOut=NULL,
                                   SchmSciStatsOut=NULL,
                                   log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  

  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  dirInData <- fs::path(DirIn,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutSciStats <- base::paste0(DirOut,'/sci_stats')
  base::dir.create(DirOutSciStats,recursive=TRUE)
  DirOutUcrt <- base::paste0(DirOut,'/uncertainty_data')
  base::dir.create(DirOutUcrt,recursive=TRUE)
  

  # Take stock of our data files. 
  fileData <- base::list.files(dirInData,full.names=FALSE)
  
  # --------- Load the data ----------
  # Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
  fileData <- fileData[1]
  trollData  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInData, '/', fileData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', dirData, '/', fileData, ' is unreadable.'))
    base::stop()
  }
  
  #add in columns that will be calculated
  trollData$startDateTime<-as.POSIXct(trollData$readout_time)
  trollData$endDateTime<-as.POSIXct(trollData$readout_time)
  trollData$sensorElevation<-NA
  trollData$z_offset<-NA
  trollData$survey_uncert<-NA
  trollData$real_world_uncert<-NA
  trollData$waterColumn<-NA
  trollData$elevation<-NA
  ###### values for computing water table elevation. Function of calibrated pressure, gravity, and density of water
  density <- 999  #m/s2 #future mod: temperature corrected density; conductivity correct density
  gravity <- 9.81  #kg/m3 #future mod: site specific gravity
  
  #incorporate location data
  fileOutSplt <- base::strsplit(DirIn,'[/]')[[1]] # Separate underscore-delimited components of the file name
  CFGLOC<-tail(x=fileOutSplt,n=1)
  
  ##### Read in location data #####
  LocationData <- NULL
  dirLocation <- base::paste0(DirIn,'/location')
  dirLocLocation <- base::dir(dirLocation,full.names=TRUE)
  
  #Could be multiple source IDs in a day. Account for all.
  sources<-unique(trollData$source_id[!is.na(trollData$source_id)])
  
  if(length(sources)>0){
    for(n in 1:length(sources)){
      source_n<-sources[n]
      trollData_n<-trollData[!is.na(trollData$source_id) & trollData$source_id==source_n,]
      #####if no "_locations" file then check that the data file is all NA's. If there is data, STOP. If NA's, move on.
      if(!is.null(dirLocLocation) & base::length(dirLocLocation)<1){
        #case where there are no files
        log$debug(base::paste0('No troll location data files in ',dirLocation, 'for source id ',source_n))
      } else if(!is.null(dirLocLocation) & any(grepl(source_n,dirLocLocation))){
        # Choose the _locations.json file
        LocationData <- base::paste0(dirLocLocation[grep(source_n,dirLocLocation)])
        log$debug(base::paste0("location datum(s) found, reading in: ",LocationData))
        LocationHist <- NEONprocIS.base::def.loc.geo.hist(LocationData, log = NULL)
      } else { 
        #case where there is only a CFGLOC file
        LocationHist <-NULL
        if(length(unique(trollData$pressure))<=1 & is.na(unique(trollData$pressure)[1])){
          #check that the data file is indeed empty then move on to next datum
          log$debug(base::paste0('Troll data file is empty in',dirTroll, '. Moving on to next datum.'))
        }else{
          log$debug(base::paste0('Data exists in troll data file. Location file is missing for ',dirTroll))
          stop()
        }
      }
      
      # Which location history matches each readout_time
      if(length(trollData_n$readout_time)>0){
        trollData_all_n<-NULL
        if(!is.null(LocationHist) & length(LocationHist$CFGLOC)>0){
          for(i in 1:length(LocationHist$CFGLOC)){
            startDate<-LocationHist$CFGLOC[[i]]$start_date
            endDate<-LocationHist$CFGLOC[[i]]$end_date
            trollData_subset<-trollData_n[trollData_n$readout_time>=startDate & trollData_n$readout_time<endDate,]
            trollData_subset$sensorElevation<- LocationHist$CFGLOC[[i]]$geometry$coordinates[3]
            trollData_subset$z_offset<- LocationHist$CFGLOC[[i]]$z_offset
            trollData_subset$survey_uncert <- LocationHist$CFGLOC[[i]]$`Survey vertical uncertainty` #includes survey uncertainty and hand measurements
            trollData_subset$real_world_uncert <-LocationHist$CFGLOC[[i]]$`Real world coordinate uncertainty`  
            if(i==1){
              trollData_all_n<-trollData_subset
            }else{
              trollData_all_n<-rbind(trollData_all_n,trollData_subset)
            }
          }
          trollData_n<-trollData_all_n
          #calculate water column height
          trollData_n$waterColumn<-1000*trollData_n$pressure/(density*gravity)
          #calculate water table elevation
          trollData_n$elevation<-trollData_n$sensorElevation+trollData_n$z_offset+trollData_n$waterColumn
        }
      }
      
      if(n==1){
        trollData_all<-trollData_n
      }else{
        trollData_all<-rbind(trollData_all,trollData_n)
      }
    }
    
    #add back in NA data
    trollNA<-trollData[is.na(trollData$source_id),]
    trollData<-rbind(trollData_all,trollNA)
    trollData<-trollData[order(trollData$readout_time),]
  }else{
    trollNA <-NULL
    trollData_all <- NULL
    LocationHist <-NULL
  }
  
  #Define troll type. Needed for including conductivity based on sensor type
  if(grepl("aquatroll",DirIn)){
    sensor<-"aquatroll200"
  }else{
    sensor<-"leveltroll500"
  }
  
  #Create dataframe for output instantaneous data
  dataOut <- trollData
  if(sensor=="aquatroll200"){
    dataCol <- c("startDateTime","endDateTime","pressure","temperature","conductivity","waterColumn","elevation")
  }else{
    dataCol <- c("startDateTime","endDateTime","pressure","temperature","waterColumn","elevation") 
  }
  dataOut <- dataOut[,dataCol]
  
  #Write out instantaneous data
  if(Context=='GW'){
    rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                     NameFile = base::paste0(DirOutData,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_005.parquet"), 
                                                     Schm = SchmDataOut),silent=FALSE)
  }else{
    rptDataOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, 
                                                     NameFile = base::paste0(DirOutData,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_001.parquet"), 
                                                     Schm = SchmDataOut),silent=FALSE)
  }
  
  if(any(grepl('try-error',class(rptDataOut)))){
    log$error(base::paste0('Writing the output data failed: ',attr(rptDataOut,"condition")))
    stop()
  } else {
    log$info("Data written out.")
  }
  
  ### Read in flags
  flags <- NULL
  dirFlags <- base::paste0(DirIn,'/flags')
  dirFlagsLocation <- base::dir(dirFlags,full.names=TRUE)
  dirFlagsLocation<-dirFlagsLocation[grepl('flagsPlausibility',dirFlagsLocation)]
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
  
  #####Calculate 5&30-min average troll data mean while excluding points that fail the spike test. Output in Sci_stats.
  if(length(WndwAgr)>0){
    #determine averaging window
    timeMeas <- base::as.POSIXlt(flagData$readout_time)# Pull out time variable
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
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
                                                           NameFile = base::paste0(DirOutSciStats,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_sciStats_",window,".parquet"), 
                                                           Schm = SchmSciStatsOut),silent=FALSE)
      
      if(any(grepl('try-error',class(rptSciStatsOut)))){
        log$error(base::paste0('Writing the output data failed: ',attr(rptSciStatsOut,"condition")))
        stop()
      } else {
        log$info("Science stats written out.")
      }
    }
  }
  
  # ##### Read in uncertainty data #####
  # uncertaintyData <- NULL
  # dirUncertainty <- base::paste0(DirIn,'/uncertainty_data')
  # dirUncertaintyLocation <- base::dir(dirUncertainty,full.names=TRUE)
  # if(base::length(dirUncertaintyLocation)<1){
  #   log$debug(base::paste0('No troll uncertainty data file in ',dirUncertainty))
  # } else{
  #   uncertaintyData <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirUncertaintyLocation),log = log), silent = FALSE)
  #   log$debug(base::paste0("Reading in: ",dirUncertaintyLocation))
  # }
  # 
  # ##### Read in uncertainty coef #####
  # uncertaintyCoef <- NULL
  # dirUncertaintyCoef <- base::paste0(DirIn,'/uncertainty_coef')
  # dirUncertaintyCoefLocation <- base::dir(dirUncertaintyCoef,full.names=TRUE)
  # if(base::length(dirUncertaintyCoefLocation)<1){
  #   log$debug(base::paste0('No troll uncertainty data file in ',dirUncertaintyCoef))
  # } else{
  #   uncertaintyCoef <- base::try(rjson::fromJSON(file=dirUncertaintyCoefLocation,simplify=TRUE),silent=FALSE)
  #   if(base::class(uncertaintyCoef) == 'try-error'){
  #     # Generate error and stop execution
  #     log$error(base::paste0('File: ', dirUncertaintyCoefLocation, ' is unreadable.')) 
  #     stop()
  #   }
  #   if(base::length(uncertaintyCoef)>0){
  #     # Turn times to POSIX
  #     uncertaintyCoef <- base::lapply(uncertaintyCoef,FUN=function(Ucrt){
  #       Ucrt$start_date <- base::strptime(Ucrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
  #       Ucrt$end_date <- base::strptime(Ucrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
  #       return(Ucrt)
  #     })
  #     log$debug(base::paste0("Reading in: ",dirUncertaintyCoefLocation))
  #   }
  # }
  # 
  # ######## Uncert for instantaneous 5-min groundwater aqua troll data #######
  # if(WndwInst==TRUE){
  #   if(sensor=="aquatroll200"){
  #     #temp and pressure uncert calculated earlier in pipeline
  #     #existing conductivity uncertainty is for raw values, need additional columns for specific conductivity
  #     uncertaintyData$rawConductivity_ucrtMeas<-uncertaintyData$conductivity_ucrtMeas
  #     uncertaintyData$conductivity_ucrtMeas<-NA
  #     uncertaintyData$rawConductivity_ucrtComb<-uncertaintyData$conductivity_ucrtComb
  #     uncertaintyData$conductivity_ucrtComb<-NA
  #     uncertaintyData$rawConductivity_ucrtExpn<-uncertaintyData$conductivity_ucrtExpn
  #     uncertaintyData$conductivity_ucrtExpn<-NA
  #     #check that data frames are equal length
  #     if(length(uncertaintyData$conductivity_ucrtMeas)!=length(trollData$raw_conductivity)){
  #       log$warn("Conductivity data and uncertainty data frames are not of equal length")
  #       stop()
  #     }
  #     if(length(uncertaintyData$temperature_ucrtMeas)!=length(trollData$temperature)){
  #       log$warn("Temperature data and uncertainty data frames are not of equal length")
  #       stop()
  #     }
  #     #calculate uncertainty for specific conductivity
  #     for(i in 1:length(uncertaintyData$rawConductivity_ucrtMeas)){
  #       U_CVALA1_cond<-uncertaintyData$rawConductivity_ucrtMeas[i]
  #       U_CVALA1_temp<-uncertaintyData$temperature_ucrtMeas[i]
  #       uncertaintyData$conductivity_ucrtMeas[i]<-((((1/(1+0.0191*(trollData$temperature[i]-25)))^2)*U_CVALA1_cond^2)+((((0.0191*trollData$raw_conductivity[i])/((1+0.0191*(trollData$temperature[i]-25))^2))^2)*U_CVALA1_temp^2))^0.5
  #     }
  #     uncertaintyData$conductivity_ucrtComb<-uncertaintyData$conductivity_ucrtMeas
  #     uncertaintyData$conductivity_ucrtExpn<-2*uncertaintyData$conductivity_ucrtMeas
  #   }
  #   #calculate instantaneous elevation uncert
  #   uncertaintyData$elevation_ucrtMeas<-NA
  #   if(!is.null(LocationHist) & length(LocationHist)>0){
  #     for(i in 1:length(uncertaintyData$pressure_ucrtMeas)){
  #       U_CVALA1_pressure<-uncertaintyData$pressure_ucrtMeas[i]
  #       uncertaintyData$elevation_ucrtMeas[i]<-(1*trollData$survey_uncert[i]^2+((1000/(density*gravity))^2)*U_CVALA1_pressure^2)^0.5
  #     }
  #   }else{
  #     uncertaintyData$elevation_ucrtMeas<-NA
  #   }
  #   uncertaintyData$elevation_ucrtComb<-uncertaintyData$elevation_ucrtMeas
  #   uncertaintyData$elevation_ucrtExpn<-2*uncertaintyData$elevation_ucrtMeas
  #   
  #   #Create dataframes for output uncertainties
  #   uncertaintyData$startDateTime<-uncertaintyData$readout_time
  #   timeDiff<-uncertaintyData$startDateTime[2]-uncertaintyData$startDateTime[1]
  #   uncertaintyData$endDateTime<-uncertaintyData$readout_time+timeDiff
  #   if(sensor=='aquatroll200'){
  #     ucrtCol_inst <- c("startDateTime","endDateTime","temperature_ucrtExpn","pressure_ucrtExpn","elevation_ucrtExpn","conductivity_ucrtExpn")
  #   }else{
  #     ucrtCol_inst <- c("startDateTime","endDateTime","temperature_ucrtExpn","pressure_ucrtExpn","elevation_ucrtExpn")
  #   }
  #   ucrtOut_inst <- uncertaintyData[,ucrtCol_inst]
  #   #standardize naming
  #   if(sensor=='aquatroll200'){
  #     names(ucrtOut_inst)<- c("startDateTime","endDateTime","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
  #   }else{
  #     names(ucrtOut_inst)<- c("startDateTime","endDateTime","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert")
  #   }
  #   #write out instantaneous uncertainty data
  #   #ucrtOut_inst[,-c(1:2)] <-round(ucrtOut_inst[,-c(1:2)],2)
  #   ucrtOut_inst$startDateTime<-as.POSIXct(ucrtOut_inst$startDateTime)
  #   ucrtOut_inst$endDateTime<-as.POSIXct(ucrtOut_inst$endDateTime)
  #   #GW inst is 5-min, SW inst is 1-min
  #   if(Context=='GW'){
  #     rptUcrtOut_Inst <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_inst, 
  #                                                           NameFile = base::paste0(DirOutUcrt,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_ucrt_005.parquet"), 
  #                                                           Schm = SchmUcrtOut),silent=FALSE)
  #   }else{
  #     rptUcrtOut_Inst <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut_inst, 
  #                                                           NameFile = base::paste0(DirOutUcrt,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_ucrt_001.parquet"), 
  #                                                           Schm = SchmUcrtOut),silent=FALSE)
  #   }
  #   
  #   
  #   if(any(grepl('try-error',class(ucrtOut_inst)))){
  #     log$error(base::paste0('Writing the output data failed: ',attr(ucrtOut_inst,"condition")))
  #     stop()
  #   } else {
  #     log$info("Instantaneous uncertainty data written out.")
  #   }
  # }
  # 
  # 
  # ######## Uncertainty for L1 mean 5 and 30 minute outputs ########
  # #the repeatability and reproducibility of the sensor and  uncertainty of the calibration procedures and coefficients including uncertainty in the standard
  # if(length(WndwAgr)>0){
  #   #determine averaging window
  #   timeMeas <- base::as.POSIXlt(uncertaintyData$readout_time)# Pull out time variable
  #   # Run through each aggregation interval, creating the daily time series of windows
  #   for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  #     #WndwAgr<-1 #for testing
  #     log$debug(base::paste0('Computing uncertainty for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
  #     
  #     # Create start and end time sequences
  #     timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
  #     timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
  #     timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
  #     
  #     # Allocate data points to aggregation windows
  #     setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
  #     
  #     # Allocate uncertainty data points to aggregation windows
  #     if(!base::is.null(uncertaintyData)){
  #       setTimeUcrt <- base::.bincode(base::as.numeric(base::as.POSIXlt(uncertaintyData$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
  #     } else {
  #       setTimeUcrt <- base::numeric(0)
  #     }
  #     
  #     # Intialize the output
  #     rptUcrt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
  #     if(sensor=="aquatroll200"){
  #       #Conductivity included for aqua troll.
  #       nameTerm <- c("temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1","conductivity_ucrtExpn_L1")
  #     }else{
  #       #Conductivity not included for level troll.
  #       nameTerm <- c("temperature_ucrtExpn_L1","pressure_ucrtExpn_L1","elevation_ucrtExpn_L1")
  #     }
  #     rptUcrt[,3:(base::length(nameTerm)+2)] <- base::as.numeric(NA)
  #     base::names(rptUcrt)[3:(base::length(nameTerm)+2)] <- nameTerm
  #     
  #     # Run through the time bins
  #     for(WndwTime in base::unique(setTime)){
  #       # Rows to pull
  #       dataWndwTime <- base::subset(trollData,subset=setTime==WndwTime) 
  #       ucrtDataWndwTime <- base::subset(uncertaintyData,subset=setTime==WndwTime)  
  #       
  #       # Compute L1 uncertainty 
  #       if(length(uncertaintyCoef)>0){
  #         #Temperature Uncertainty
  #         #combined uncertainty of temperature is equal to the standard uncertainty values provided by CVAL
  #         #numPts <- base::sum(x=!base::is.na(dataWndwTime$temperature),na.rm=FALSE)
  #         #se <- stats::sd(dataWndwTime$temperature,na.rm=TRUE)/base::sqrt(numPts)
  #         #TemperatureExpUncert<-2*(se^2+U_CVALA3_temp^2)^0.5
  #         temperature_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='temperature',ucrtCoef=uncertaintyCoef)
  #         
  #         #Pressure Uncertainty
  #         #combined uncertainty of pressure is equal to the standard uncertainty values provided by CVAL
  #         #numPts <- base::sum(x=!base::is.na(dataWndwTime$pressure),na.rm=FALSE)
  #         #se <- stats::sd(dataWndwTime$pressure,na.rm=TRUE)/base::sqrt(numPts)
  #         #pressure_ucrtExpn_L1<-2*(se^2+U_CVALA3_pressure^2)^0.5
  #         pressure_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='pressure',ucrtCoef=uncertaintyCoef)
  #         pressure_ucrtComb_L1<-pressure_ucrtExpn_L1/2
  #         
  #         #Elevation Uncertainty
  #         #survey_uncert is the uncertainty of the sensor elevation relative to other aquatic instruments at the NEON site. 
  #         #survey_uncert includes the total station survey uncertainty and the uncertainty of hand measurements between the sensor and survey point.
  #         survey_uncert<-mean(dataWndwTime$survey_uncert)
  #         elevation_ucrtExpn_L1<-2*((1*survey_uncert^2+((1000/(density*gravity))^2)*pressure_ucrtComb_L1^2)^0.5)
  #         
  #         if(sensor=='aquatroll200'){
  #           #Raw Conductivity Uncertainty
  #           #combined uncertainty of actual conductivity (not published) is equal to the standard uncertainty values provided by CVAL
  #           #numPts <- base::sum(x=!base::is.na(dataWndwTime$raw_conductivity),na.rm=FALSE)
  #           #se <- stats::sd(dataWndwTime$raw_conductivity,na.rm=TRUE)/base::sqrt(numPts)
  #           #rawConductivity_ucrtExpn_L1<-2*(se^2+U_CVALA3_cond^2)^0.5
  #           rawConductivity_ucrtExpn_L1<-NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=dataWndwTime,VarUcrt='conductivity',ucrtCoef=uncertaintyCoef)
  #           
  #           #Specific Conductivity Uncertainty
  #           #grab U_CVALA3 values
  #           U_CVALA3_cond<-NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=uncertaintyCoef,
  #                                                                  NameCoef='U_CVALA3',
  #                                                                  VarUcrt='conductivity',
  #                                                                  TimeAgrBgn=dataWndwTime$readout_time[1],
  #                                                                  TimeAgrEnd=dataWndwTime$readout_time[base::nrow(dataWndwTime)]+as.difftime(.001,units='secs'))
  #           U_CVALA3_temp<-NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=uncertaintyCoef,
  #                                                                  NameCoef='U_CVALA3',
  #                                                                  VarUcrt='temperature',
  #                                                                  TimeAgrBgn=dataWndwTime$readout_time[1],
  #                                                                  TimeAgrEnd=dataWndwTime$readout_time[base::nrow(dataWndwTime)]+as.difftime(.001,units='secs'))
  #           
  #           # Compute uncertainty of the mean due to natural variation, represented by the standard error of the mean
  #           #log$debug(base::paste0('Computing L1 uncertainty due to natural variation (standard error)'))
  #           dataComp<-dataWndwTime$conductivity
  #           conductivity_ucrtComb_L1<-NA
  #           numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
  #           se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
  #           # Compute combined uncertainty for L1 specific conductivity
  #           for(i in 1:length(dataWndwTime$conductivity_ucrtMeas)){
  #             dataWndwTime$conductivity_ucrtComb_L1[i]<-((se^2)*(((1/(1+0.0191*(dataWndwTime$temperature[i]-25)))^2)*U_CVALA3_cond^2)+((((0.0191*dataWndwTime$raw_conductivity[i])/((1+0.0191*(dataWndwTime$temperature[i]-25))^2))^2)*U_CVALA3_temp^2))^0.5
  #           }
  #           # Compute expanded uncertainty for L1 specific conductivity
  #           conductivity_ucrtExpn_L1<-2*mean(dataWndwTime$conductivity_ucrtComb_L1)
  #         }
  #       }else{
  #         temperature_ucrtExpn_L1<-NA
  #         pressure_ucrtExpn_L1<-NA
  #         elevation_ucrtExpn_L1<-NA
  #         conductivity_ucrtExpn_L1<-NA
  #       }
  #       #copy info to output dataframe
  #       rptUcrt$temperature_ucrtExpn_L1[WndwTime] <- temperature_ucrtExpn_L1
  #       rptUcrt$pressure_ucrtExpn_L1[WndwTime] <- pressure_ucrtExpn_L1
  #       rptUcrt$elevation_ucrtExpn_L1[WndwTime] <- elevation_ucrtExpn_L1
  #       if(sensor=="aquatroll200"){
  #         rptUcrt$conductivity_ucrtExpn_L1[WndwTime] <- conductivity_ucrtExpn_L1
  #       }
  #     } # End loop through time windows
  #     
  #     
  #     #standardize column names
  #     if(Context=="GW"){
  #       names(rptUcrt)<- c("startDateTime","endDateTime","groundwaterTempExpUncert","groundwaterPressureExpUncert","groundwaterElevExpUncert","groundwaterCondExpUncert")
  #     }else if(sensor=="aquatroll200"){
  #       names(rptUcrt)<- c("startDateTime","endDateTime","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert","surfacewaterCondExpUncert")
  #     }else{
  #       names(rptUcrt)<- c("startDateTime","endDateTime","surfacewaterTempExpUncert","surfacewaterPressureExpUncert","surfacewaterElevExpUncert")
  #     }
  #     
  #     #Write out aggregate uncertainty data
  #     if(WndwAgr[idxWndwAgr]==5){
  #       window<-"005"
  #     }else if(WndwAgr[idxWndwAgr]==30){
  #       window<-"030"
  #     }else{
  #       window<-NA
  #     }
  #     rptUcrtOut_Agr <- try(NEONprocIS.base::def.wrte.parq(data = rptUcrt, 
  #                                                          NameFile = base::paste0(DirOutUcrt,"/",Context,"_",sensor,"_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_","ucrt_",window,".parquet"), 
  #                                                          Schm = SchmUcrtOut),silent=FALSE)
  #     
  #     if(any(grepl('try-error',class(rptUcrtOut_Agr)))){
  #       log$error(base::paste0('Writing the output data failed: ',attr(rptUcrtOut_Agr,"condition")))
  #       stop()
  #     } else {
  #       log$info("Averaged uncertainty data written out.")
  #     }
  #   }
  # }

  return()
} 
