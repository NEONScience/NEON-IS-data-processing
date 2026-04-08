##############################################################################################
#' @title Wrapper for SUNA sensor-specific quality flagging

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply sensor-specific quality flags to SUNA data.  
#' Measurements where the lamp has not had enough time to stabilize  (nitrateLampStabilizeQF=1) are removed. 
#'
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility flags and quality flag thresholds.
#'  
#' @param DirOut Character value. The base file path for the output data. 
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the data file.
#' This should be the same for the input as the output.  Only the number of rows of measurements should change. 
#' 
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for the output flags. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return SUNA data file and combined flag file in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/nitrate_analyze_pad_and_qaqc_plau/2025/06/01/nitrate-surfacewater_CRAM103100/sunav2/CFGLOC110733" 
# DirOut<-"~/pfs/nitrate_sensor_flag_and_remove/2025/06/01/nitrate-surfacewater_CRAM103100/sunav2/CFGLOC110733" 
# SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_logfilled.avsc'),collapse='')
# SchmFlagsOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_all_flags.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-08-30)
#' Initial creation.
#' 
#' Bobby Hensley (2025-09-18)
#' Updated so that measurements prior to lamp stabilization (never intended to be
#' used in downstream pipeline) are removed.
#' 
#' Bobby Hensley (2025-09-22)
#' Updated to use single input directory and added check that data and flag file
#' have same number of measurements.
#' 
#'  Bobby Hensley (2025-10-30)
#'  Updated to revert over-flagged measurements at end of burst. 
#'  
#'  Bobby Hensley (2025-12-10)
#'  Updated lamp stabilization to pass added null "filler" for completely missing bursts.
#' 
#' Bobby Hensley (2025-12-16)
#' Updated so that dark measurements caused by lamp temperature cutoff are still counted as part of same burst.
#' Updated so that any low transmittance error codes ("-1") are always flagged and set to NA.
#' 
#' Bobby Hensley (2025-12-18)
#' Updated so lamp stabilization test sets failed values to NA rather than removing entire line.
##############################################################################################
wrap.sunav2.quality.flags <- function(DirIn,
                                      DirOutBase,
                                      SchmDataOut=NULL,
                                      SchmFlagsOut=NULL,
                                      log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInData <- paste0(DirIn,"/data")
  DirInFlags <- paste0(DirIn,"/flags")
  DirInThresholds <- paste0(DirIn,"/threshold")
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,"/data")
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,"/flags")
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  #' Read in parquet file of SUNA data.
  dataFileName<-base::list.files(DirInData,full.names=FALSE)
  if(length(dataFileName)==0){
    log$error(base::paste0('Data file not found in ', DirInData)) 
    stop()
  } else {
    sunaData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData, '/', dataFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',dataFileName))
  }
  
  #' Read in parquet file of QAQC plausibility flags.
  plausFileName<-grep("flagsPlaus",base::list.files(DirInFlags,full.names=FALSE),value=TRUE)
  if(length(plausFileName)==0){
    log$error(base::paste0('Plausibility flags not found in ', DirInFlags)) 
    stop()
  } else {
    plausFlags<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags, '/', plausFileName),
                                                         log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',plausFileName))
  }
  
  #' Read in parquet file of calibration flags.
  calFileName<-grep("flagsCal",base::list.files(DirInFlags,full.names=FALSE),value=TRUE)
  if(length(calFileName)==0){
    log$error(base::paste0('Calibration flags not found in ', DirInFlags)) 
    stop()
  } else {
    calFlags<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags, '/', calFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',calFileName))
  }
  
  #' Read in parquet file of logged file flags.
  logFileName<-grep("logFlags",base::list.files(DirInFlags,full.names=FALSE),value=TRUE)
  if(length(calFileName)==0){
    log$error(base::paste0('Log flags not found in ', DirInFlags)) 
    stop()
  } else {
    logFlags<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags, '/', logFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',logFileName))
  }
  
  #' Convert measurements to be tested from class character to numeric.
  sunaData$relative_humidity<-as.numeric(sunaData$relative_humidity)
  sunaData$lamp_temperature<-as.numeric(sunaData$lamp_temperature)
  sunaData$spec_average<-as.numeric(sunaData$spec_average)
  sunaData$dark_signal_average<-as.numeric(sunaData$dark_signal_average)
  
  #' Create data frame of input data file readout_times to serve as basis of sensor specific flag file.
  sensorFlags<-as.data.frame(sunaData$readout_time)
  colnames(sensorFlags)<-c("readout_time")
  
  #' Read in json file of quality flag thresholds.
  thresholdFileName<-base::list.files(DirInThresholds,full.names=FALSE)
  sunaThresholds<-base::try(NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = base::paste0(DirInThresholds, '/', thresholdFileName)),silent = FALSE)
  
  #' Perform internal humidity test.
  humidityThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Maximum Internal humidity"),]
  maxHumidity<-humidityThreshold$number_value
  sensorFlags$nitrateHumidityQF<-NA
  sensorFlags[['nitrateHumidityQF']] <- ifelse(
    is.na(sunaData[['relative_humidity']]),
    -1,
    ifelse(sunaData[['relative_humidity']] > maxHumidity, 1, 0)
  )
  
  #' Perform lamp temperature test (New condition need to be created. Using default for now).
  # lampTempThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Maximum Lamp Temperature"),]
  # maxLampTemp<-lampTempThreshold$number_value
  maxLampTemp=35 #' Hard-coded until thresholds are updated.
  sensorFlags$nitrateLampTempQF<-NA
  sensorFlags[['nitrateLampTempQF']] <- ifelse(
    is.na(sunaData[['lamp_temperature']]),
    -1,
    ifelse(sunaData[['lamp_temperature']] > maxLampTemp, 1, 0)
  )
   
  #' Perform light to dark spectral ratio test.
  spectralRatioThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Minimum Light to Dark Spec Average Ratio"),]
  minLightDarkRatio<-spectralRatioThreshold$number_value
  sensorFlags$nitrateLightDarkRatioQF<-NA
  sensorFlags[['nitrateLightDarkRatioQF']] <- case_when(
    is.na(sunaData[['dark_signal_average']]) | is.na(sunaData[['spec_average']]) ~ -1,
    sunaData[['light_dark_frame']] == 0 ~ -1,
    sunaData[['spec_average']] / sunaData[['dark_signal_average']] < minLightDarkRatio ~ 1,
    TRUE ~ 0
  )
  #' Extra test so that low transmittance error codes (-1) always trigger spectral ratio test regardless of threshold
  sensorFlags[!is.na(sunaData[['nitrate']]) & !is.na(sunaData[['nitrogen_in_nitrate']]) & 
                sunaData[['nitrate']] == -1 & sunaData[['nitrogen_in_nitrate']] == -1, 
              'nitrateLightDarkRatioQF'] <- 1
  
  #' Identifies light measurement number within burst and performs lamp stabilization test.
  # lampStabilizeThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Lamp Stabilization Points"),]
  # lampStabilizePoints<-lampStabilizeThreshold$number_value
  lampStabilizePoints=9 #' Hard-coded until thresholds are updated.
  sensorFlags$burstNumber<-0 #' Assumes each burst starts with a dark measurement.
  #' If measurement is a light frame, or if the lamp temp caused a dark measurement, it is counted as the next measuremnt in a burst.
  for(i in 2:nrow(sunaData)){
    if(!is.na(sunaData[i,which(colnames(sunaData)=='light_dark_frame')])){
      if(sunaData[i,which(colnames(sunaData)=='light_dark_frame')]==1|sensorFlags[i,which(colnames(sensorFlags)=='nitrateLampTempQF')]==1){
        sensorFlags[i,which(colnames(sensorFlags)=='burstNumber')]=sensorFlags[i-1,which(colnames(sensorFlags)=='burstNumber')]+1}
      else{sensorFlags[i,which(colnames(sensorFlags)=='burstNumber')]=0}}
  }
  #' If light dark header is missing, assumes value was added null "filler" for a missing burst that needs to be passed.
  sensorFlags[is.na(sunaData[['light_dark_frame']]), 'burstNumber'] <- 9999
  sensorFlags$nitrateLampStabilizeQF<-0
  sensorFlags[sensorFlags[['burstNumber']] <= lampStabilizePoints, 'nitrateLampStabilizeQF'] <- 1
  
  #' Combines all flags into a single file.
  allFlags<-base::merge(plausFlags,sensorFlags)
  allFlags<-base::merge(allFlags,calFlags)
  allFlags<-base::merge(allFlags,logFlags)
  
  #' Revert plausibility flags for last measurement of each burst to prevent over-flagging.
  #' (Plausibility tests were run across bursts, where the time step is much larger than between measurements within bursts)
  for(i in 3:nrow(allFlags)){
    if((allFlags[i,which(colnames(allFlags)=='burstNumber')]==0)&(allFlags[i-2,which(colnames(allFlags)=='nitrateStepQF')]==0)){
      allFlags[i-1,which(colnames(allFlags)=='nitrateStepQF')]=0}
  } 
  for(i in 3:nrow(allFlags)){
    if((allFlags[i,which(colnames(allFlags)=='burstNumber')]==0)&(allFlags[i-2,which(colnames(allFlags)=='nitratePersistenceQF')]==0)){
      allFlags[i-1,which(colnames(allFlags)=='nitratePersistenceQF')]=0}
  } 
  
  #' Drops burst number column since it's no longer needed.
  allFlags<-allFlags[,-which(colnames(allFlags)=='burstNumber')] 
  
  #' Removes measurements where lamp has not stabilized from data and flag files.
  #lampStabilizeFlagsOnly<-sensorFlags[,c("readout_time","nitrateLampStabilizeQF")]
  #sunaData<-base::merge(sunaData,lampStabilizeFlagsOnly) #' Adds lamp stabilize QF to data file
  #sunaData<-sunaData[(sunaData$nitrateLampStabilizeQF==0),]
  #allFlags<-allFlags[(allFlags$nitrateLampStabilizeQF==0),]
  #sunaData<-sunaData[,-which(colnames(sunaData)=='nitrateLampStabilizeQF')]
  #sunaData<-sunaData[,c(2,3,1,4:37)]  
  
  #' Checks that data file and flag file have same number of measurements
  if(nrow(sunaData) != nrow(allFlags)){
    log$error(base::paste0('Error: Data and flags have different number of measuremnts'))
    stop()
  } else {
    log$debug(base::paste0('Data and flags have same number of measurements'))
  }
  
  #' Replace with NA's so that flagged data is excluded from averaging
  dataOut<-merge(sunaData,allFlags,by='readout_time')
  dataOut$nitrate[dataOut$light_dark_frame==0]<-NA    #' Set any dark measurements to NA (just in case)
  dataOut$nitrate[dataOut$nitrateHumidityQF==1]<-NA
  dataOut$nitrate[dataOut$nitrateLampTempQF==1]<-NA
  dataOut$nitrate[dataOut$nitrateLightDarkRatioQF==1]<-NA
  dataOut$nitrate[dataOut$nitrateLampStabilizeQF==1]<-NA
  dataOut<-dataOut[,which(colnames(dataOut)%in%colnames(sunaData))]
  
  #' Write out data file.  
  rptOutData <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                                    NameFile = base::paste0(DirOutData,'/',dataFileName),
                                                    Schm = SchmDataOut),silent=TRUE)
  if(class(rptOutData)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',dataFileName),'. ',attr(rptOutData, "condition")))
    stop()
  } else {
    log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',dataFileName)))
  }
  
  #' Write out flags file.  
  allFlagFileName<-paste0(stringr::str_remove(dataFileName,".parquet"),'_all_flags')
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = allFlags,
                                                    NameFile = base::paste0(DirOutFlags,'/',allFlagFileName,".parquet"),
                                                    Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',allFlagFileName,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',allFlagFileName,".parquet")))
  }
  
}



