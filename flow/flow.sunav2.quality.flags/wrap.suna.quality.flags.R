##############################################################################################
#' @title Wrapper for SUNA sensor-specific quality flagging

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply quality flags to SUNA data.
#'
#' @param DirIn Character value. The file path to the input data.
#'  
#' @param DirOut Character value. The file path for the output data. 
#' 
#' @param DirThresholds Character value. The file path for the quality flag thresholds. 
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return SUNA data with sensor-specific quality flags applied in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirInData<-"~/pfs/sunav2_location_group_and_restructure/sunav2/2024/09/10/CFGLOC110733/data" 
# DirInThresholds<-"~/pfs/nitrate_thresh_select_ts_pad/2024/09/10/nitrate_CRAM103100/sunav2/CFGLOC110733/threshold"
# DirOutFlags<-"~/pfs/sunav2_sensor_specific_flags/sunav2/2024/09/10/CFGLOC110733/flags/" 
# SchmFlagsOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_sensor_specific_flags.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                              
#'                                                                                             
#'                                                                                                                            
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-08-30) created
#' 
##############################################################################################
wrap.sunav2.quality.flags <- function(DirInData=NULL,
                                      DirInThresholds=NULL,
                                      DirOutFlags=NULL,
                                      SchmFlagsOut=NULL,
                                      log=NULL
){
  
  #' Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  #' Read in parquet file of SUNA data
  dataFileName<-base::list.files(DirInData,full.names=FALSE)
  sunaData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData, '/', dataFileName),
                                             log = log),silent = FALSE)
  
  #' Convert measurements to be tested from class character to numeric
  sunaData$relative_humidity<-as.numeric(sunaData$relative_humidity)
  sunaData$lamp_temperature<-as.numeric(sunaData$lamp_temperature)
  sunaData$spectrum_average<-as.numeric(sunaData$spectrum_average)
  sunaData$dark_value_used_for_fit<-as.numeric(sunaData$dark_value_used_for_fit)
  
  #' Create data frame of input file readout_times to serve as basis of output flag file
  flagFile<-as.data.frame(sunaData$readout_time)
  colnames(flagFile)<-c("readout_time")
  
  #' Read in json file of quality flag thresholds
  thresholdFileName<-base::list.files(DirInThresholds,full.names=FALSE)
  sunaThresholds<-base::try(NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = base::paste0(DirInThresholds, '/', thresholdFileName)),silent = FALSE)
  
  #' Perform internal humidity test
  humidityThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Maximum Internal humidity"),]
  maxHumidity<-humidityThreshold$number_value
  flagFile$nitrateHumidityQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='relative_humidity')])){
      flagFile[i,which(colnames(flagFile)=='nitrateHumidityQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='relative_humidity')])){
      if(sunaData[i,which(colnames(sunaData)=='relative_humidity')]>maxHumidity){
        flagFile[i,which(colnames(flagFile)=='nitrateHumidityQF')]=1}
      else{flagFile[i,which(colnames(flagFile)=='nitrateHumidityQF')]=0}}  
  }
  
  #' Perform lamp temperature test (New condition need to be created. Using default for now)
  # lampTempThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Maximum Lamp Temperature"),]
  # maxLampTemp<-lampTempThreshold$number_value
  maxLampTemp=35
  flagFile$nitrateLampTempQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='lamp_temperature')])){
      flagFile[i,which(colnames(flagFile)=='nitrateLampTempQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='lamp_temperature')])){
      if(sunaData[i,which(colnames(sunaData)=='lamp_temperature')]>maxLampTemp){
        flagFile[i,which(colnames(flagFile)=='nitrateLampTempQF')]=1}
      else{flagFile[i,which(colnames(flagFile)=='nitrateLampTempQF')]=0}}  
  }
   
  #' Perform light to dark spectral ratio test
  spectralRatioThreshold<-sunaThresholds[(sunaThresholds$threshold_name=="Nitrates Minimum Light to Dark Spec Average Ratio"),]
  minLightDarkRatio<-spectralRatioThreshold$number_value
  flagFile$nitrateLightDarkRatioQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')])|is.na(sunaData[i,which(colnames(sunaData)=='spectrum_average')])){
      flagFile[i,which(colnames(flagFile)=='nitrateLightDarkRatioQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')])&!is.na(sunaData[i,which(colnames(sunaData)=='spectrum_average')])){
      if(sunaData[i,which(colnames(sunaData)=='spectrum_average')]/sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')]<minLightDarkRatio){
        flagFile[i,which(colnames(flagFile)=='nitrateLightDarkRatioQF')]=1}
      else{flagFile[i,which(colnames(flagFile)=='nitrateLightDarkRatioQF')]=0}}  
  }  
  
  #' Write out data file and log flags file  
  base::dir.create(DirOutFlags,recursive=TRUE)
  sensorFlagFileName<-paste0(stringr::str_remove(fileName,".parquet"),'_sensor_specific_flags')

  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagFile,
                                                    NameFile = base::paste0(DirOutFlags,'/',sensorFlagFileName,".parquet"),
                                                    Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',sensorFlagFileName,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',sensorFlagFileName,".parquet")))
  }
  

  
}



