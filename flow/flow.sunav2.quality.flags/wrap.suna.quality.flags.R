##############################################################################################
#' @title Wrapper for SUNA quality flagging

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
#' @return SUNA data with quality flags applied in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/sunav2_location_group_and_restructure/sunav2/2024/09/10/CFGLOC110733/data" 
# DirIn<-"~/pfs/nitrate_thresh_select_ts_pad/2024/09/10/nitrate_CRAM103100/sunav2/CFGLOC110733"
# DirOut<-"~/pfs/sunav2_quality_flagged_data/sunav2/2024/09/10/CFGLOC110733" 
# DirThresholds<-"~/pfs/sunav2_thresholds" 
# SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_quality_flagged.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#' ParaTest <- list(nitrate_concentration=list(term='nitrate_concentration',test=c("null","gap","range","step","spike","persistence"),
#'                                        rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)))                               
#'                                                              
#'                                                                                             
#'                                                                                                                            
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-08-30) created
#' 
##############################################################################################
wrap.sunav2.quality.flags <- function(DirIn=NULL,
                                      DirOut=NULL,
                                      SchmDataOut=NULL,
                                      log=NULL
){
  
  #' Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  #' Read in parquet file of input data
  fileName<-base::list.files(DirIn,full.names=FALSE)
  sunaData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirIn, '/', fileName),
                                             log = log),silent = FALSE)
  
  #' Identify each burst using dark measurements
  sunaData$burst_number<-1 
  for(i in 2:nrow(sunaData)){
    if(sunaData[i,which(colnames(sunaData)=='header_light_frame')]=='0'){
      sunaData[i,which(colnames(sunaData)=='burst_number')]=sunaData[i-1,which(colnames(sunaData)=='burst_number')]+1}
    else{sunaData[i,which(colnames(sunaData)=='burst_number')]=sunaData[i-1,which(colnames(sunaData)=='burst_number')]}
  }
  
  #' Identify measurement number within burst
  sunaData$number_within_burst<-1
  for(i in 2:nrow(sunaData)){
    if(sunaData[i,which(colnames(sunaData)=='burst_number')]==sunaData[i-1,which(colnames(sunaData)=='burst_number')]){
      sunaData[i,which(colnames(sunaData)=='number_within_burst')]=sunaData[i-1,which(colnames(sunaData)=='number_within_burst')]+1}
    else{sunaData[i,which(colnames(sunaData)=='number_within_burst')]=1}
  }
  
  #' Read in csv file of quality flag thresholds
  sunaThresholds<-read.csv(file=(base::paste0(DirThresholds,'/sunav2_thresholds.csv')))
  #############################################################################################
  #' Will be a different module that find the thresholds for the site and date range of the data
  #' This is just a temporary workaround
  sunaThresholds<-sunaThresholds[(sunaThresholds$Named.Location.Name=="CRAM"),]
  #############################################################################################
  
  #' Loads individual quality flag thresholds
  HumidityMax<-sunaThresholds$Nitrates.Maximum.Internal.humidity
  MinLightDarkRatio<-sunaThresholds$Nitrates.Minimum.Light.to.Dark.Spec.Average.Ratio
  #' LampTempMax<-sunaThresholds$Nitrates.Maximum.Lamp.Temp  #' New test we need to add
  #' LampTempMax<-35
  RangeMin<-sunaThresholds$Range.Threshold.Hard.Min
  RangeMax<-sunaThresholds$Range.Threshold.Hard.Max
  StepMax<-sunaThresholds$Step.Test.value
  Gap.Test.value.....missing.points<-sunaThresholds$Gap.Test.value.....missing.points
  Persistence..time...seconds.<-sunaThresholds$Persistence..time...seconds.
  Persistence..change.<-sunaThresholds$Persistence..change.
  Despiking.Method<-sunaThresholds$Despiking.Method
  Despiking.window.size...points<-sunaThresholds$Despiking.window.size...points
  Despiking.window.step...points.<-sunaThresholds$Despiking.window.step...points.
  Despiking.maximum.consecutive.points..n.<-sunaThresholds$Despiking.maximum.consecutive.points..n.
  Despiking.maximum.....missing.points.per.window<-sunaThresholds$Despiking.maximum.....missing.points.per.window
  Despiking.MAD<-sunaThresholds$Despiking.MAD
  
  #' Converts measurements to be tested from class character to numeric
  sunaData$nitrate_concentration<-as.numeric(sunaData$nitrate_concentration)
  sunaData$relative_humidity<-as.numeric(sunaData$relative_humidity)
  sunaData$lamp_temperature<-as.numeric(sunaData$lamp_temperature)
  sunaData$spectrum_average<-as.numeric(sunaData$spectrum_average)
  sunaData$dark_value_used_for_fit<-as.numeric(sunaData$dark_value_used_for_fit)
  
  #' Performs range test
  sunaData$rangeQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')])){
      sunaData[i,which(colnames(sunaData)=='rangeQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')])){
      if(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')]<RangeMin){
        sunaData[i,which(colnames(sunaData)=='rangeQF')]=1}
      if(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')]>RangeMax){
        sunaData[i,which(colnames(sunaData)=='rangeQF')]=1}
      else{sunaData[i,which(colnames(sunaData)=='rangeQF')]=0}}  
  }
 
  #' Performs step test (only applied if sequential measurements are in the same burst)
  sunaData$stepQF<-NA
  for(i in 2:nrow(sunaData)){
    if(is.na(sunaData[i-1,which(colnames(sunaData)=='nitrate_concentration')])|is.na(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')])){
      sunaData[i,which(colnames(sunaData)=='stepQF')]=-1}  
    if(!is.na(sunaData[i-1,which(colnames(sunaData)=='nitrate_concentration')])&!is.na(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')])){
      if((abs(sunaData[i,which(colnames(sunaData)=='nitrate_concentration')]-sunaData[i-1,which(colnames(sunaData)=='nitrate_concentration')])>StepMax)&
           (sunaData[i,which(colnames(sunaData)=='burst_number')]==sunaData[i-1,which(colnames(sunaData)=='burst_number')])){
          (sunaData[i,which(colnames(sunaData)=='stepQF')]=1)&(sunaData[i-1,which(colnames(sunaData)=='stepQF')]=1)}
      else{sunaData[i,which(colnames(sunaData)=='stepQF')]=0}} 
  }
  
  #' Performs internal humidity test
  sunaData$humidityQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='relative_humidity')])){
      sunaData[i,which(colnames(sunaData)=='humidityQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='relative_humidity')])){
      if(sunaData[i,which(colnames(sunaData)=='relative_humidity')]>HumidityMax){
        sunaData[i,which(colnames(sunaData)=='humidityQF')]=1}
      else{sunaData[i,which(colnames(sunaData)=='humidityQF')]=0}}  
  }
  
  #' Performs lamp temperature test
  sunaData$lampTempQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='lamp_temperature')])){
      sunaData[i,which(colnames(sunaData)=='lampTempQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='lamp_temperature')])){
      if(sunaData[i,which(colnames(sunaData)=='lamp_temperature')]>LampTempMax){
        sunaData[i,which(colnames(sunaData)=='lampTempQF')]=1}
      else{sunaData[i,which(colnames(sunaData)=='lampTempQF')]=0}}  
  }  
  
  #' Performs light to dark spectral ratio test
  sunaData$spectralRatioQF<-NA
  for(i in 1:nrow(sunaData)){
    if(is.na(sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')])|is.na(sunaData[i,which(colnames(sunaData)=='spectrum_average')])){
      sunaData[i,which(colnames(sunaData)=='spectralRatioQF')]=-1}
    if(!is.na(sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')])&!is.na(sunaData[i,which(colnames(sunaData)=='spectrum_average')])){
      if(sunaData[i,which(colnames(sunaData)=='spectrum_average')]/sunaData[i,which(colnames(sunaData)=='dark_value_used_for_fit')]<MinLightDarkRatio){
        sunaData[i,which(colnames(sunaData)=='spectralRatioQF')]=1}
      else{sunaData[i,which(colnames(sunaData)=='spectralRatioQF')]=0}}  
  }  
  
  
 
  
   
 #' Persistence test
 sunaData$persistenceQF<-NA   
      
  
  
  
  
  
  
  #' Write out data file and log flags file  
  
  #write out data file
  fileOutSplt <- base::strsplit(DirInStream,'[/]')[[1]] # Separate underscore-delimited components of the file name
  asset<-tail(x=fileOutSplt,n=1)
  csv_name <-paste0('sunav2_',asset,'_',format(timeBgn,format = "%Y-%m-%d"))
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                               NameFile = base::paste0(DirOutData,'/',csv_name,".parquet"),
                                               Schm = SchmDataOut),silent=TRUE)
  if(class(rptOut)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',csv_name,".parquet"),'. ',attr(rptOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',csv_name,".parquet")))
  }
  
  #write out log flags file
  csv_name_flags <-paste0('sunav2_',asset,'_',format(timeBgn,format = "%Y-%m-%d"),'_logFlags')
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,
                                                    NameFile = base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),
                                                    Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',csv_name_flags,".parquet")))
  }
  
}



