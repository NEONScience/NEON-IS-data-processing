##############################################################################################
#' @title Wrapper for Troll Log File Comparison and Gap Filling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Compares logged data to streamed data and fills gaps.#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/sensor/yyyy/mm/dd/source-id. The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirInStream (optional) Character value. This input is used for testing purposes only prior to joining repos.
#' The input path to the streamed L0 data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/sensor/yyyy/mm/dd/source-id. The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirInLogs (optional) Character value. This input is used for testing purposes only prior to joining repos.
#' The input path to the log data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/sensor/yyyy/mm/dd/source-id. The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for the output flags 
#' file. If this input is not provided, the output schema for the data will be the same as the input flags
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Combined logged and streamed L0 data in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
#' DirInLogs<-'/home/NEON/ncatolico/pfs/logjam_clean_troll_files/leveltroll500/2022/03/10/21115' #cleaned log data
#' DirInStream<-'/home/NEON/ncatolico/pfs/leveltroll500_data_source_trino/leveltroll500/2022/03/10/21115' #streamed L0 data
#' DirIn<-'/home/NEON/ncatolico/pfs/logjam_clean_troll_files/leveltroll500/2022/03/10/21115'
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.logfiles.fill <- function(DirInLogs=DirInLogs,
#'                               DirInStream=DirInStream,
#'                               DirIn=NULL,
#'                               DirOutBase="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               SchmFlagsOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#   Nora Catolico (2024-01-30) original creation
#' 
##############################################################################################
wrap.troll.logfiles.fill <- function(DirInLogs=NULL,
                             DirInStream=NULL,
                             DirIn,
                             DirOutBase,
                             SchmDataOut=NULL,
                             SchmFlagsOut=NULL,
                             log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date), and create base output directory
  if(is.null(DirInLogs)){
    DirInLogs<-DirIn #only need one dir if this is run after filter joiner
  }
  if(is.null(DirInStream)){
    DirInStream<-DirIn #only need one dir if this is run after filter joiner
  }
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInLogs)
  dirInDataStream <- fs::path(DirInStream,'data')
  dirInDataLogs <- fs::path(DirInLogs,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,'/flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  # Take stock of our data files.
  fileDataStream<-base::list.files(dirInDataStream,full.names=FALSE)
  L0File <- fileDataStream[!grepl('_log',fileDataStream)]
  fileDataLogs<-base::list.files(dirInDataLogs,full.names=FALSE)
  LogFile <- fileDataLogs[grepl('_log',fileDataLogs)]
  
  # --------- Load the data ----------
  #load in streamed data, if exists
  if(length(L0File)>=1){
    L0Data  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInDataStream, '/', L0File),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInDataStream, '/', L0File, ' is unreadable.'))
      base::stop()
    }
    if('conductivity.x' %in% names(L0Data)){
      sensor<-'aquatroll200'
    }else{
      sensor<-'leveltroll500'
    }
  }else{
    L0Data <- NULL
  }
  
  #load in log data, if exists
  if(length(LogFile)>=1){
    LogData  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInDataLogs, '/', LogFile),
                                               log = log),
                silent = FALSE)
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInDataLogs, '/', LogFile, ' is unreadable.'))
      base::stop()
    }
    if('conductivity.x' %in% names(L0Data)){
      sensor<-'aquatroll200'
    }else{
      sensor<-'leveltroll500'
    }
  }else{
    LogData <- NULL
  }
  # note, already binned into 1-min or 5-mins in flow.troll.logfiles script
  
  ## if only one file, keep that file, otherwise merge files
  if(is.null(L0Data) & !is.null(LogData)){
    dataOut<-LogData
    dataOut$pressure_data_quality<-NA
    dataOut$temperature_data_quality<-NA
    dataOut$internal_battery<-NA
    dataOut$site_id<-NA
    dataOut$pressureLogFlag<-1
    dataOut$logDateFlag<-1
    dataOut$temperatureLogFlag<-1
    if(sensor=='aquatroll200'){
      dataOut$conductivityLogFlag<-1
    }
  }else if(!is.null(L0Data) & is.null(LogData)){
    dataOut<-L0Data
    dataOut$pressureLogFlag<-0
    dataOut$logDateFlag<-0
    dataOut$temperatureLogFlag<-0
    if(sensor=='aquatroll200'){
      dataOut$conductivityLogFlag<-0
    }
  }else{
    
    ##time to merge 
    #(LogData$readout_time[!LogData$readout_time %in% L0Data$readout_time])
    #(L0Data$readout_time[!L0Data$readout_time %in% LogData$readout_time])
    if(!identical(L0Data[['readout_time']],LogData[['readout_time']])){
      log$error(base::paste0('Files do not have identical readout times: ', DirIn))
      base::stop()
    }
    
    dataOut<-merge(L0Data,LogData,by='readout_time',all.x = TRUE)
    dataOut$pressureLogFlag<-0
    dataOut$logDateFlag<-0
    dataOut$temperatureLogFlag<-0
    #plot(dataOut$pressure.x,dataOut$readout_time)
    
    dataOut$pressureLogFlag[is.na(dataOut$pressure.x)]<-dataOut$logFlag[is.na(dataOut$pressure.x)]
    dataOut$logDateFlag[is.na(dataOut$pressure.x)]<-dataOut$logDateErrorFlag[is.na(dataOut$pressure.x)]
    dataOut$pressure.x[is.na(dataOut$pressure.x)]<-dataOut$pressure.y[is.na(dataOut$pressure.x)]
    
    dataOut$temperatureLogFlag[is.na(dataOut$temperature.x)]<-dataOut$logFlag[is.na(dataOut$temperature.x)]
    dataOut$temperature.x[is.na(dataOut$temperature.x)]<-dataOut$temperature.y[is.na(dataOut$temperature.x)]
    
    if(sensor=='aquatroll200'){
      dataOut$conductivityLogFlag<-0
      dataOut$conductivityLogFlag[is.na(dataOut$conductivity.x)]<-dataOut$logFlag[is.na(dataOut$conductivity.x)]
      dataOut$conductivity.x[is.na(dataOut$conductivity.x)]<-dataOut$conductivity.y[is.na(dataOut$conductivity.x)]
    }
  }
  
  #standardize column names
  if(sensor=='leveltroll500'){
    keep_flags<-c('readout_time','pressureLogFlag','logDateErrorFlag','temperatureLogFlag')
    flagsOut<-dataOut[keep_flags]
    
    keep_data<-c('readout_time','source_id.x','site_id','pressure.x','temperature.x','pressure_data_quality','temperature_data_quality')
    dataOut<-dataOut[keep_data]
    names(dataOut)<- c('readout_time','source_id','site_id','pressure','temperature','pressure_data_quality','temperature_data_quality')
  
  }else if(sensor=='aquatroll200'){
    keep_flags<-c('readout_time','pressureLogFlag','logDateErrorFlag','temperatureLogFlag','conductivityLogFlag')
    flagsOut<-dataOut[keep_flags]
    
    keep_data<-c('readout_time','source_id.x','site_id','pressure.x','temperature.x','conductivity.x','pressure_data_quality','temperature_data_quality','conductivity_data_quality')
    dataOut<-dataOut[keep_data]
    names(dataOut)<- c('readout_time','source_id','site_id','pressure','temperature','conductivity','pressure_data_quality','temperature_data_quality','conductivity_data_quality')
  }
  
  #write out data
  fileOutSplt <- base::strsplit(DirInStream,'[/]')[[1]] # Separate underscore-delimited components of the file name
  asset<-tail(x=fileOutSplt,n=1)
  csv_name <-paste0(sensor,'_',asset,'_',format(timeBgn,format = "%Y-%m-%d"))
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                               NameFile = base::paste0(DirOutData,csv_name,".parquet"),
                                               Schm = SchmDataOut),silent=TRUE)
  if(class(rptOut)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',csv_name,".parquet"),'. ',attr(rptOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',csv_name,".parquet")))
  }
  
  #write out flags
  csv_name_flags <-paste0(sensor,'_',asset,'_',format(timeBgn,format = "%Y-%m-%d"),'_logFlags')
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,
                                               NameFile = base::paste0(DirOutFlags,csv_name_flags,".parquet"),
                                               Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',csv_name_flags,".parquet")))
  }
  

}
















