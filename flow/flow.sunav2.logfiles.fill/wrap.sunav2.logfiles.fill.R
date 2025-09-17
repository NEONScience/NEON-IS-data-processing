##############################################################################################
#' @title Wrapper for SUNA Log File Comparison and Gap Filling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Compares logged data to streamed data.
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
#' THE INPUT DATA.
#' 
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for the output flags 
#' file. If this input is not provided, the output schema for the data will be the same as the input flags
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
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
# DirInLogs<-"~/pfs/sunav2_logjam_assign_clean_files/sunav2/2024/09/11/20349" #cleaned log data
# DirInStream<-"~/pfs/sunav2_trino_data_parser/sunav2/2025/06/22/20345" #streamed L0 data
# DirIn<-NULL
# DirOutBase="~/pfs/out"
# SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2/sunav2_logfilled.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# SchmFlagsOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'),collapse='')
#'                               
#' @changelog
#' Nora Catolico (2024-01-30) original creation
#' Bobby Hensley (2025-05-30) adapted for suna
#' 
##############################################################################################
wrap.sunav2.logfiles.fill <- function(DirInLogs=NULL,
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
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInStream)
  dirInDataStream <- fs::path(DirInStream,'data')
  dirInDataLogs <- fs::path(DirInLogs,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,'/flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
#' Load any L0 streamed data
  fileDataStream<-base::list.files(dirInDataStream,full.names=FALSE)
  L0File <- fileDataStream[!grepl('_log',fileDataStream)]
  if(length(L0File)>=1){
    L0Data  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInDataStream, '/', L0File),
                                               log = log),silent = FALSE)
    if (base::any(base::class(L0Data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInDataStream, '/', L0File, ' is unreadable.'))
      base::stop()}
  }else{
    L0Data<-NULL
  }
  
#' Load any logged data
  fileDataLogs<-base::list.files(dirInDataLogs,full.names=FALSE)
  logFile <- fileDataLogs[grepl('_log',fileDataLogs)]
  if(length(logFile)>=1){
    logData  <-
      base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInDataLogs, '/', logFile),
                                               log = log),silent = FALSE)
    if (base::any(base::class(logData) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', dirInDataLogs, '/', logFile, ' is unreadable.'))
      base::stop()}
  }else{
    logData<-NULL
  }
  
#' update columns to same format
  if(length(L0Data)>=1){
    L0Data$spectrum_channels <- lapply(L0Data$spectrum_channels, function(x) paste(x, collapse = ";")) 
  }
  
  
#' Determine whether to use logged or streamed data.  
  #' Logged data is used if available, and log data flag set to 1
  if(length(logFile)>=1){
    dataOut<-as.data.frame(logData)
    flagsOut<-data.frame(matrix(ncol=2,nrow=nrow(dataOut), dimnames=list(NULL, c("readout_time", "sunaLogDataQF"))))
    flagsOut$readout_time<-dataOut$readout_time
    flagsOut$sunaLogDataQF<-1
    }
  #' Streamed data is used if no logged data is available, and log data flags set to 0
  if(length(logFile)<1 & length(L0Data)>=1){
    dataOut<-as.data.frame(L0Data)
    flagsOut<-data.frame(matrix(ncol=2,nrow=nrow(dataOut), dimnames=list(NULL, c("readout_time", "sunaLogDataQF"))))
    flagsOut$readout_time<-dataOut$readout_time
    flagsOut$sunaLogDataQF<-0
    }
  
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
















