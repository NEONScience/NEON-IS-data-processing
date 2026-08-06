##############################################################################################
#' @title Wrapper for combining logged and streamed EXO2 data

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Combines logged and streamed EXO2 data.
#'
#' @param DirInStream Character value. The input path to daily parquet file containing streamed data 
#' from a single input source ID, structured as follows: /BASE_REPO/yyyy/mm/dd/assetuid.
#'
#' @param DirInLog Character value. The input path to daily parquet file containing logged data 
#' from a single input source ID, structured as follows: /BASE_REPO/yyyy/mm/dd/assetuid.
#' 
#' @param DirOutBase Character value. The base output path that replaces the BASE_REPO part of DirIn. 
#' 
#' @param SchmExo2 (optional), Schema for the output data from the sonde body.
#' 
#' @param SchmExo2Conductivity (optional), Schema for the output data from the conductivity and temperature probe.
#' 
#' @param SchmExo2DissolvedOxygen (optional), Schema for the output data from the dissolved oxygen probe.
#' 
#' @param SchmExo2Ph (optional), Schema for the output data from the pH probe.
#' 
#' @param SchmExo2Turbidity (optional), Schema for the output data from the turbidity probe.
#' 
#' @param SchmExo2Fdom (optional), Schema for the output data from the fdom probe.
#' 
#' @param SchmExo2Chlorophyll (optional), Schema for the output data from the chlorophyll probe.
#'                                                                 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return Combined streamed and logged data from EXO2 multisonde in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2/2025/09/07/15853"
DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2conductivity/2025/09/07/26669/"
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2dissolvedoxygen/2025/09/07/21269/data"
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2phorp/2025/09/07/24799/data"
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2turbidity/2025/09/07/25219/data"
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2fdom/2025/09/07/59477/data"
#DirInStream <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_data_source_trino/exo2alage/2025/09/07/26669/data"
DirInLogs <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_replace_sn_with_assetuid/exo2conductance/2025/09/07/16231"
DirOutBase="//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_log_filled"
# SchmExo2 <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exo2/exo2_calibrated.avsc"
SchmCond <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exoconductivity/exoconductivity_calibrated.avsc"
# SchmDO <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exodissolvedoxygen/exodissolvedoxygen_calibrated.avsc"
# SchmpH <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exophorp/exophorp_calibrated.avsc"
# SchmTurb <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exoturbidity/exoturbidity_calibrated.avsc"
# SchmfDOM <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exofdom/exofdom_calibrated.avsc"
# SchmChl <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exototalalgae/exototalalgae_calibrated.avsc"
# SchmFlagsExo2 <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exo2/flags_logs_exo2.avsc"
SchmFlagsCond <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exoconductivity/flags_logs_exoconductivity.avsc"
# SchmFlagsDO <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exodissolvedoxygen/flags_logs_exodissolvedoxygen.avsc"
# SchmFlagspH <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exophorp/flags_logs_exophorp.avsc"
# SchmFlagsTurb <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exoturbidity/flags_logs_exoturbidity.avsc"
# SchmFlagsfDOM <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exofdom/flags_logs_exofdom.avsc"
# SchmFlagsChl <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas/exototalalgae/flags_logs_exototalalgae.avsc"
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'                               
#' @changelog
#' Bobby Hensley (2026-08-05)
#'   Initial creation
##############################################################################################

wrap.exo2.logfiles.fill <- function(DirInStream=NULL,
                                    DirInLogs=NULL,
                                    DirOutBase,
                                    SchmExo2=NULL,
                                    SchmCond=NULL,
                                    SchmDO=NULL,
                                    SchmpH=NULL,
                                    SchmTurb=NULL,
                                    SchmfDOM=NULL,
                                    SchmChl=NULL,
                                    SchmFlagsExo2=NULL,
                                    SchmFlagsCond=NULL,
                                    SchmFlagsDO=NULL,
                                    SchmFlagspH=NULL,
                                    SchmFlagsTurb=NULL,
                                    SchmFlagsfDOM=NULL,
                                    SchmFlagsChl=NULL,
                                    log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info and creates full directories
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInStream)
  DirInStreamData <- fs::path(DirInStream,'data')
  DirInLogsData <- fs::path(DirInLogs,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,'/log_flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  # Load any streamed data
  streamedFileName<-base::list.files(DirInStreamData,full.names=FALSE)
  if(length(streamedFileName)>=1){
    streamedData  <-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInStreamData, '/', streamedFileName),log = log),silent = FALSE)
    if (base::any(base::class(streamedData) == 'try-error')) {
      log$error(base::paste0('File ', DirInStreamData, '/', streamedFileName, ' is unreadable.'))
      base::stop()}
  }else{streamedData<-NULL}
  
  # Load any logged data
  loggedFileNameLog<-base::list.files(DirInLogsData,full.names=FALSE)
  if(length(loggedFileNameLog)>=1){
    loggedData  <-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInLogsData, '/', loggedFileNameLog),log = log),silent = FALSE)
    if (base::any(base::class(loggedData) == 'try-error')) {
      log$error(base::paste0('File ', DirInLogsData, '/', loggedFileNameLog, ' is unreadable.'))
      base::stop()}
  }else{loggedData<-NULL}
  
  # Determine which sensor the data comes from and set corresponding schema.
  if(any(grepl("depth", colnames(streamedData)))|any(grepl("depth", colnames(loggedData)))){sensor="exo2";SchmData<-SchmExo2;SchmFlags<-SchmFlagsExo2}
  if(any(grepl("conductivity", colnames(streamedData)))|any(grepl("conductivity", colnames(loggedData)))){sensor="exo2conductivity";SchmData<-SchmCond;SchmFlags<-SchmFlagsCond}
  if(any(grepl("oxygen", colnames(streamedData)))|any(grepl("oxygen", colnames(loggedData)))){sensor="exo2dissolvedoxygen";SchmData<-SchmDO;SchmFlags<-SchmFlagsDO}
  if(any(grepl("ph", colnames(streamedData)))|any(grepl("ph", colnames(loggedData)))){sensor="exo2phorp";SchmData<-SchmpH;SchmFlags<-SchmFlagspH}
  if(any(grepl("turbidity", colnames(streamedData)))|any(grepl("turbidity", colnames(loggedData)))){sensor="exo2turbidity";SchmData<-SchmTurb;SchmFlags<-SchmFlagsTurb}
  if(any(grepl("fdom", colnames(streamedData)))|any(grepl("fdom", colnames(loggedData)))){sensor="exo2fdom";SchmData<-SchmfDOM;SchmFlags<-SchmFlagsfDOM}
  if(any(grepl("chlorophyll", colnames(streamedData)))|any(grepl("chlorophyll", colnames(loggedData)))){sensor="exo2algae";SchmData<-SchmChl;SchmFlags<-SchmFlagsChl}  
  
  
  # Adjust streamed data headers to match schemas (this will be fixed earlier)
  if(sensor=="exo2"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id", "site_id", "readout_time","sensorDepth","sondeSurfaceWaterPressure","wiperPosition","batteryVoltage","sensorVoltage") }}
  if(sensor=="exo2conductivity"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","conductance","specificConductance","surfaceWaterTemperature") }}
  if(sensor=="exo2dissolvedoxygen"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","dissolvedOxygen","dissolvedOxygenSaturation","localDissolvedOxygenSat") }}
  if(sensor=="exo2phorp"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","pH","pHvoltage") }}
  if(sensor=="exo2turbidity"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","turbidity") }}
  if(sensor=="exo2fdom"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","fDOM") }}
  if(sensor=="exo2algae"){if(!is.null(streamedData)){
    colnames(streamedData) <- c("source_id","site_id","readout_time","chlorophyll","chlaRelativeFluorescence","blueGreenAlgaePhycocyanin") }}
  
  # Add log data flags based on sensor type
  streamedData$logDataQF<-0 #Generic flag used only for filtering
  loggedData$logDataQF<-1 #Generic flag used only for filtering
  if(sensor=="exo2"){if(!is.null(streamedData)){streamedData$sensorDepthLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$sensorDepthLogDataQF<-1}}
  if(sensor=="exo2conductivity"){if(!is.null(streamedData)){streamedData$specificCondLogDataQF<-0;streamedData$surfaceWaterTempLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$specificCondLogDataQF<-1;loggedData$surfaceWaterTempLogDataQF<-1}}
  if(sensor=="exo2dissolvedoxygen"){if(!is.null(streamedData)){streamedData$dissolvedOxygenLogDataQF<-0;streamedData$seaLevelDOSatLogDataQF<-0;streamedData$localDOSatLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$dissolvedOxygenLogDataQF<-1;loggedData$seaLevelDOSatLogDataQF<-1;loggedData$localDOSatLogDataQF<-1 }}
  if(sensor=="exo2phorp"){if(!is.null(streamedData)){streamedData$pHLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$pHLogDataQF<-1 }}
  if(sensor=="exo2turbidity"){if(!is.null(streamedData)){streamedData$turbidityLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$turbidityLogDataQF<-1 }}
  if(sensor=="exo2fdom"){if(!is.null(streamedData)){streamedData$fdomLogDataQF<-0};
            if(!is.null(loggedData)){loggedData$fdomLogDataQF<-1}}
  if(sensor=="exo2algae"){if(!is.null(streamedData)){streamedData$chlorophyllLogDataQF<-0;streamedData$chlaRelativeFluorLogDataQF<-0 };
            if(!is.null(loggedData)){loggedData$chlorophyllLogDataQF<-0;loggedData$chlaRelativeFluorLogDataQF<-0 }}

  # If there is only streamed data use that.
  if(!is.null(streamedData) & is.null(loggedData)){dataOut<-streamedData}
  # If there is only logged data use that.
  if(is.null(streamedData) & !is.null(loggedData)){dataOut<-loggedData}
  # Combine logged and streamed data.  
  if(!is.null(streamedData) & !is.null(loggedData)){
    dataOut<-rbind(loggedData,streamedData)
    # If there is logged and streamed data for a bin time, logged data is used.
    dataOut$bin_time<-lubridate::floor_date(dataOut$readout_time,unit = "5 minutes")
    dataOut <- dataOut %>%
      group_by(bin_time) %>%
      slice_max(order_by = logDataQF, n = 1, with_ties = FALSE) %>%
      dplyr::ungroup()
      dataOut <- subset(dataOut, select = -c(bin_time))
  }
  dataOut <- subset(dataOut, select = -c(logDataQF))
  
  # Separate data and log flags back into separate data frames.
  logFlags <- dataOut[, grepl("readout_time|QF", names(dataOut))]
  dataOut <- dataOut[, !grepl("QF", names(dataOut))]
  
  # Write out data and flags.
  fileOutSplt <- base::strsplit(DirInStream,'[/]')[[1]]
  assetuid<-tail(x=fileOutSplt,n=1)
  outputFileName <-paste0(sensor,'_',assetuid,'_',format(timeBgn,format = "%Y-%m-%d"))
  flagsFileName <-paste0(sensor,'_',assetuid,'_',format(timeBgn,format = "%Y-%m-%d"),'_log_flags')
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, NameFile = base::paste0(DirOutData,'/',outputFileName,".parquet"),
                                               Schm = NULL),silent=TRUE)
  if(class(rptOut)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',outputFileName,".parquet"),'. ',attr(rptOut, "condition")))
    stop()
  } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',outputFileName,".parquet")))}
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = logFlags, NameFile = base::paste0(DirOutFlags,'/',flagsFileName,".parquet"),
                                                    Schm = NULL),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',flagsFileName,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',flagsFileName,".parquet")))}
  
} # End function.
















