##############################################################################################
#' @title Wrapper for combining logged and streamed EXO2 data

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Combines logged and streamed EXO2 data.
#'
#' @param DirInBase Character value. The input path structured as follows: /BASE_REPO/yyyy/mm/dd/assetuid.
#' Within this directory should be sub-folders for logged and streamed data.
#'
#' @param DirOutBase Character value. The base output path that replaces the BASE_REPO part of DirInBase.
#' Sub-folders for data and log flag will be created in this directory. 
#' 
#' @param SchmBase (optional), Base directory containing schema for exo2 multisonde data streams.
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
#DirInBase <- "//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_testing/exo2conductance/2025/09/08/26669"
#DirOutBase="//wsl.localhost/Ubuntu/home/hensley/Git/pfs/exo2_log_filled"
#SchmBase <-"//wsl.localhost/Ubuntu/home/hensley/Git/pfs/schemas"
#log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'                               
#' @changelog
#' Bobby Hensley (2026-08-05)
#'   Initial creation.
#' Bobby Hensley (2026-08-10)
#'   Simplified to use single directories for input data and schema.  
#' Nora Catolico (2026-08-12) 
#'   updated to work with final input structure and added schema support 
##############################################################################################

wrap.exo2.logfiles.fill <- function(DirInBase,
                                    DirOutBase,
                                    SchmBase=NULL,
                                    log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info and creates full directories
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInBase)
  DirInData <- fs::path(DirInBase,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOut,'/log_flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  # All file names
  fileNames<-base::list.files(DirInData,full.names=FALSE)
  loggedFileName<-fileNames[grepl("_log",fileNames)]
  streamedFileName<-fileNames[!grepl("_log",fileNames)]

  # Load any streamed data
  if(length(streamedFileName)>=1){
    streamedData  <-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData, '/', streamedFileName),log = log),silent = FALSE)
    if (base::any(base::class(streamedData) == 'try-error')) {
      log$error(base::paste0('File ', DirInData, '/', streamedFileName, ' is unreadable.'))
      base::stop()}
  }else{streamedData<-NULL}
  
  # Load any logged data
  if(length(loggedFileName)>=1){
    loggedData  <-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData, '/', loggedFileName),log = log),silent = FALSE)
    if (base::any(base::class(loggedData) == 'try-error')) {
      log$error(base::paste0('File ', DirInData, '/', loggedFileName, ' is unreadable.'))
      base::stop()}
  }else{loggedData<-NULL}
  
  # Determine which sensor the data comes from. 
  sensor <- base::strsplit(InfoDirIn$dirRepo,'[/]')[[1]][2]
  #check that sensor is one of the expected types. If not, log an error and stop.
  if(!sensor %in% c("exo2","exo2conductivity","exo2dissolvedoxygen","exo2phorp","exo2turbidity","exo2fdom","exo2algae")){
    log$error(base::paste0('Sensor type ', sensor, ' is not recognized.'))
    base::stop()
  }

  # Set corrected schema based on sensor type
  if(!is.null(SchmBase)){
    SchmData<-base::paste0(SchmBase,"/",sensor,"/",sensor,'_filled.avsc')
    SchmData <- base::paste0(base::readLines(SchmData),collapse='')
    SchmFlags<-base::paste0(SchmBase,"/",sensor,"/flags_logs_",sensor,'.avsc')
    SchmFlags <- base::paste0(base::readLines(SchmFlags),collapse='')
  }
  
  #!!!!!!!!!!!!!!!!!!!!!!!Adjust streamed data headers to match schemas (For testing; this will get fixed earlier)!!!!!!!!!!!!!!!!!!!!!!!  
  # if(sensor=="exo2"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id", "site_id", "readout_time","sensorDepth","sondeSurfaceWaterPressure","wiperPosition","batteryVoltage","sensorVoltage") }}
  # if(sensor=="exo2conductivity"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","conductance","specificConductance","surfaceWaterTemperature") }}
  # if(sensor=="exo2dissolvedoxygen"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","dissolvedOxygen","dissolvedOxygenSaturation","localDissolvedOxygenSat") }}
  # if(sensor=="exo2phorp"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","pH","pHvoltage") }}
  # if(sensor=="exo2turbidity"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","turbidity") }}
  # if(sensor=="exo2fdom"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","fDOM") }}
  # if(sensor=="exo2algae"){if(!is.null(streamedData)){colnames(streamedData) <- c("source_id","site_id","readout_time","chlorophyll","chlaRelativeFluorescence","blueGreenAlgaePhycocyanin") }}
  # #!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
  
  # Add log data flags based on sensor type
  if(!is.null(streamedData)){streamedData$logDataQF<-0} # First a generic flag used only temporarily for filtering
  if(!is.null(loggedData)){loggedData$logDataQF<-1} # First a generic flag used only temporarily for filtering
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
  fileOutSplt <- base::strsplit(DirInBase,'[/]')[[1]]
  assetuid<-tail(x=fileOutSplt,n=1)
  outputFileName <-paste0(sensor,'_',assetuid,'_',format(timeBgn,format = "%Y-%m-%d"))
  flagsFileName <-paste0(sensor,'_',assetuid,'_',format(timeBgn,format = "%Y-%m-%d"),'_log_flags')
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut, NameFile = base::paste0(DirOutData,'/',outputFileName,".parquet"),
                                               Schm = SchmData),silent=TRUE)
  if(class(rptOut)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutData,'/',outputFileName,".parquet"),'. ',attr(rptOut, "condition")))
    stop()
  } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOutData,'/',outputFileName,".parquet")))}
  
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = logFlags, NameFile = base::paste0(DirOutFlags,'/',flagsFileName,".parquet"),
                                                    Schm = SchmFlags),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',flagsFileName,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',flagsFileName,".parquet")))}
  
} # End function.
















