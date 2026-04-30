##############################################################################################
#' @title Wrapper for EXO2 Multisonde Log File Formatting and Parsing

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Parses combined sonde log files into individual sensors.
#'
#' @param FileIn Character value. The input path to the data from a single input source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id/file. The input source-id is the unique identifier of the sonde body.  
#' 
#' @param DirOutBase Character value. The base output path.  Each sensor will get its own subfolders in this directory. 
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
#' @return Data from EXO2 multisonde log files in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
# FileIn <- "~/pfs/exo2_logjam_load_files_testprod/15847/21a311d930e1212339285ee3944ee4d3.csv"
# FileIn <- "~/pfs/exo2_logjam_load_files_testprod/55949/02f7a3866774546f7ec6ca85ba3cdbf7.csv"
# FileIn <- "~/pfs/exo2_logjam_load_files_testprod/55949/cc6e671c6b5b12507e28b75a63ad4373.csv"
# DirOutBase="~/pfs/out/exo2_logfile_output"
# SchmExo2 <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exo2_calibrated.avsc'),collapse='')
# SchmCond <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exoconductivity_calibrated.avsc'),collapse='')
# SchmDO <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exodissolvedoxygen_calibrated.avsc'),collapse='')
# SchmPh <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exophorp_calibrated.avsc'),collapse='')
# SchmTurb <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exoturbidity_calibrated.avsc'),collapse='')
# SchmFdom <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exofdom_calibrated.avsc'),collapse='')
# SchmChl <-base::paste0(base::readLines('~/pfs/exo2_avro_schemas/exototalalgae_calibrated.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'                               
#' @changelog
#' Bobby Hensley (2026-04-14)
#'   Initial creation
#' Bobby Hensley (2026-04-23)   
#'   Updates for error handling and to allow for older file formats
#' Nora Catolico (2026-04-29)
#'   Updates for file encoding
##############################################################################################
wrap.exo2.logfiles <- function(FileIn,
                             DirOutBase,
                             SchmExo2=NULL,
                             SchmCond=NULL,
                             SchmDO=NULL,
                             SchmPh=NULL,
                             SchmTurb=NULL,
                             SchmFdom=NULL,
                             SchmChl=NULL,
                             log=NULL
){
  
# Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

#get body asset info
  bodyAsset <- basename(dirname(FileIn))
  fname <- basename(FileIn)

  
# Load in the csv log file(s) 
  
  log$debug(base::paste0(FileIn,' file size: ',file.info(FileIn)$size))
  
  #determine encoding
  encoding<-readr::guess_encoding(FileIn)
  encoding_to_use<-encoding$encoding[1]
  log$debug(base::paste0(FileIn,' encoding is: ',encoding_to_use))

  logFile  <-  base::try(read.table(paste0(FileIn), fileEncoding = encoding_to_use, header = FALSE, sep = ",", 
                                    blank.lines.skip = TRUE, strip.white = TRUE, fill = TRUE,
                                    stringsAsFactors = FALSE,na.strings=c(-1,'')))
  
# File error checking  
  if (base::any(base::class(logFile) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', FileIn, ' is unreadable.'))
    base::stop()
  }
  if (any(sapply(logFile, function(x) grepl("[\u4e00-\u9fa5]", as.character(x))))=="TRUE"){
    log$error(base::paste0('File ', FileIn, ' contains non standard characters.'))
    base::stop()
  }
  if(any(grepl('TROLL',logFile))){
    log$debug(base::paste0('Skipping troll file: ', FileIn))
    base::stop()
  }else if(any(grepl('SATS',logFile))){
    log$debug(base::paste0('Skipping suna file: ', FileIn))
    base::stop()
  }

# Determine whether file is old format or new format
  if(any(sapply(logFile, function(x) any(x %in% "Devices List:")))==TRUE){formatType="OLD"
  }else{formatType="NEW"}
  
# Create generic table of all possible data streams, to be populated with sensor serial numbers.
# (Needed to make sure missing probes still get populated with an NA serial number).  
  dataStreams<-data.frame(dataStream = c("sensorDepth","sensorVoltage","wiperPosition","conductance",
                                         "dissolvedOxygen","pH","turbidity","fDOM","chlorophyll"))
  
# Create table of serial numbers for old format files (new format will be done later)
  if(formatType=="OLD"){
    serialStart_id<- which(apply(logFile, 1, function(x) any(grepl("Data Collection Device", x))))+1
    serialEnd_id<- min(which(apply(logFile, 1, function(x) any(grepl("MM/DD/YYYY", x)))))-3
    serialNumbers<-logFile[serialStart_id:serialEnd_id,1:2 ]
    colnames(serialNumbers)<-c("dataStream","serialNumber")
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Depth Non-Vented 0-10m", "sensorDepth", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Depth Non-Vented 0-100m", "sensorDepth", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("EXO2 Sonde", "sensorVoltage", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Wiper", "wiperPosition", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Conductivity/Temp", "conductance", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Optical DO", "dissolvedOxygen", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("pH/ORP", "pH", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Turbidity", "turbidity", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("fDOM", "fDOM", x))
    serialNumbers[] <- lapply(serialNumbers, function(x) gsub("Total Algae BGA-PC", "chlorophyll", x))
    serialNumbers<-merge(dataStreams,serialNumbers,by.x="dataStream",by.y="dataStream",all.x=T,all.y=F)}
  
# Removes parts of file header(s) that are not used
  logFile <- logFile[rowSums(!is.na(logFile)) > 5, ]
  logFile <- logFile[rowSums(sapply(logFile, function(x) grepl("MEAN VALUE:", x))) == 0,]
  logFile <- logFile[rowSums(sapply(logFile, function(x) grepl("STANDARD DEVIATION:", x))) == 0,]
  
# Identify if multiple headers are present in the file. (Multiple headers may signify probe serial numbers were swapped)  
  header_ids <- which(apply(logFile, 1, function(x) any(grepl("MM/DD/YYYY", x))))-1
  
# Split into list of multiple data tables for each new header  
  list_of_tables <- split(logFile, cumsum(seq_len(nrow(logFile)) %in% (header_ids)))
  names(list_of_tables) <- paste0("dataTable_", seq_along(list_of_tables))
  
# Outer loop: Format data tables and determine serial numbers for each table in list of tables   
  for(i in 1:length(list_of_tables)) {
    dataTable<-data.frame(list_of_tables[i])
    
    # Change different possible header names to match NEON terms in schemas
    dataTable[] <- lapply(dataTable, function(x) gsub("Time \\(HH:mm:ss\\)", "time", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Time \\(HH:MM:SS\\)", "time", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("TIME \\(HH:MM:SS\\)", "time", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Date \\(MM/DD/YYYY\\)", "date", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("DATE \\(MM/DD/YYYY\\)", "date", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Cable Pwr V", "sensorVoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("CABLE PWR V", "sensorVoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Battery V", "batteryVoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("BATTERY V", "batteryVoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Wiper Position volt", "wiperPosition", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("WIPER POSITION VOLT", "wiperPosition", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Depth m", "sensorDepth", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("DEPTH M", "sensorDepth", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Pressure psi a", "sondeSurfaceWaterPressure", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("PRESSURE PSI A", "sondeSurfaceWaterPressure", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Vertical Position m", "verticalPosition", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("VERTICAL POSITION M", "verticalPosition", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("SpCond µS/cm", "specificConductance", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("SPCOND µS/CM", "specificConductance", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Cond µS/cm", "conductance", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("COND µS/CM", "conductance", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Temp °C", "surfaceWaterTemperature", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("TEMP °C", "surfaceWaterTemperature", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO mg/L", "dissolvedOxygen", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO MG/L", "dissolvedOxygen", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO % sat", "dissolvedOxygenSaturation", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO % SAT", "dissolvedOxygenSaturation", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO % CB", "localDissolvedOxygenSat", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("ODO % CB", "localDissolvedOxygenSat", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("pH", "pH", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("PH", "pH", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("pH mV", "pHvoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("PH MV", "pHvoltage", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Turbidity FNU", "turbidity", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("TURBIDITY FNU", "turbidity", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("fDOM QSU", "fDOM", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("FDOM QSU", "fDOM", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Chlorophyll ug/L", "chlorophyll", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("CHLOROPHYLL ug/L", "chlorophyll", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("Chlorophyll RFU", "chlaRelativeFluorescence", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("CHLOROPHYLL RFU", "chlaRelativeFluorescence", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("TAL PC ug/L", "blueGreenAlgaePhycocyanin", x))
    dataTable[] <- lapply(dataTable, function(x) gsub("TAL PC uG/L", "blueGreenAlgaePhycocyanin", x))
    colnames(dataTable)<-apply(dataTable[2, ], 2, paste0) # Apply this header to data table(s)
    
    # Create table(s) of serial numbers from new format files
    if(formatType=="NEW"){
    serialNumbers<-na.omit(t(dataTable[1:2,]))
    colnames(serialNumbers)<-c("serialNumber","dataStream")
    serialNumbers<-merge(dataStreams,serialNumbers,by.x="dataStream",by.y="dataStream",all.x=T,all.y=F)}
    
    # Extract sensor serial numbers
    SNbody<-serialNumbers[serialNumbers$dataStream == "sensorVoltage", "serialNumber"]
    SNdepth<-serialNumbers[serialNumbers$dataStream == "sensorDepth", "serialNumber"]
    SNwipe<-serialNumbers[serialNumbers$dataStream == "wiperPosition", "serialNumber"]
    SNcond<-serialNumbers[serialNumbers$dataStream == "conductance", "serialNumber"]
    SNdo<-serialNumbers[serialNumbers$dataStream == "dissolvedOxygen", "serialNumber"]
    SNph<-serialNumbers[serialNumbers$dataStream == "pH", "serialNumber"]
    SNturb<-serialNumbers[serialNumbers$dataStream == "turbidity", "serialNumber"]
    SNfdom<-serialNumbers[serialNumbers$dataStream == "fDOM", "serialNumber"]
    SNchl<-serialNumbers[serialNumbers$dataStream == "chlorophyll", "serialNumber"]
    
    # Keep only columns and rows to be used
    keep_cols <- c("date", "time","sensorVoltage","batteryVoltage",
                   "wiperPosition","sensorDepth","sondeSurfaceWaterPressure","verticalPosition",
                   "specificConductance","conductance","surfaceWaterTemperature","dissolvedOxygen",
                   "dissolvedOxygenSaturation","localDissolvedOxygenSat","pH","pHvoltage","turbidity",
                   "fDOM","chlorophyll","chlaRelativeFluorescence","blueGreenAlgaePhycocyanin")
    dataTable <- dataTable[, names(dataTable) %in% keep_cols]
    dataTable <- dataTable[-(1:2), ] #Removes header info from data field
    # Add fields for source_id and site_id to match schemas (will be populated later)
    dataTable$source_id<-NA
    dataTable$site_id<-NA
    
    # Calculate readout date and time
    if(all(grepl(":", dataTable$time)==FALSE)){
    dataTable$readout_time<-lubridate::with_tz(as.Date(dataTable$date,format = "%m/%d/%Y") 
                                              + (as.numeric(dataTable$time)),'UTC')}
    if(all(grepl(":", dataTable$time)==TRUE)){
    dataTable$readout_time<-lubridate::with_tz(as.Date(dataTable$date,format = "%m/%d/%Y") 
                                              + lubridate::hms(dataTable$time),'UTC')}
    dataTable[, c("date","time")] <- list(NULL)
    
    #' Check that there are no dates prior to when NEON began collecting IS data
    if(any(dataTable$readout_time<"2014-01-01 00:00:00 UTC")){
      log$debug(base::paste0("Data contains dates prior to when NEON began collecting IS data"))}
    #' Check that there are no future dates after the current date
    if(any(dataTable$readout_time>Sys.time())){
      log$debug(base::paste0("Data contains future dates after the current date"))}
    # Determine file start year, month and day (for output directory)
    startYear<-substr(min(dataTable$readout_time[1]),1,4)
    startMonth<-substr(min(dataTable$readout_time[1]),6,7)
    startDay<-substr(min(dataTable$readout_time[1]),9,10)  

    # Loop for exo body data streams
    if(!is.na(SNdepth)){
      bodyTable<-dataTable[,names(dataTable) %in% c("source_id","site_id","readout_time","sensorDepth","sondeSurfaceWaterPressure","wiperPosition","batteryVoltage","sensorVoltage")]
      if(!"sensorDepth" %in% names(bodyTable)){bodyTable$sensorDepth <- NA}
      if(!"sondeSurfaceWaterPressure" %in% names(bodyTable)){bodyTable$sondeSurfaceWaterPressure <- NA}
      if(!"wiperPosition" %in% names(bodyTable)){bodyTable$wiperPosition <- NA}
      if(!"batteryVoltage" %in% names(bodyTable)){bodyTable$batteryVoltage <- NA}
      if(!"sensorVoltage" %in% names(bodyTable)){bodyTable$sensorVoltage <- NA}
      bodyTable <- bodyTable[, c("source_id","site_id","readout_time","sensorDepth","sondeSurfaceWaterPressure","wiperPosition","batteryVoltage","sensorVoltage")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2/',SNdepth,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNdepth,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = bodyTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End exo body loop
    
    # Loop for conductance
    if(!is.na(SNcond)){
      condTable<-dataTable[,names(dataTable) %in% c("source_id","site_id","readout_time","conductance","specificConductance","surfaceWaterTemperature")]
      if(!"conductance" %in% names(condTable)){condTable$conductance <- NA}
      if(!"specificConductance" %in% names(condTable)){condTable$specificConductance <- NA}
      if(!"surfaceWaterTemperature" %in% names(condTable)){condTable$surfaceWaterTemperature <- NA}
      condTable <- condTable[, c("source_id","site_id","readout_time","conductance","specificConductance","surfaceWaterTemperature")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2conductance/',SNcond,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNcond,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = condTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End conductance loop   
    
    # Loop for dissolved oxygen
    if(!is.na(SNdo)){
      doTable<-dataTable[,names(dataTable) %in% c("source_id","site_id","readout_time","dissolvedOxygen","dissolvedOxygenSaturation","localDissolvedOxygenSat")]
      if(!"dissolvedOxygen" %in% names(doTable)){doTable$dissolvedOxygen <- NA}
      if(!"dissolvedOxygenSaturation" %in% names(doTable)){doTable$dissolvedOxygenSaturation <- NA}
      if(!"localDissolvedOxygenSat" %in% names(doTable)){doTable$localDissolvedOxygenSat <- NA}
      doTable <- doTable[, c("source_id","site_id","readout_time","dissolvedOxygen","dissolvedOxygenSaturation","localDissolvedOxygenSat")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2dissolvedOxygen/',SNdo,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNdo,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = doTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End dissolvedoxygen loop 
    
    # Loop for pH
    if(!is.na(SNph)){
      phTable<-dataTable[,names(dataTable) %in% c("source_id","site_id","readout_time","pH","pHvoltage")]
      if(!"pH" %in% names(phTable)){phTable$pH <- NA}
      if(!"pHvoltage" %in% names(phTable)){phTable$pHvoltage <- NA}
      phTable <- phTable[, c("source_id","site_id","readout_time","pH","pHvoltage")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2ph/',SNph,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNph,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = phTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End pH loop
    
    # Loop for turbidity
    if(!is.na(SNturb)){
      turbTable <- dataTable[, c("source_id","site_id","readout_time","turbidity")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2turbidity/',SNturb,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNturb,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = turbTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End turbidity loop
    
    # Loop for fDOM
    if(!is.na(SNfdom)){
      fdomTable <- dataTable[, c("source_id","site_id","readout_time","fDOM")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2fdom/',SNfdom,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNfdom,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = fdomTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End fDOM loop
    
    # Loop for chlorophyll
    if(!is.na(SNchl)){
      chlTable<-dataTable[,names(dataTable) %in% c("source_id","site_id","readout_time","chlorophyll","chlaRelativeFluorescence","blueGreenAlgaePhycocyanin")]
      if(!"chlorophyll" %in% names(chlTable)){chlTable$chlorophyll <- NA}
      if(!"chlaRelativeFluorescence" %in% names(chlTable)){chlTable$chlaRelativeFluorescence <- NA}
      if(!"blueGreenAlgaePhycocyanin" %in% names(chlTable)){chlTable$blueGreenAlgaePhycocyanin <- NA}
      chlTable <- chlTable[, c("source_id","site_id","readout_time","chlorophyll","chlaRelativeFluorescence","blueGreenAlgaePhycocyanin")]
      # Create directory and write out file
      DirOut <- paste0(DirOutBase,'/exo2chlorophyll/',SNchl,"/")
      base::dir.create(DirOut,recursive=TRUE)
      csv_name <-paste0(bodyAsset,'_',SNchl,'_',startYear,'-',startMonth,'-',startDay,'_log')
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = chlTable, NameFile = base::paste0(DirOut,csv_name,".parquet"),Schm = NULL),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){log$error(base::paste0('Cannot write Data to ',base::paste0(DirOut,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {log$info(base::paste0('Data written successfully in ', base::paste0(DirOut,csv_name,".parquet"), ' from file ',fname))}
    } #End chlorophyll loop     
    
  } #End outer loop
 
} # End of function

