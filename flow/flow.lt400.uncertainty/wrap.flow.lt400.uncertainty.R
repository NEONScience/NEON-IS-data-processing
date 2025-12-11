##############################################################################################
#' @title Wrapper for HOBO U24 File Processing

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}
#' 
#' @description Wrapper function. Validates, cleans, and formats HOBO U24 files into daily parquets.
#'
#' @param FileIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id/file.
#' The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of FileIn. 
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
#' @return Cleaned leveltroll400 files in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
#' FileIn <- "b6a5483d7675e2f5294cbb0b22021694.csv"
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.subs.leveltroll400.logfiles <- function(FileIn = "~/pfs/troll_logjam_load_files/49150/29f52c1330bf24da67f9ab3a64d0ab51.csv",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#   Kaelin Cawley (2024-12-06) original creation
#' 
##############################################################################################
wrap.subs.HOBOU24.files <- function(FileIn,
                             DirOut,
                             SchmDataOut=NULL,
                             log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  
  # --------- Load the data ----------
  asset_string <- regexpr("\\/[0-9]{5}\\/",FileIn) # AssetUID, input folder name?
  Asset <- gsub("\\/","",substr(FileIn,asset_string[1],asset_string[1]+attributes(asset_string)$match.length-1))
  # Load in the csv log file(s)
  # Read in a single test file from leveltroll400
  log_file <- base::try(utils::read.table(base::paste0(FileIn),
                                   header = FALSE,
                                   sep = ",",
                                   #col.names = paste0("V",seq_len(11)),
                                   encoding = 'utf-8',
                                   stringsAsFactors = FALSE,
                                   fill = TRUE,
                                   strip.white = TRUE,
                                   na.strings=c(-1,'')))
  
  
  if (base::any(base::class(log_file) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', FileIn, ' is unreadable. Likely not a HOBO file.'))
    base::stop()
  }
  if(any(grepl('SUNA',log_file$V2))){
    log$debug(base::paste0('skipping SUNA file: ', FileIn))
    base::stop()
  }else if(all(!grepl('Plot Title',log_file$V1))){
    log$debug(base::paste0('skipping sonde file: ', FileIn))
    base::stop()
  }else{
    
    # Things needed to write out file to the correct directory
    sensor <- "hobou24" # source-type? 
    
    #find row where data actually starts (third row for HOBO files)
    start<-3
    if(nrow(log_file)<=start){
      log$error(base::paste0('File Error: No data in ', FileIn))
    }
    #figure out column order and standardize headers (sometimes differs based on log settings/ version)
    col1<-log_file$V1[start-1]  # #
    col2<-base::gsub(",.*","",log_file$V2[start-1])  # Date Time, TZ
    col3<-base::gsub(",.*","",log_file$V3[start-1]) 
    col4<-base::gsub(",.*","",log_file$V4[start-1])
    col5<-base::gsub(",.*","",log_file$V5[start-1])
    col6<-base::gsub(",.*","",log_file$V6[start-1])
    col7<-base::gsub(",.*","",log_file$V7[start-1])
    col8<-base::gsub(",.*","",log_file$V8[start-1])
    col9<-base::gsub(",.*","",log_file$V9[start-1])
    col10<-base::gsub(",.*","",log_file$V10[start-1])
    col11<-base::gsub(",.*","",log_file$V10[start-1])

    allCols <- c(col1,col2,col3,col4,col5,col6,col7,col8,col9,col10,col11)
    
    # Pull out timezone
    log_file_TZ <- gsub(".*, ","",log_file[start-1,allCols == "Date Time"])
    # Pull out low range conductivity units
    log_file_low_units <- gsub("Low Range, | \\(.*$","",log_file[start-1,allCols == "Low Range"])
    # Pull out high range conductivity units
    log_file_high_units <- gsub("High Range, | \\(.*$","",log_file[start-1,allCols == "High Range"])
    # Pull out temp units
    log_file_temp_units <- gsub("Temp, | \\(.*$","",log_file[start-1,allCols == "Temp"])
    # Pull out specific conductance units
    log_file_spcond_units <- gsub("Specific Conductance, | \\(.*$","",log_file[start-1,allCols == "Specific Conductance"])
    
    if(length(log_file_high_units) == 0 & length(log_file_low_units) > 0){
      rangeType <- "lowOnly"
    }else if(length(log_file_low_units) == 0 & length(log_file_high_units > 0)){
      rangeType <- "highOnly"
    }else if(length(log_file_high_units) > 0 & length(log_file_low_units) > 0){
      rangeType <- "highAndLow"
    }else if(length(log_file_high_units) == 0 & length(log_file_low_units) == 0 & length(log_file_spcond_units) > 0){
      rangeType <- "specificCondOnly"
    }else{
      log$error(base::paste0('File Error: No conductivity data in ', FileIn))
    }
    
    # Now start making the output data table
    log_data_names <- c("readout_time",
                        "day",
                        "conductivity_low",
                        "conductivity_high",
                        "temperature",
                        "specific_conductance")
    
    log_data <- base::as.data.frame(base::matrix(data = NA, nrow = (nrow(log_file)-2), ncol = length(log_data_names)))
    names(log_data) <- log_data_names
    
    # Convert timestamps to UTC, if needed
    timeData <- log_file[start:nrow(log_file),allCols == "Date Time"]
    timePOSIX <- base::as.POSIXct(timeData, format = "%m/%d/%y %H:%M:%S %p", tz = log_file_TZ)
    timeSOM <- format(timePOSIX, format = "%Y-%m-%dT%H:%M:%S", tz = "UTC")
    day <- format(timePOSIX, format = "%Y-%m-%d", tz = "UTC")
    
    # Populate the output file with timestamps
    log_data$readout_time <- timeSOM
    log_data$day <- day
    
    # Populate conductivity data, I think it should all be in uS/cm
    if(rangeType == "lowOnly" | rangeType == "highAndLow"){
      log_data$conductivity_low <- log_file[start:nrow(log_file),allCols == "Low Range"]
    }
    if(rangeType == "highOnly" | rangeType == "highAndLow"){
      log_data$conductivity_high <- log_file[start:nrow(log_file),allCols == "High Range"]
    }
    
    # Convert temperature to Celsius if it's Fahrenheit
    if(log_file_temp_units == "°C"){
      log_data$temperature <- log_file[start:nrow(log_file),allCols == "Temp"]
    }else if(log_file_temp_units == "°F"){
      tempFerenheit <- log_file[start:nrow(log_file),allCols == "Temp"]
      tempCelsius <- (tempFerenheit - 32)/1.8
      log_data$temperature <- tempCelsius
    }else{
      log$error(base::paste0('File Error: Temp data units unknown ', FileIn))
    }
    
    # Populate specific conductance only if low and high are both missing
    if(rangeType == "specificCondOnly"){
      log_data$specific_conductance <- log_file[3:nrow(log_file),allCols == "Specific Conductance"]
    }
    
    # Remove rows that don't have cond data
    log_data <- log_data[!(is.na(log_data$conductivity_low)&is.na(log_data$conductivity_high)&is.na(log_data$specific_conductance)),]
    # Remove rows that are missing temp and don't have spCond
    log_data <- log_data[!(is.na(log_data$specific_conductance)&is.na(log_data$temperature)),]
    
    if(nrow(log_data) < 1){
      log$error(base::paste0('File Error: No valid conductivity data ', FileIn))
    }
    
    # Create file metadata (not sure where this goes)
    logFilename <- FileIn
    loggerSN <- gsub(".*LGR S/N: |, SEN.*","",log_file[start-1,allCols == "Temp"])
    if(length(loggerSN) == 0){
      loggerSN <- gsub(".*LGR S/N: |)*","",log_file[start-1,allCols == "Specific Conductance"])
    }
    plotTitle <- gsub(".*: ","",log_file[start-2,1])

    
    out <- log_data
    out$source_id <- Asset
    out_columns <- c('readout_time','day','conductivity_low','conductivity_high','temperature','specific_conductance','source_id')
    ###subset into 1-day data files
    all_days<-split(out, as.Date(out$day))
    
    #output daily files
    if(length(all_days)>0){
      for(j in 1:length(all_days)){
        #create DF
        out_file <- as.data.frame(all_days[j])
        colnames(out_file) <- out_columns
        year <- substr(out_file$day[1],1,4)
        month <- substr(out_file$day[1],6,7)
        day <- substr(out_file$day[1],9,10)
        out_file <- out_file[,c('source_id','readout_time','conductivity_low','conductivity_high','temperature','specific_conductance')]
        # if(sensor=='aquatroll200'){
        #   out_file <- out_file[,c('source_id','readout_time','pressure','temperature','conductivity','logFlag','logDateErrorFlag')]
        # }else if(sensor=='leveltroll500'){
        #   out_file <- out_file[,c('source_id','readout_time','pressure','temperature','logFlag','logDateErrorFlag')]
        # }
        #create output directory
        DirOutLogFile <- paste0(DirOut,'/',sensor,'/',year,'/',month,'/',day,'/',Asset,'/data/')
        base::dir.create(DirOutLogFile,recursive=TRUE)
        csv_name <-paste0(sensor,'_',Asset,'_',year,'-',month,'-',day,'_log')
        
        rptOut <- try(NEONprocIS.base::def.wrte.parq(data = out_file,
                                                     NameFile = base::paste0(DirOutLogFile,csv_name,".parquet"),
                                                     Schm = SchmDataOut),silent=TRUE)
        if(class(rptOut)[1] == 'try-error'){
          log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutLogFile,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
          stop()
        } else {
          log$info(base::paste0('Data written successfully in ', base::paste0(DirOutLogFile,csv_name,".parquet")))
        }
      }#end of days loop
    }else{
      log$error(base::paste0('No days can be written out for ', FileIn))
    }
    
    
    
    
    
    # # Nora's code
    #   log_data<-log_file[start:(length(log_file$V1)),1:6]
    #   if(grepl('date', tolower(col1))){
    #     colnames(log_data)[1]<-'readout_time'
    #   }else{
    #     log$error(base::paste0('File Error: No datetime column where expected in ', FileIn))
    #   }
    #   if(grepl('seconds', tolower(col2))){
    #     colnames(log_data)[2]<-'seconds'
    #   }else{
    #     log$error(base::paste0('File Error: No seconds column where expected in ', FileIn))
    #   }
    #   if(grepl('pressure', tolower(col3))){
    #     colnames(log_data)[3]<-'pressure'
    #   }else if(grepl('temp', tolower(col3))){
    #     colnames(log_data)[3]<-'temperature'
    #   }else if(grepl('cond', tolower(col3))){
    #     colnames(log_data)[3]<-'conductivity'
    #   }else if(grepl('depth', tolower(col3))){
    #     colnames(log_data)[3]<-'depth'
    #   }else{
    #     log$error(base::paste0('File Error: No expected streams present in column 3 of ', FileIn))
    #   }
    #   if(grepl('pressure', tolower(col4))){
    #     colnames(log_data)[4]<-'pressure'
    #   }else if(grepl('temp', tolower(col4))){
    #     colnames(log_data)[4]<-'temperature'
    #   }else if(grepl('cond', tolower(col4))){
    #     colnames(log_data)[4]<-'conductivity'
    #   }else if(grepl('depth', tolower(col4))){
    #     colnames(log_data)[4]<-'depth'
    #   }else{
    #     log$error(base::paste0('File Error: No expected streams present in column 4 of ', FileIn))
    #   }
    #   if(!is.na(col5)){
    #     if(grepl('cond', tolower(col5))){
    #       colnames(log_data)[5]<-'conductivity'
    #     }else if(grepl('pressure', tolower(col5))){
    #       colnames(log_data)[5]<-'pressure'
    #     }else if(grepl('temp', tolower(col5))){
    #       colnames(log_data)[5]<-'temperature'
    #     }else if(grepl('depth', tolower(col5))|grepl('elevation', tolower(col5))){
    #       colnames(log_data)[5]<-'depth'
    #     }else{
    #       log$error(base::paste0('File Error: No expected streams present in column 5 of ', FileIn))
    #     }
    #   }
    #   if(!is.na(col6)){
    #     if(grepl('cond', tolower(col6))){
    #       colnames(log_data)[6]<-'conductivity'
    #     }else if(grepl('pressure', tolower(col6))){
    #       colnames(log_data)[6]<-'pressure'
    #     }else if(grepl('temp', tolower(col6))){
    #       colnames(log_data)[6]<-'temperature'
    #     }else if(grepl('depth', tolower(col6))|grepl('elevation', tolower(col6))){
    #       colnames(log_data)[6]<-'depth'
    #     }else{
    #       log$error(base::paste0('File Error: No expected streams present in column 5 of ', FileIn))
    #     }
    #   }
    #   log_data<-log_data[!is.na(log_data$readout_time),]
    #   log_metadata<-log_file[1:start,]
    # 
    # 
    # #check timezone. lot's of different styles... 
    # if(any(grepl('Time Zone: ',log_metadata$V1))){
    #   timezone<-log_metadata$V1[grepl('Time Zone: ',log_metadata$V1)]
    #   timezone<-gsub('Time Zone: ','',timezone)
    # }else if(any(grepl('Time Zone',log_metadata$V1))){
    #   timezone<-log_metadata$V2[grepl('Time Zone',log_metadata$V1)]
    # }else if(any(grepl('Time Zone',log_metadata$V2))){
    #   timezone<-log_metadata$V3[grepl('Time Zone',log_metadata$V2)]
    # }else{
    #   timezone<-'ERROR'
    #   log$error(base::paste0('File Error: timezone not specified in ', FileIn))
    # }
    # #then clean up TZ 
    # #grep("Dateline", OlsonNames(), value=TRUE)
    # if(timezone=="Coordinated Universal Time"){
    #   timezone<-'UTC'
    # }else if(grepl('Eastern Standard Time',timezone)|grepl('Eastern Daylight Time',timezone)|grepl('Dateline',timezone)){
    #   timezone<-'EST'
    # }else if(grepl('Central Daylight Time',timezone)|grepl('Central Standard Time',timezone)){
    #   timezone<-'US/Central'
    # }else if(grepl('Pacific Daylight Time',timezone)|grepl('Pacific Standard Time',timezone)|grepl('UTC-08',timezone)){
    #   timezone<-'US/Pacific'
    # }else if(grepl('Mountain Daylight Time',timezone)|grepl('Mountain Standard Time',timezone)){
    #   timezone<-'US/Mountain'
    # }else if(grepl('Alaskan Daylight Time',timezone)|grepl('Alaskan Standard Time',timezone)|grepl('UTC-09',timezone)){
    #   timezone<-'US/Alaska'
    # }else if(grepl('SA Western  Daylight Time',timezone)|grepl('SA Western Standard Time',timezone)){
    #   timezone<-'America/Puerto_Rico'
    # }else if(grepl('GMT',timezone)|grepl('Greenwich Standard Time',timezone)){
    #   timezone<-'GMT'
    # }else if(timezone=='Unknown'){
    #   if(any(grepl('UTC',log_metadata))){
    #     timezone<-'UTC'
    #   }
    # }
    # 
    # #clean up metadata
    # removeAfter<-which(log_metadata$V1=='Log Notes:')
    # if(length(removeAfter)>0){
    #   log_metadata <- as.data.frame(log_metadata[1:(removeAfter),])
    # }
    # log_metadata$V1[is.na(log_metadata$V1)]<-log_metadata$V2[is.na(log_metadata$V1)]
    # log_metadata$V2[!is.na(log_metadata$V3)]<-log_metadata$V3[!is.na(log_metadata$V3)]
    # log_metadata<-log_metadata[,1:2]
    # colnames(log_metadata)<-c("label","value")
    # 
    # #Metadata values
    # logName <- log_metadata$value[!is.na(log_metadata$label) & (log_metadata$label=="Log Name"|log_metadata$label=="File Name")][1]
    # Troll_SN <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Serial Number"][1]
    # Asset <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device Name"][1]
    # #log$debug(base::paste0('metadata: ',logName,'_',Troll_SN,'_',Asset))
    # if(length(Asset)<1 || Asset == " " || nchar(Asset) == 0){
    #   log$error(base::paste0('File Info: No asset specified in ', FileIn))
    #   stop()
    # }
    # #define Site
    # Site <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Site"]
    # if(length(Site)<1){
    #   log$info(base::paste0('File Info: No site specified in ', FileIn))
    # }else if(Site == 'Default Site'){
    #   Site <- NA
    #   log$info(base::paste0('File Info: Default site specified in ', FileIn))
    # }else if(length(Site)>1){
    #   log$info(base::paste0('File Info: More than one site specified in ', FileIn))
    # }else if(nchar(Site)>4){
    #   Site <-substr(Site,5,8)
    # }
    # #fix for specific use case
    # if(grepl('Central America Standard Time',timezone) & !is.na(Site) & (Site == "MCDI"|Site == "KING")){
    #   timezone<-'US/Central'
    # }  
    # Device <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device"][1]
    # if(Device == "Level TROLL 400"){
    #   Device<-"Level TROLL 400"
    #   Context <- "surfacewater"
    #   sensor <- "leveltroll400"
    #   keep<-c('readout_time','seconds','pressure','temperature')
    #   log_data<-log_data[keep]
    # }else{
    #   log$debug(base::paste0('skipping other TROLL file: ', FileIn))
    #   base::stop()
    # }
    # 
    # # if(!is.na(Device) & grepl('level',tolower(Device))){
    # #   Device<-"Level TROLL 500"
    # #   Context <- "surfacewater"
    # #   sensor <- "leveltroll500"
    # #   keep<-c('readout_time','seconds','pressure','temperature')
    # #   log_data<-log_data[keep]
    # # }else if(!is.na(Device) & grepl('aqua',tolower(Device))){
    # #   Device<-"Aqua TROLL 200"
    # #   sensor <- "aquatroll200"
    # #   if(!is.na(logName) & (grepl("IN",logName)|grepl("Inlet",logName)|grepl("OT",logName)|grepl("Outlet",logName)|grepl("L1",logName)|
    # #                         grepl("L2",logName)|grepl("Lit",logName)|grepl("S1",logName)|grepl("S2",logName))){
    # #     Context <- "surfacewater"
    # #   }else if(!is.na(logName) & (grepl("GW",logName)|any(grepl("conductivity",tolower(colnames(log_data)))))){
    # #     Context <- "groundwater"
    # #   }else{
    # #     log$error(base::paste0('File Error: Context not specified in ', FileIn))
    # #   }
    # # }else{
    # #   log$error(base::paste0('File Error: Device not specified in ', FileIn))
    # # }
    # 
    # 
    # ###check and update date format
    # #sometimes ymd others mdy, sometimes has / others -, some don't have seconds
    # #check if date contains seconds
    # if(length(base::gregexpr(':', log_data$readout_time[1])[[1]])==2){ #if 2 : then it has seconds
    #   #Check if date begins with year (assume 4 digit year, which seems to always be true)
    #   if(grepl('^\\d{4}', log_data$readout_time[1])){
    #     #ymd format
    #     log_data$dateTime <- lubridate::ymd_hms(log_data$readout_time, tz = timezone)
    #   }else{
    #     #assume mdy format
    #     log_data$dateTime <- lubridate::mdy_hms(log_data$readout_time, tz = timezone)
    #   }
    # }else if(length(base::gregexpr(':', log_data$readout_time[1])[[1]])==1){
    #   #doesn't have seconds
    #   #Check if date begins with year (assuming 4 digit year)
    #   if(grepl('^\\d{4}', log_data$readout_time[1])){
    #     #ymd format
    #     log_data$dateTime <- lubridate::ymd_hm(log_data$readout_time, tz = timezone)
    #   }else{
    #     #assume mdy format
    #     log_data$dateTime <- lubridate::mdy_hm(log_data$readout_time, tz = timezone)
    #   }
    # }else{
    #   log$error(base::paste0('File Error: Invalid date time format',log_data$readout_time[1],' in ', FileIn))#this shouldn't happen
    # } 
    # log_data<-log_data[!is.na(log_data$dateTime),]
    # 
    # #add date as UTC
    # log_data$dateUTC<-lubridate::with_tz(log_data$dateTime,'UTC')
    # 
    # #check that dates are 2018 or later (some files have 1970 error)
    # log_data$logFlag<-1
    # log_data$logDateErrorFlag<-0
    # 
    # if(any(log_data$dateUTC<"2018-01-01 00:00:00 UTC")){
    #   log$debug(base::paste0("Data contains dates prior to NEON logging implementation. Attempt will be made to align and flag data."))
    #   logDateError<-which(log_data$dateUTC<"2018-01-01 00:00:00 UTC")
    #   if(logDateError[1]!=1){ #If there is a good date before the 1970 shift we can try to continue the data and add a flag
    #     # sampling frequency
    #     if(Context=='groundwater'){
    #       freq <- 300 #5 min in seconds
    #     }else{
    #       freq <- 60
    #     }
    #     idx_start<-logDateError[1]
    #     idx_end<-logDateError[length(logDateError)]
    #     time_last_read<-log_data$dateUTC[idx_start-1]
    #     first_new_time<-time_last_read + freq
    #     num_readings<-length(logDateError)
    #     new_times <- seq(first_new_time, by = freq, length = num_readings)
    #     log_data$dateUTC[idx_start:idx_end]<-new_times
    #     log_data$logDateErrorFlag[idx_start:idx_end]<-1
    #   }else{
    #     #cannot use log data with bad dates
    #     #log$debug(base::paste0("Log data contains erroneous dates that cannot be linked to the correct time."))
    #     log_data<-log_data[log_data$dateUTC>"2018-01-01 00:00:00 UTC",]
    #     log$debug(base::paste0('File Error: ALL DATA 1970 in ', FileIn))
    #   }
    # }
    # if(nrow(log_data)>0){
    #   log_data$readout_time<-log_data$dateUTC
    #   
    #   #round to minute
    #   if(Context=='surfacewater'){
    #     log_data$readout_time<-lubridate::round_date(log_data$dateUTC,unit = "minute")
    #   }else if(Context=='groundwater'){
    #     log_data$readout_time<-lubridate::round_date(log_data$dateUTC,unit = "5 minutes")
    #   }
    #   
    #   log_data$day<-lubridate::floor_date(log_data$dateUTC,"days")
    #   
    #   log_data$source_id<-Asset
    #   
    #   #format output file
    #   #create any missing columns in log file
    #   if(!'pressure' %in% names(log_data)){log_data$pressure<-NA}
    #   if(!'temperature' %in% names(log_data)){log_data$temperature<-NA}
    #   out_columns <- c('source_id','readout_time','pressure','temperature','logFlag','logDateErrorFlag','day')
    #   # if(sensor=='aquatroll200'){
    #   #   if(!'conductivity' %in% names(log_data)){log_data$conductivity<-NA}
    #   #   out_columns <- c('source_id','readout_time','pressure','temperature','conductivity','logFlag','logDateErrorFlag','day')
    #   # }else if(sensor=='leveltroll500'){
    #   #   out_columns <- c('source_id','readout_time','pressure','temperature','logFlag','logDateErrorFlag','day')
    #   # }
    #   out<-log_data[out_columns]
    #   
    #   first_reading<-log_data$dateUTC[1]
    #   if(length(log_data$dateUTC)>0){
    #     last_reading<-log_data$dateUTC[length(log_data$dateUTC)]
    #   }else{
    #     last_reading<-NA
    #   }
    #   

    #}
  }
} #end of file

















