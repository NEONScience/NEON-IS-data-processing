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
wrap.subs.hobou24.files <- function(FileIn,
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
    allCols <- gsub("Full Range", "High Range", allCols)
    
    
    # Pull out timezone
    log_file_TZ <- gsub(".*, ","",log_file[start-1,allCols == "Date Time"])
    # Pull out low range conductivity units
    log_file_low_units <- gsub("Low Range, | \\(.*$","",log_file[start-1,allCols == "Low Range"])
    # Pull out high range conductivity units
    if(length(log_file[start-1,allCols == "Full Range"])>0){
      log_file[start-1,] <- gsub("Full Range", "High Range", log_file[start-1,])
    }
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
    time_posix <- as.POSIXct(timeData, format="%m/%d/%y %I:%M:%S %p", tz = log_file_TZ)
    time_utc <- format(time_posix, tz="UTC", usetz=TRUE)
    time_utc_posix <- as.POSIXct(time_utc, format="%Y-%m-%d %H:%M:%S", tz="UTC")
    
    # Populate the output file with timestamps
    log_data$readout_time <- time_utc_posix
    log_data$day <- format(log_data$readout_time, format = "%Y-%m-%d", tz = "UTC")
    
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
  
  }
} #end of file

















