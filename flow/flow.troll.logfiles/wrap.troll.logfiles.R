##############################################################################################
#' @title Wrapper for Troll Log File Processing

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Validates, cleans, and formats troll log files into daily parquets.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id.The source-id folder may have multiple csv log files. 
#' The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Cleaned troll log files in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
#' DirIn<-'/home/NEON/ncatolico/pfs/logjam_load_files/23646'
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.logfiles <- function(DirIn="~/pfs/logjam_load_files/23646",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#   Nora Catolico (2024-01-09) original creation
#' 
##############################################################################################
wrap.troll.logfiles <- function(DirIn,
                             DirOut,
                             SchmDataOut=NULL,
                             log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Take stock of our data files. 
  fileData <- base::list.files(DirIn,full.names=FALSE)
  #log$debug(base::paste0('Files identified:', DirIn, '/', fileData))
  
  # --------- Load the data ----------
  # Load in the csv log file(s)
  for (i in 1:length(fileData)){
    fileData_i <- fileData[i]
    #log$debug(base::paste0('File of interest:', DirIn, '/', fileData_i))
    log_file  <-
      base::try(read.table(paste0(DirIn, '/', fileData_i), header = FALSE, sep = ",", 
                           col.names = paste0("V",seq_len(6)),encoding = 'utf-8',
                           stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
    if (base::any(base::class(data) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0('File ', DirIn, '/', fileData_i, ' is unreadable.'))
      base::stop()
    }
    
    if(any(grepl('SUNA',log_file$V2))){
      log$debug(base::paste0('skipping SUNA file: ', DirIn, '/', fileData_i))
    }else if(any(grepl('_Depth',log_file$V1))){
      log$debug(base::paste0('skipping sonde file: ', DirIn, '/', fileData_i))
    }else{
      #find row where data actually starts
      start<-which(grepl('Seconds',log_file$V2))+1
      #figure out column order and standardize headers (sometimes differs based on log settings/ version)
      col1<-log_file$V1[start-1]
      col2<-log_file$V2[start-1]
      col3<-log_file$V3[start-1]
      col3<-substr(col3,1,14)
      col4<-log_file$V4[start-1]
      col4<-substr(col4,1,14)
      col5<-log_file$V5[start-1]
      col5<-substr(col5,1,14)
      
      if(start>0){
        log_data<-log_file[start:(length(log_file$V1)),1:5]
        if(grepl('date', tolower(col1))){
          colnames(log_data)[1]<-'readout_time'
        }else{
          log$error(base::paste0('File Error: No datetime column where expected in ',DirIn, '/', fileData_i))
        }
        if(grepl('seconds', tolower(col2))){
          colnames(log_data)[2]<-'seconds'
        }else{
          log$error(base::paste0('File Error: No seconds column where expected in ',DirIn, '/', fileData_i))
        }
        if(grepl('pressure', tolower(col3))){
          colnames(log_data)[3]<-'pressure'
        }else if(grepl('temp', tolower(col3))){
          colnames(log_data)[3]<-'temperature'
        }else if(grepl('cond', tolower(col3))){
          colnames(log_data)[3]<-'conductivity'
        }else if(grepl('depth', tolower(col3))){
          colnames(log_data)[3]<-'depth'
        }else{
          log$error(base::paste0('File Error: No expected streams present in column 3 of ',DirIn, '/', fileData_i))
        }
        if(grepl('pressure', tolower(col4))){
          colnames(log_data)[4]<-'pressure'
        }else if(grepl('temp', tolower(col4))){
          colnames(log_data)[4]<-'temperature'
        }else if(grepl('cond', tolower(col4))){
          colnames(log_data)[4]<-'conductivity'
        }else if(grepl('depth', tolower(col4))){
          colnames(log_data)[4]<-'depth'
        }else{
          log$error(base::paste0('File Error: No expected streams present in column 4 of ',DirIn, '/', fileData_i))
        }
        if(is.na(col5)|grepl('cond', tolower(col5))){
          colnames(log_data)[5]<-'conductivity'
        }else if(grepl('pressure', tolower(col5))){
          colnames(log_data)[5]<-'pressure'
        }else if(grepl('temp', tolower(col5))){
          colnames(log_data)[5]<-'temperature'
        }else if(grepl('depth', tolower(col5))|grepl('elevation', tolower(col5))){
          colnames(log_data)[5]<-'depth'
        }else{
          log$error(base::paste0('File Error: No expected streams present in column 5 of ',DirIn, '/', fileData_i))
        }
        log_data<-log_data[!is.na(log_data$readout_time),]
        log_metadata<-log_file[1:start,]
      }else{
        log$error(base::paste0('File Error: No data in ',DirIn, '/', fileData_i))
      }
      
      #check timezone. lot's of different styles... 
      if(any(grepl('Time Zone: ',log_metadata$V1))){
        timezone<-log_metadata$V1[grepl('Time Zone: ',log_metadata$V1)]
        timezone<-gsub('Time Zone: ','',timezone)
      }else if(any(grepl('Time Zone',log_metadata$V1))){
        timezone<-log_metadata$V2[grepl('Time Zone',log_metadata$V1)]
      }else if(any(grepl('Time Zone',log_metadata$V2))){
        timezone<-log_metadata$V3[grepl('Time Zone',log_metadata$V2)]
      }else{
        timezone<-'ERROR'
        log$error(base::paste0('File Error: timezone not specified in ',DirIn, '/', fileData_i))
      }
      #then clean up TZ 
      #grep("Dateline", OlsonNames(), value=TRUE)
      if(timezone=="Coordinated Universal Time"){
        timezone<-'UTC'
      }else if(grepl('Eastern Standard Time',timezone)|grepl('Eastern Daylight Time',timezone)|grepl('Dateline',timezone)){
        timezone<-'EST'
      }else if(grepl('Central Daylight Time',timezone)|grepl('Central Standard Time',timezone)){
        timezone<-'US/Central'
      }else if(grepl('Pacific Daylight Time',timezone)|grepl('Pacific Standard Time',timezone)|grepl('UTC-08',timezone)){
        timezone<-'US/Pacific'
      }else if(grepl('Mountain Daylight Time',timezone)|grepl('Mountain Standard Time',timezone)){
        timezone<-'US/Mountain'
      }else if(grepl('Alaskan Daylight Time',timezone)|grepl('Alaskan Standard Time',timezone)|grepl('UTC-09',timezone)){
        timezone<-'US/Alaska'
      }else if(grepl('SA Western  Daylight Time',timezone)|grepl('SA Western Standard Time',timezone)){
        timezone<-'America/Puerto_Rico'
      }else if(grepl('GMT',timezone)|grepl('Greenwich Standard Time',timezone)){
        timezone<-'GMT'
      }else if(timezone=='Unknown'){
        if(any(grepl('UTC',log_metadata))){
          timezone<-'UTC'
        }
      }
      
      #clean up metadata
      removeAfter<-which(log_metadata$V1=='Log Notes:')
      if(length(removeAfter)>0){
        log_metadata <- as.data.frame(log_metadata[1:(removeAfter),])
      }
      log_metadata$V1[is.na(log_metadata$V1)]<-log_metadata$V2[is.na(log_metadata$V1)]
      log_metadata$V2[!is.na(log_metadata$V3)]<-log_metadata$V3[!is.na(log_metadata$V3)]
      log_metadata<-log_metadata[,1:2]
      colnames(log_metadata)<-c("label","value")
      
      #Metadata values
      logName <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Log Name"]
      Troll_SN <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Serial Number"]
      Asset <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device Name"]
      #log$debug(base::paste0('metadata: ',logName,'_',Troll_SN,'_',Asset))
      if(length(Asset)<1){
        log$info(base::paste0('File Info: No asset specified in ',DirIn, '/', fileData_i))
      }
      Site <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Site"]
      if(length(Site)<1){
        log$info(base::paste0('File Info: No site specified in ',DirIn, '/', fileData_i))
      }else if(Site == 'Default Site'){
        Site <- NA
        log$info(base::paste0('File Info: Default site specified in ',DirIn, '/', fileData_i))
      }else if(length(Site)>1){
        log$info(base::paste0('File Info: More than one site specified in ',DirIn, '/', fileData_i))
      }else if(nchar(Site)>4){
        Site <-substr(Site,5,8)
      }
      Device <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device"]
      if(grepl('level',tolower(Device))){
        Device<-"Level TROLL 500"
        Context <- "surfacewater"
        sensor <- "leveltroll500"
        keep<-c('readout_time','seconds','pressure','temperature')
        log_data<-log_data[keep]
      }else if(grepl('aqua',tolower(Device))){
        Device<-"Aqua TROLL 200"
        sensor <- "aquatroll200"
        if(grepl("IN",logName)|grepl("Inlet",logName)|grepl("OT",logName)|grepl("Outlet",logName)|grepl("L1",logName)|
           grepl("L2",logName)|grepl("Lit",logName)|grepl("S1",logName)|grepl("S2",logName)){
          Context <- "surfacewater"
        }else if(grepl("GW",logName)|any(grepl("conductivity",tolower(colnames(log_data))))){
          Context <- "groundwater"
        }else{
          log$error(base::paste0('File Error: Context not specified in ',DirIn, '/', fileData_i))
        }
      }else{
        log$error(base::paste0('File Error: Device not specified in ',DirIn, '/', fileData_i))
      }
      
      
      ###check and update date format
      #sometimes ymd others mdy, sometimes has / others -, some don't have seconds
      #check if date contains seconds
      if(length(base::gregexpr(':', log_data$readout_time[1])[[1]])==2){ #if 2 : then it has seconds
        #Check if date begins with year (assume 4 digit year, which seems to always be true)
        if(grepl('^\\d{4}', log_data$readout_time[1])){
          #ymd format
          log_data$dateTime <- lubridate::ymd_hms(log_data$readout_time, tz = timezone)
        }else{
          #assume mdy format
          log_data$dateTime <- lubridate::mdy_hms(log_data$readout_time, tz = timezone)
        }
      }else if(length(base::gregexpr(':', log_data$readout_time[1])[[1]])==1){
        #doesn't have seconds
        #Check if date begins with year (assuming 4 digit year)
        if(grepl('^\\d{4}', log_data$readout_time[1])){
          #ymd format
          log_data$dateTime <- lubridate::ymd_hm(log_data$readout_time, tz = timezone)
        }else{
          #assume mdy format
          log_data$dateTime <- lubridate::mdy_hm(log_data$readout_time, tz = timezone)
        }
      }else{
        log$error(base::paste0('File Error: Invalid date time format',log_data$readout_time[1],' in ',DirIn, '/', fileData_i))#this shouldn't happen
      } 
      log_data<-log_data[!is.na(log_data$dateTime),]
      
      #add date as UTC
      log_data$dateUTC<-lubridate::with_tz(log_data$dateTime,'UTC')
      
      #check that dates are 2018 or later (some files have 1970 error)
      log_data$logFlag<-1
      log_data$logDateErrorFlag<-0
      
      if(any(log_data$dateUTC<"2018-01-01 00:00:00 UTC")){
        log$debug(base::paste0("Data contains dates prior to NEON logging implementation. Attempt will be made to align and flag data."))
        logDateError<-which(log_data$dateUTC<"2018-01-01 00:00:00 UTC")
        if(logDateError[1]!=1){ #If there is a good date before the 1970 shift we can try to continue the data and add a flag
          #for(k in logDateError[1]:logDateError[length(logDateError)]){
          for(k in 1:length(logDateError)){
            idx<-logDateError[k]
            if(Context=='groundwater'){
              log_data$dateUTC[idx] <- log_data$dateUTC[idx-1]+ 5*60
              log_data$logDateErrorFlag[idx]<-1
            }else if(Context=='surfacewater'){
              log_data$dateUTC[idx] <- log_data$dateUTC[idx-1]+ 60
              log_data$logDateErrorFlag[idx]<-1
            }
          }
        }else{
          #cannot use log data with bad dates
          #log$debug(base::paste0("Log data contains erroneous dates that cannot be linked to the correct time."))
          log_data<-log_data[log_data$dateUTC>"2018-01-01 00:00:00 UTC",]
          log$error(base::paste0('File Error: ALL DATA 1970 in ',DirIn, '/', fileData_i))
        }
      }
      log_data$readout_time<-log_data$dateUTC
      
      #round to minute
      if(Context=='surfacewater'){
        log_data$readout_time<-lubridate::round_date(log_data$dateUTC,unit = "minute")
      }else if(Context=='groundwater'){
        log_data$readout_time<-lubridate::round_date(log_data$dateUTC,unit = "5 minutes")
      }
      
      log_data$day<-lubridate::floor_date(log_data$dateUTC,"days")
      
      log_data$source_id<-Asset
      
      #format output file
      if(sensor=='aquatroll200'){
        out_columns <- c('source_id','readout_time','pressure','temperature','conductivity','logFlag','logDateErrorFlag','day')
      }else if(sensor=='leveltroll500'){
        out_columns <- c('source_id','readout_time','pressure','temperature','logFlag','logDateErrorFlag','day')
      }
      out<-log_data[out_columns]
      
      first_reading<-log_data$dateUTC[1]
      if(length(log_data$dateUTC)>0){
        last_reading<-log_data$dateUTC[length(log_data$dateUTC)]
      }else{
        last_reading<-NA
      }
      
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
          if(sensor=='aquatroll200'){
            out_file <- out_file[,c('source_id','readout_time','pressure','temperature','conductivity','logFlag','logDateErrorFlag')]
          }else if(sensor=='leveltroll500'){
            out_file <- out_file[,c('source_id','readout_time','pressure','temperature','logFlag','logDateErrorFlag')]
          }
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
        log$error(base::paste0('No days can be written out for ',DirIn, '/', fileData_i))
      }
    } #end of troll
  } #end of file loop
}
















