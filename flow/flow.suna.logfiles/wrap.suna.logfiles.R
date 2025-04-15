##############################################################################################
#' @title Wrapper for SUNA Log File Processing

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Validates, cleans, and formats SUNA log files into daily parquets.
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
#' @return Cleaned SUNA log files in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
#' FileIn <- "~/pfs/suna_logjam_load_files/20349/logjam_prod_20349_0b05a4c0da3bb05af840fece674fe34c.csv"
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.suna.logfiles <- function(FileIn = "~/pfs/suna_logjam_load_files/20349/logjam_prod_20349_0b05a4c0da3bb05af840fece674fe34c.csv",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#' Nora Catolico (2024-01-09) original creation
#' Bobby Hensley (2025-04-09) adapted for SUNA 
##############################################################################################
wrap.suna.logfiles <- function(FileIn,
                             DirOut,
                             SchmDataOut=NULL,
                             log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Load in the csv log file(s)
  log_file  <-
    base::try(read.table(paste0(FileIn), header = FALSE, sep = ",", 
                         col.names = paste0("V",seq_len(286)),encoding = 'utf-8',
                         stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
  if (base::any(base::class(log_file) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', FileIn, ' is unreadable. Likely not a data file.'))
    base::stop()
  }
  if(any(grepl('TROLL',log_file))){
    log$debug(base::paste0('skipping troll file: ', FileIn))
    base::stop()
  }else if(any(grepl('Turbidity',log_file))){
    log$debug(base::paste0('skipping sonde file: ', FileIn))
    base::stop()
  }
  
  # Find row where data actually starts
  start<-which(grepl('Zeiss Coefficient',log_file$V2))+1
  # Separate data and metadata
  log_data<-log_file[start:(nrow(log_file)),]
  log_metadata<-log_file[1:(start-1),2:6]
  # Create column names for data
  names(log_data)<-c("serial_number","date","time","nitrate_uM","nitrate_mgL","absorbance_254","absorbance_350","bromide",
                     "spec_avg","dark_value","int_time_factor",
                     "channel_1","channel_2","channel_3","channel_4","channel_5","channel_6","channel_7","channel_8","channel_9","channel_10",
                     "channel_11","channel_12","channel_13","channel_14","channel_15","channel_16","channel_17","channel_18","channel_19","channel_20",
                     "channel_21","channel_22","channel_23","channel_24","channel_25","channel_26","channel_27","channel_28","channel_29","channel_30",
                     "channel_31","channel_32","channel_33","channel_34","channel_35","channel_36","channel_37","channel_38","channel_39","channel_40",
                     "channel_41","channel_42","channel_43","channel_44","channel_45","channel_46","channel_47","channel_48","channel_49","channel_50",
                     "channel_51","channel_52","channel_53","channel_54","channel_55","channel_56","channel_57","channel_58","channel_59","channel_60",
                     "channel_61","channel_62","channel_63","channel_64","channel_65","channel_66","channel_67","channel_68","channel_69","channel_70",
                     "channel_71","channel_72","channel_73","channel_74","channel_75","channel_76","channel_77","channel_78","channel_79","channel_80",
                     "channel_81","channel_82","channel_83","channel_84","channel_85","channel_86","channel_87","channel_88","channel_89","channel_90",
                     "channel_91","channel_92","channel_93","channel_94","channel_95","channel_96","channel_97","channel_98","channel_99","channel_100",
                     "channel_101","channel_102","channel_103","channel_104","channel_105","channel_106","channel_107","channel_108","channel_109","channel_110",
                     "channel_111","channel_112","channel_113","channel_114","channel_115","channel_116","channel_117","channel_118","channel_119","channel_120",
                     "channel_121","channel_122","channel_123","channel_124","channel_125","channel_126","channel_127","channel_128","channel_129","channel_130",
                     "channel_131","channel_132","channel_133","channel_134","channel_135","channel_136","channel_137","channel_138","channel_139","channel_140",
                     "channel_141","channel_142","channel_143","channel_144","channel_145","channel_146","channel_147","channel_148","channel_149","channel_150",
                     "channel_151","channel_152","channel_153","channel_154","channel_155","channel_156","channel_157","channel_158","channel_159","channel_160",
                     "channel_161","channel_162","channel_163","channel_164","channel_165","channel_166","channel_167","channel_168","channel_169","channel_170",
                     "channel_171","channel_172","channel_173","channel_174","channel_175","channel_176","channel_177","channel_178","channel_179","channel_180",
                     "channel_181","channel_182","channel_183","channel_184","channel_185","channel_186","channel_187","channel_188","channel_189","channel_190",
                     "channel_191","channel_192","channel_193","channel_194","channel_195","channel_196","channel_197","channel_198","channel_199","channel_200",
                     "channel_201","channel_202","channel_203","channel_204","channel_205","channel_206","channel_207","channel_208","channel_209","channel_210",
                     "channel_211","channel_212","channel_213","channel_214","channel_215","channel_216","channel_217","channel_218","channel_219","channel_220",
                     "channel_221","channel_222","channel_223","channel_224","channel_225","channel_226","channel_227","channel_228","channel_229","channel_230",
                     "channel_231","channel_232","channel_233","channel_234","channel_235","channel_236","channel_237","channel_238","channel_239","channel_240",
                     "channel_241","channel_242","channel_243","channel_244","channel_245","channel_246","channel_247","channel_248","channel_249","channel_250",
                     "channel_251","channel_252","channel_253","channel_254","channel_255","channel_256",
                     "internal_temp","spec_temp","lamp_temp","cum_lamp_time","humidity","main_volt","lamp_volt","internal_volt","current","fit_aux_1","fit_aux_2",
                     "fit_base_1","fit_base_2","fit_RMSE","ctd_time","ctd_salinity","ctd_temp","ctd_pressure","check_sum")
  
  # Gets metadata
  sensor<-"suna"
  serial_number<-log_metadata[1,2]
  eprom<-"20349"     #' Need to figure out a way to get this from the folder name the file came from since it's not included in the file itself
  
  # Calculates the date and time in POSIXct format 
  log_data$date<-lubridate::parse_date_time(as.character(log_data$date),order="yj") 
  log_data$date<-lubridate::with_tz(log_data$date+(as.numeric(log_data$time)*60*60),'UTC')
  # Checks that there are no dates prior to when NEON began collecting IS data
  if(any(log_data$date<"2014-01-01 00:00:00 UTC")){
    log$debug(base::paste0("Data contains dates prior to when NEON began collecting IS data"))}
  
  # Output file
      # Create output directory
      year <- substr(log_data$date[1],1,4)
      month <- substr(log_data$date[1],6,7)
      day <- substr(log_data$date[1],9,10)
      DirOutLogFile <- paste0(DirOut,'/',sensor,'/',year,'/',month,'/',day,'/',eprom,'/data/')
      base::dir.create(DirOutLogFile,recursive=TRUE)
      csv_name <-paste0(sensor,'_',eprom,'_',year,'-',month,'-',day,'_log')
      # Writes parquet file to output directory
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = log_data,
                                                   NameFile = base::paste0(DirOutLogFile,csv_name,".parquet"),
                                                   Schm = SchmDataOut),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){
        log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutLogFile,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {
        log$info(base::paste0('Data written successfully in ', base::paste0(DirOutLogFile,csv_name,".parquet")))
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
      log$error(base::paste0('File Error: timezone not specified in ', FileIn))
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
    logName <- log_metadata$value[!is.na(log_metadata$label) & (log_metadata$label=="Log Name"|log_metadata$label=="File Name")][1]
    Troll_SN <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Serial Number"][1]
    Asset <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device Name"][1]
    #log$debug(base::paste0('metadata: ',logName,'_',Troll_SN,'_',Asset))
    if(length(Asset)<1 || Asset == " " || nchar(Asset) == 0){
      log$error(base::paste0('File Info: No asset specified in ', FileIn))
      stop()
    }
    #define Site
    Site <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Site"]
    if(length(Site)<1){
      log$info(base::paste0('File Info: No site specified in ', FileIn))
    }else if(Site == 'Default Site'){
      Site <- NA
      log$info(base::paste0('File Info: Default site specified in ', FileIn))
    }else if(length(Site)>1){
      log$info(base::paste0('File Info: More than one site specified in ', FileIn))
    }else if(nchar(Site)>4){
      Site <-substr(Site,5,8)
    }
    #fix for specific use case
    if(grepl('Central America Standard Time',timezone) & !is.na(Site) & (Site == "MCDI"|Site == "KING")){
      timezone<-'US/Central'
    }  
    Device <- log_metadata$value[!is.na(log_metadata$label) & log_metadata$label=="Device"][1]
    if(!is.na(Device) & grepl('level',tolower(Device))){
      Device<-"Level TROLL 500"
      Context <- "surfacewater"
      sensor <- "leveltroll500"
      keep<-c('readout_time','seconds','pressure','temperature')
      log_data<-log_data[keep]
    }else if(!is.na(Device) & grepl('aqua',tolower(Device))){
      Device<-"Aqua TROLL 200"
      sensor <- "aquatroll200"
      if(!is.na(logName) & (grepl("IN",logName)|grepl("Inlet",logName)|grepl("OT",logName)|grepl("Outlet",logName)|grepl("L1",logName)|
                            grepl("L2",logName)|grepl("Lit",logName)|grepl("S1",logName)|grepl("S2",logName))){
        Context <- "surfacewater"
      }else if(!is.na(logName) & (grepl("GW",logName)|any(grepl("conductivity",tolower(colnames(log_data)))))){
        Context <- "groundwater"
      }else{
        log$error(base::paste0('File Error: Context not specified in ', FileIn))
      }
    }else{
      log$error(base::paste0('File Error: Device not specified in ', FileIn))
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
      log$error(base::paste0('File Error: Invalid date time format',log_data$readout_time[1],' in ', FileIn))#this shouldn't happen
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
        # sampling frequency
        if(Context=='groundwater'){
          freq <- 300 #5 min in seconds
        }else{
          freq <- 60
        }
        idx_start<-logDateError[1]
        idx_end<-logDateError[length(logDateError)]
        time_last_read<-log_data$dateUTC[idx_start-1]
        first_new_time<-time_last_read + freq
        num_readings<-length(logDateError)
        new_times <- seq(first_new_time, by = freq, length = num_readings)
        log_data$dateUTC[idx_start:idx_end]<-new_times
        log_data$logDateErrorFlag[idx_start:idx_end]<-1
      }else{
        #cannot use log data with bad dates
        #log$debug(base::paste0("Log data contains erroneous dates that cannot be linked to the correct time."))
        log_data<-log_data[log_data$dateUTC>"2018-01-01 00:00:00 UTC",]
        log$debug(base::paste0('File Error: ALL DATA 1970 in ', FileIn))
      }
    }
    if(nrow(log_data)>0){
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
      #create any missing columns in log file
      if(!'pressure' %in% names(log_data)){log_data$pressure<-NA}
      if(!'temperature' %in% names(log_data)){log_data$temperature<-NA}
      if(sensor=='aquatroll200'){
        if(!'conductivity' %in% names(log_data)){log_data$conductivity<-NA}
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
        log$error(base::paste0('No days can be written out for ', FileIn))
      }
    }
  }
} #end of file

















