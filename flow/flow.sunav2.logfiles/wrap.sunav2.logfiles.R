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
#' @return Data from SUNA log files in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# FileIn <- "~/pfs/sunav2_logjam_load_files/20349/logjam_prod_20349_0b05a4c0da3bb05af840fece674fe34c.csv"
# DirOut="~/pfs/sunav2_logs_output"
# SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.sunav2.logfiles <- function(FileIn = "~/pfs/sunav2_logjam_load_files/20349/logjam_prod_20349_0b05a4c0da3bb05af840fece674fe34c.csv",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
#' @changelog
#' Nora Catolico (2024-01-09) original creation
#' Bobby Hensley (2025-04-09) adapted for SUNA 
##############################################################################################
wrap.sunav2.logfiles <- function(FileIn,
                             DirOut,
                             SchmDataOut=NULL,
                             log=NULL
){
  
#' Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

#' Load in the csv log file(s)
  logFile  <-
    base::try(read.table(paste0(FileIn), header = FALSE, sep = ",", 
                         col.names = paste0("V",seq_len(286)),encoding = 'utf-8',
                         stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
  if (base::any(base::class(logFile) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', FileIn, ' is unreadable. Likely not a data file.'))
    base::stop()
  }
  if(any(grepl('TROLL',logFile))){
    log$debug(base::paste0('skipping troll file: ', FileIn))
    base::stop()
  }else if(any(grepl('Turbidity',logFile))){
    log$debug(base::paste0('skipping sonde file: ', FileIn))
    base::stop()
  }
  
#' Find row where data actually starts
  start<-which(grepl('Zeiss Coefficient',logFile$V2))+1
  # Separate data and metadata
  logData<-logFile[start:(nrow(logFile)),]
  logMetadata<-logFile[1:(start-1),2:6]
  
#' Update names of existing columns to match avro schema
  names(logData)<-c("header_serial_number","year_and_day","time","nitrate_concentration","nitrogen_in_nitrate","absorbance_254nm","absorbance_350nm",
                     "bromide_trace","spectrum_average","dark_value_used_for_fit","integration_time_factor",
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
                     "internal_temperature","spectrometer_temperature","lamp_temperature","lamp_on_time","relative_humidity","main_voltage","lamp_voltage",
                     "internal_voltage","main_current","fit_aux_1","fit_aux_2","fit_base_1","fit_base_2","fit_rmse","ctd_time","ctd_salinity","ctd_temperature",
                     "ctd_pressure","check_sum")
  
#' Checks that each data burst is complete (Right now only checks whether last column is a value or not)
  logData$error_missing_data<-NA
  for(i in 1:nrow(logData)){if(is.na(logData[i,which(colnames(logData)=="check_sum")])){logData[i,which(colnames(logData)=="error_missing_data")]=TRUE}
    else{logData[i,which(colnames(logData)=="error_missing_data")]=FALSE}}
  
#' Combines all 256 spectrum channels into single array
  logData$spectrum_channels<-paste(logData$channel_1,logData$channel_2,logData$channel_3,logData$channel_4,logData$channel_5,logData$channel_6,logData$channel_7,logData$channel_8,logData$channel_9,logData$channel_10,
                                    logData$channel_11,logData$channel_12,logData$channel_13,logData$channel_14,logData$channel_15,logData$channel_16,logData$channel_17,logData$channel_18,logData$channel_19,logData$channel_20,
                                    logData$channel_21,logData$channel_22,logData$channel_23,logData$channel_24,logData$channel_25,logData$channel_26,logData$channel_27,logData$channel_28,logData$channel_29,logData$channel_30,
                                    logData$channel_31,logData$channel_32,logData$channel_33,logData$channel_34,logData$channel_35,logData$channel_36,logData$channel_37,logData$channel_38,logData$channel_39,logData$channel_40,
                                    logData$channel_41,logData$channel_42,logData$channel_43,logData$channel_44,logData$channel_45,logData$channel_46,logData$channel_47,logData$channel_48,logData$channel_49,logData$channel_50,
                                    logData$channel_51,logData$channel_52,logData$channel_53,logData$channel_54,logData$channel_55,logData$channel_56,logData$channel_57,logData$channel_58,logData$channel_59,logData$channel_60,
                                    logData$channel_61,logData$channel_62,logData$channel_63,logData$channel_64,logData$channel_65,logData$channel_66,logData$channel_67,logData$channel_68,logData$channel_69,logData$channel_70,
                                    logData$channel_71,logData$channel_72,logData$channel_73,logData$channel_74,logData$channel_75,logData$channel_76,logData$channel_77,logData$channel_78,logData$channel_79,logData$channel_80,
                                    logData$channel_81,logData$channel_82,logData$channel_83,logData$channel_84,logData$channel_85,logData$channel_86,logData$channel_87,logData$channel_88,logData$channel_89,logData$channel_90,
                                    logData$channel_91,logData$channel_92,logData$channel_93,logData$channel_94,logData$channel_95,logData$channel_96,logData$channel_97,logData$channel_98,logData$channel_99,logData$channel_100,
                                    logData$channel_101,logData$channel_102,logData$channel_103,logData$channel_104,logData$channel_105,logData$channel_106,logData$channel_107,logData$channel_108,logData$channel_109,logData$channel_110,
                                    logData$channel_111,logData$channel_112,logData$channel_113,logData$channel_114,logData$channel_115,logData$channel_116,logData$channel_117,logData$channel_118,logData$channel_119,logData$channel_120,
                                    logData$channel_121,logData$channel_122,logData$channel_123,logData$channel_124,logData$channel_125,logData$channel_126,logData$channel_127,logData$channel_128,logData$channel_129,logData$channel_130,
                                    logData$channel_131,logData$channel_132,logData$channel_133,logData$channel_134,logData$channel_135,logData$channel_136,logData$channel_137,logData$channel_138,logData$channel_139,logData$channel_140,
                                    logData$channel_141,logData$channel_142,logData$channel_143,logData$channel_144,logData$channel_145,logData$channel_146,logData$channel_147,logData$channel_148,logData$channel_149,logData$channel_150,
                                    logData$channel_151,logData$channel_152,logData$channel_153,logData$channel_154,logData$channel_155,logData$channel_156,logData$channel_157,logData$channel_158,logData$channel_159,logData$channel_160,
                                    logData$channel_161,logData$channel_162,logData$channel_163,logData$channel_164,logData$channel_165,logData$channel_166,logData$channel_167,logData$channel_168,logData$channel_169,logData$channel_170,
                                    logData$channel_171,logData$channel_172,logData$channel_173,logData$channel_174,logData$channel_175,logData$channel_176,logData$channel_177,logData$channel_178,logData$channel_179,logData$channel_180,
                                    logData$channel_181,logData$channel_182,logData$channel_183,logData$channel_184,logData$channel_185,logData$channel_186,logData$channel_187,logData$channel_188,logData$channel_189,logData$channel_190,
                                    logData$channel_191,logData$channel_192,logData$channel_193,logData$channel_194,logData$channel_195,logData$channel_196,logData$channel_197,logData$channel_198,logData$channel_199,logData$channel_200,
                                    logData$channel_201,logData$channel_202,logData$channel_203,logData$channel_204,logData$channel_205,logData$channel_206,logData$channel_207,logData$channel_208,logData$channel_209,logData$channel_210,
                                    logData$channel_211,logData$channel_212,logData$channel_213,logData$channel_214,logData$channel_215,logData$channel_216,logData$channel_217,logData$channel_218,logData$channel_219,logData$channel_220,
                                    logData$channel_221,logData$channel_222,logData$channel_223,logData$channel_224,logData$channel_225,logData$channel_226,logData$channel_227,logData$channel_228,logData$channel_229,logData$channel_230,
                                    logData$channel_231,logData$channel_232,logData$channel_233,logData$channel_234,logData$channel_235,logData$channel_236,logData$channel_237,logData$channel_238,logData$channel_239,logData$channel_240,
                                    logData$channel_241,logData$channel_242,logData$channel_243,logData$channel_244,logData$channel_245,logData$channel_246,logData$channel_247,logData$channel_248,logData$channel_249,logData$channel_250,
                                    logData$channel_251,logData$channel_252,logData$channel_253,logData$channel_254,logData$channel_255,logData$channel_256,sep=";")
  
#' Calculates the readout date and time in POSIXct format 
  logData$readout_time<-lubridate::parse_date_time(as.character(logData$year_and_day),order="yj") 
  op <- options(digits.secs=3)
  logData$readout_time<-lubridate::with_tz(logData$readout_time+(as.numeric(logData$time)*60*60),'UTC')
  
#' Create additional header columns needed to match avro schema
  asset_string <- regexpr("\\/[0-9]{5}\\/",FileIn) #' For SUNA asset info not included in log file header.  Need it from input file folder name.
  asset<-gsub("\\/","",substr(FileIn,asset_string[1],asset_string[1]+attributes(asset_string)$match.length-1))
  logData$source_id<-asset
  logData$site_id<-NA  #' This can be left blank for now
  serial_number<-as.data.frame(strsplit(logMetadata[1,2],":"))
  logData$header_manufacturer<-"SATS"
  logData$header_serial_number<-serial_number[2,1]
  logData$header_light_frame<-NA
  for(i in 1:nrow(logData)){if(logData[i,which(colnames(logData)=="dark_value_used_for_fit")]==0){logData[i,which(colnames(logData)=="header_light_frame")]=0}
    else{logData[i,which(colnames(logData)=="header_light_frame")]=1}}
  
#' Re-orders columns so they match the avro schema
  logData<-logData[,c("source_id","site_id","readout_time","header_manufacturer","header_serial_number","header_light_frame","year_and_day","time","nitrate_concentration",
                      "nitrogen_in_nitrate","absorbance_254nm","absorbance_350nm","bromide_trace","spectrum_average","dark_value_used_for_fit","integration_time_factor",
                      "spectrum_channels","internal_temperature","spectrometer_temperature","lamp_temperature","lamp_on_time","relative_humidity","main_voltage","lamp_voltage",
                      "internal_voltage","main_current","fit_aux_1","fit_aux_2","fit_base_1","fit_base_2","fit_rmse","ctd_time","ctd_salinity","ctd_temperature","ctd_pressure",
                      "check_sum","error_missing_data")]
  
#' Checks that there are no dates prior to when NEON began collecting IS data
  if(any(logData$readout_time<"2014-01-01 00:00:00 UTC")){
    log$debug(base::paste0("Data contains dates prior to when NEON began collecting IS data"))}
#' Checks that there are no future dates after the current date
  if(any(logData$readout_time>Sys.time())){
    log$debug(base::paste0("Data contains future dates after the current date"))}
  
#' Output file
    #' Create output directory
      year <- substr(logData$readout_time[1],1,4)
      month <- substr(logData$readout_time[1],6,7)
      day <- substr(logData$readout_time[1],9,10)
      DirOutLogFile <- paste0(DirOut,'/sunav2/',year,'/',month,'/',day,'/',asset,'/data/')
      base::dir.create(DirOutLogFile,recursive=TRUE)
      csv_name <-paste0('sunav2_',asset,'_',year,'-',month,'-',day,'_log')
    #' Writes parquet file to output directory
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = logData,
                                                   NameFile = base::paste0(DirOutLogFile,csv_name,".parquet"),
                                                   Schm = SchmDataOut),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){
        log$error(base::paste0('Cannot write Data to ',base::paste0(DirOutLogFile,csv_name,".parquet"),'. ',attr(rptOut, "condition")))
        stop()
      } else {
        log$info(base::paste0('Data written successfully in ', base::paste0(DirOutLogFile,csv_name,".parquet")))
      }
      
} 
#' End of file

















