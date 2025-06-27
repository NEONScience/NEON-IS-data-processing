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
DirInLogs<-"~/pfs/sunav2_logs_output/sunav2/2024/09/10/20349" #cleaned log data
DirInStream<-"~/pfs/sunav2_data_source_trino/sunav2/2024/09/10/20349" #streamed L0 data
DirIn<-NULL
DirOutBase="~/pfs/sunav2_filled_output"
SchmDataOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2.avsc'),collapse='')
log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# SchmFlagsOut<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'),collapse='')
# wrap.troll.logfiles.fill(
#   DirInLogs=DirInLogs,
#   DirInStream=DirInStream,
#   DirIn=DirIn,
#   DirOutBase="~/pfs/out",
#   SchmDataOut="~/pfs/aquatroll200_avro_schemas/aquatroll200/aquatroll200_log_data.avsc",
#   SchmFlagsOut=SchmFlagsOut,
#   log=log)
#'                               
#' @changelog
#' Nora Catolico (2024-01-30) original creation
#' Bobby Hensley (2025-05-30) adapted for suna
#' 
##############################################################################################
wrap.suna.logfiles.fill <- function(DirInLogs=NULL,
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
    }
  
#' Parse serial output into individual columns   
  L0DataParsed<-tidyr::separate(data = L0Data,col = serial_output,sep = ",|:", 
    into = c("pos_0","header_light_frame","pos_1","year_and_day","pos_2","time","pos_3","nitrate_concentration","pos_4","nitrogen_in_nitrate",
             "pos_5","absorbance_254nm","pos_6","absorbance_350nm","pos_7","bromide_trace","pos_8","spectrum_average","pos_9","dark_value_used_for_fit","pos_10","integration_time_factor",
             "pos_11","channel_1","pos_12","channel_2","pos_13","channel_3","pos_14","channel_4","pos_15","channel_5",
             "pos_16","channel_6","pos_17","channel_7","pos_18","channel_8","pos_19","channel_9","pos_20","channel_10",
             "pos_21","channel_11","pos_22","channel_12","pos_23","channel_13","pos_24","channel_14","pos_25","channel_15",
             "pos_26","channel_16","pos_27","channel_17","pos_28","channel_18","pos_29","channel_19","pos_30","channel_20",
             "pos_31","channel_21","pos_32","channel_22","pos_33","channel_23","pos_34","channel_24","pos_35","channel_25",
             "pos_36","channel_26","pos_37","channel_27","pos_38","channel_28","pos_39","channel_29","pos_40","channel_30",
             "pos_41","channel_31","pos_42","channel_32","pos_43","channel_33","pos_44","channel_34","pos_45","channel_35",
             "pos_46","channel_36","pos_47","channel_37","pos_48","channel_38","pos_49","channel_39","pos_50","channel_40",
             "pos_51","channel_41","pos_52","channel_42","pos_53","channel_43","pos_54","channel_44","pos_55","channel_45",
             "pos_56","channel_46","pos_57","channel_47","pos_58","channel_48","pos_59","channel_49","pos_60","channel_50",
             "pos_61","channel_51","pos_62","channel_52","pos_63","channel_53","pos_64","channel_54","pos_65","channel_55",
             "pos_66","channel_56","pos_67","channel_57","pos_68","channel_58","pos_69","channel_59","pos_70","channel_60",
             "pos_71","channel_61","pos_72","channel_62","pos_73","channel_63","pos_74","channel_64","pos_75","channel_65",
             "pos_76","channel_66","pos_77","channel_67","pos_78","channel_68","pos_79","channel_69","pos_80","channel_70",
             "pos_81","channel_71","pos_82","channel_72","pos_83","channel_73","pos_84","channel_74","pos_85","channel_75",
             "pos_86","channel_76","pos_87","channel_77","pos_88","channel_78","pos_89","channel_79","pos_90","channel_80",
             "pos_91","channel_81","pos_92","channel_82","pos_93","channel_83","pos_94","channel_84","pos_95","channel_85",
             "pos_96","channel_86","pos_97","channel_87","pos_98","channel_88","pos_99","channel_89","pos_100","channel_90",
             "pos_101","channel_91","pos_102","channel_92","pos_103","channel_93","pos_104","channel_94","pos_105","channel_95",
             "pos_106","channel_96","pos_107","channel_97","pos_108","channel_98","pos_109","channel_99","pos_110","channel_100",
             "pos_111","channel_101","pos_112","channel_102","pos_113","channel_103","pos_114","channel_104","pos_115","channel_105",
             "pos_116","channel_106","pos_117","channel_107","pos_118","channel_108","pos_119","channel_109","pos_120","channel_110",
             "pos_121","channel_111","pos_122","channel_112","pos_123","channel_113","pos_124","channel_114","pos_125","channel_115",
             "pos_126","channel_116","pos_127","channel_117","pos_128","channel_118","pos_129","channel_119","pos_130","channel_120",
             "pos_131","channel_121","pos_132","channel_122","pos_133","channel_123","pos_134","channel_124","pos_135","channel_125",
             "pos_136","channel_126","pos_137","channel_127","pos_138","channel_128","pos_139","channel_129","pos_140","channel_130",
             "pos_141","channel_131","pos_142","channel_132","pos_143","channel_133","pos_144","channel_134","pos_145","channel_135",
             "pos_146","channel_136","pos_147","channel_137","pos_148","channel_138","pos_149","channel_139","pos_150","channel_140",
             "pos_151","channel_141","pos_152","channel_142","pos_153","channel_143","pos_154","channel_144","pos_155","channel_145",
             "pos_156","channel_146","pos_157","channel_147","pos_158","channel_148","pos_159","channel_149","pos_160","channel_150",
             "pos_161","channel_151","pos_162","channel_152","pos_163","channel_153","pos_164","channel_154","pos_165","channel_155",
             "pos_166","channel_156","pos_167","channel_157","pos_168","channel_158","pos_169","channel_159","pos_170","channel_160",
             "pos_171","channel_161","pos_172","channel_162","pos_173","channel_163","pos_174","channel_164","pos_175","channel_165",
             "pos_176","channel_166","pos_177","channel_167","pos_178","channel_168","pos_179","channel_169","pos_180","channel_170",
             "pos_181","channel_171","pos_182","channel_172","pos_183","channel_173","pos_184","channel_174","pos_185","channel_175",
             "pos_186","channel_176","pos_187","channel_177","pos_188","channel_178","pos_189","channel_179","pos_190","channel_180",
             "pos_191","channel_181","pos_192","channel_182","pos_193","channel_183","pos_194","channel_184","pos_195","channel_185",
             "pos_196","channel_186","pos_197","channel_187","pos_198","channel_188","pos_199","channel_189","pos_200","channel_190",
             "pos_201","channel_191","pos_202","channel_192","pos_203","channel_193","pos_204","channel_194","pos_205","channel_195",
             "pos_206","channel_196","pos_207","channel_197","pos_208","channel_198","pos_209","channel_199","pos_210","channel_200",
             "pos_211","channel_201","pos_212","channel_202","pos_213","channel_203","pos_214","channel_204","pos_215","channel_205",
             "pos_216","channel_206","pos_217","channel_207","pos_218","channel_208","pos_219","channel_209","pos_220","channel_210",
             "pos_221","channel_211","pos_222","channel_212","pos_223","channel_213","pos_224","channel_214","pos_225","channel_215",
             "pos_226","channel_216","pos_227","channel_217","pos_228","channel_218","pos_229","channel_219","pos_230","channel_220",
             "pos_231","channel_221","pos_232","channel_222","pos_233","channel_223","pos_234","channel_224","pos_235","channel_225",
             "pos_236","channel_226","pos_237","channel_227","pos_238","channel_228","pos_239","channel_229","pos_240","channel_230",
             "pos_241","channel_231","pos_242","channel_232","pos_243","channel_233","pos_244","channel_234","pos_245","channel_235",
             "pos_246","channel_236","pos_247","channel_237","pos_248","channel_238","pos_249","channel_239","pos_250","channel_240",
             "pos_251","channel_241","pos_252","channel_242","pos_253","channel_243","pos_254","channel_244","pos_255","channel_245",
             "pos_256","channel_246","pos_257","channel_247","pos_258","channel_248","pos_259","channel_249","pos_260","channel_250",
             "pos_261","channel_251","pos_262","channel_252","pos_263","channel_253","pos_264","channel_254","pos_265","channel_255",
             "pos_266","channel_256",
             "pos_267","internal_temperature","pos_268","spectrometer_temperature","pos_269","lamp_temperature","pos_270","lamp_on_time",
             "pos_271","relative_humidity","pos_272","main_voltage","pos_273","lamp_voltage","pos_274","internal_voltage",
             "pos_275","main_current","pos_276","fit_aux_1","pos_277","fit_aux_2","pos_278","fit_base_1","pos_279","fit_base_2",
             "pos_280","fit_rmse","pos_281","ctd_time","pos_282","ctd_salinity","pos_283","ctd_temperature",
             "pos_284","ctd_pressure","pos_285","check_sum"))  
 
#' Drops serial output position columns
  L0DataParsed<-L0DataParsed[!grepl("pos",names(L0DataParsed))]  

#' Combines all 256 spectrum channels into single array
  L0DataParsed$spectrum_channels<-paste(L0DataParsed$channel_1,L0DataParsed$channel_2,L0DataParsed$channel_3,L0DataParsed$channel_4,L0DataParsed$channel_5,L0DataParsed$channel_6,L0DataParsed$channel_7,L0DataParsed$channel_8,L0DataParsed$channel_9,L0DataParsed$channel_10,
                                 L0DataParsed$channel_11,L0DataParsed$channel_12,L0DataParsed$channel_13,L0DataParsed$channel_14,L0DataParsed$channel_15,L0DataParsed$channel_16,L0DataParsed$channel_17,L0DataParsed$channel_18,L0DataParsed$channel_19,L0DataParsed$channel_20,
                                 L0DataParsed$channel_21,L0DataParsed$channel_22,L0DataParsed$channel_23,L0DataParsed$channel_24,L0DataParsed$channel_25,L0DataParsed$channel_26,L0DataParsed$channel_27,L0DataParsed$channel_28,L0DataParsed$channel_29,L0DataParsed$channel_30,
                                 L0DataParsed$channel_31,L0DataParsed$channel_32,L0DataParsed$channel_33,L0DataParsed$channel_34,L0DataParsed$channel_35,L0DataParsed$channel_36,L0DataParsed$channel_37,L0DataParsed$channel_38,L0DataParsed$channel_39,L0DataParsed$channel_40,
                                 L0DataParsed$channel_41,L0DataParsed$channel_42,L0DataParsed$channel_43,L0DataParsed$channel_44,L0DataParsed$channel_45,L0DataParsed$channel_46,L0DataParsed$channel_47,L0DataParsed$channel_48,L0DataParsed$channel_49,L0DataParsed$channel_50,
                                 L0DataParsed$channel_51,L0DataParsed$channel_52,L0DataParsed$channel_53,L0DataParsed$channel_54,L0DataParsed$channel_55,L0DataParsed$channel_56,L0DataParsed$channel_57,L0DataParsed$channel_58,L0DataParsed$channel_59,L0DataParsed$channel_60,
                                 L0DataParsed$channel_61,L0DataParsed$channel_62,L0DataParsed$channel_63,L0DataParsed$channel_64,L0DataParsed$channel_65,L0DataParsed$channel_66,L0DataParsed$channel_67,L0DataParsed$channel_68,L0DataParsed$channel_69,L0DataParsed$channel_70,
                                 L0DataParsed$channel_71,L0DataParsed$channel_72,L0DataParsed$channel_73,L0DataParsed$channel_74,L0DataParsed$channel_75,L0DataParsed$channel_76,L0DataParsed$channel_77,L0DataParsed$channel_78,L0DataParsed$channel_79,L0DataParsed$channel_80,
                                 L0DataParsed$channel_81,L0DataParsed$channel_82,L0DataParsed$channel_83,L0DataParsed$channel_84,L0DataParsed$channel_85,L0DataParsed$channel_86,L0DataParsed$channel_87,L0DataParsed$channel_88,L0DataParsed$channel_89,L0DataParsed$channel_90,
                                 L0DataParsed$channel_91,L0DataParsed$channel_92,L0DataParsed$channel_93,L0DataParsed$channel_94,L0DataParsed$channel_95,L0DataParsed$channel_96,L0DataParsed$channel_97,L0DataParsed$channel_98,L0DataParsed$channel_99,L0DataParsed$channel_100,
                                 L0DataParsed$channel_101,L0DataParsed$channel_102,L0DataParsed$channel_103,L0DataParsed$channel_104,L0DataParsed$channel_105,L0DataParsed$channel_106,L0DataParsed$channel_107,L0DataParsed$channel_108,L0DataParsed$channel_109,L0DataParsed$channel_110,
                                 L0DataParsed$channel_111,L0DataParsed$channel_112,L0DataParsed$channel_113,L0DataParsed$channel_114,L0DataParsed$channel_115,L0DataParsed$channel_116,L0DataParsed$channel_117,L0DataParsed$channel_118,L0DataParsed$channel_119,L0DataParsed$channel_120,
                                 L0DataParsed$channel_121,L0DataParsed$channel_122,L0DataParsed$channel_123,L0DataParsed$channel_124,L0DataParsed$channel_125,L0DataParsed$channel_126,L0DataParsed$channel_127,L0DataParsed$channel_128,L0DataParsed$channel_129,L0DataParsed$channel_130,
                                 L0DataParsed$channel_131,L0DataParsed$channel_132,L0DataParsed$channel_133,L0DataParsed$channel_134,L0DataParsed$channel_135,L0DataParsed$channel_136,L0DataParsed$channel_137,L0DataParsed$channel_138,L0DataParsed$channel_139,L0DataParsed$channel_140,
                                 L0DataParsed$channel_141,L0DataParsed$channel_142,L0DataParsed$channel_143,L0DataParsed$channel_144,L0DataParsed$channel_145,L0DataParsed$channel_146,L0DataParsed$channel_147,L0DataParsed$channel_148,L0DataParsed$channel_149,L0DataParsed$channel_150,
                                 L0DataParsed$channel_151,L0DataParsed$channel_152,L0DataParsed$channel_153,L0DataParsed$channel_154,L0DataParsed$channel_155,L0DataParsed$channel_156,L0DataParsed$channel_157,L0DataParsed$channel_158,L0DataParsed$channel_159,L0DataParsed$channel_160,
                                 L0DataParsed$channel_161,L0DataParsed$channel_162,L0DataParsed$channel_163,L0DataParsed$channel_164,L0DataParsed$channel_165,L0DataParsed$channel_166,L0DataParsed$channel_167,L0DataParsed$channel_168,L0DataParsed$channel_169,L0DataParsed$channel_170,
                                 L0DataParsed$channel_171,L0DataParsed$channel_172,L0DataParsed$channel_173,L0DataParsed$channel_174,L0DataParsed$channel_175,L0DataParsed$channel_176,L0DataParsed$channel_177,L0DataParsed$channel_178,L0DataParsed$channel_179,L0DataParsed$channel_180,
                                 L0DataParsed$channel_181,L0DataParsed$channel_182,L0DataParsed$channel_183,L0DataParsed$channel_184,L0DataParsed$channel_185,L0DataParsed$channel_186,L0DataParsed$channel_187,L0DataParsed$channel_188,L0DataParsed$channel_189,L0DataParsed$channel_190,
                                 L0DataParsed$channel_191,L0DataParsed$channel_192,L0DataParsed$channel_193,L0DataParsed$channel_194,L0DataParsed$channel_195,L0DataParsed$channel_196,L0DataParsed$channel_197,L0DataParsed$channel_198,L0DataParsed$channel_199,L0DataParsed$channel_200,
                                 L0DataParsed$channel_201,L0DataParsed$channel_202,L0DataParsed$channel_203,L0DataParsed$channel_204,L0DataParsed$channel_205,L0DataParsed$channel_206,L0DataParsed$channel_207,L0DataParsed$channel_208,L0DataParsed$channel_209,L0DataParsed$channel_210,
                                 L0DataParsed$channel_211,L0DataParsed$channel_212,L0DataParsed$channel_213,L0DataParsed$channel_214,L0DataParsed$channel_215,L0DataParsed$channel_216,L0DataParsed$channel_217,L0DataParsed$channel_218,L0DataParsed$channel_219,L0DataParsed$channel_220,
                                 L0DataParsed$channel_221,L0DataParsed$channel_222,L0DataParsed$channel_223,L0DataParsed$channel_224,L0DataParsed$channel_225,L0DataParsed$channel_226,L0DataParsed$channel_227,L0DataParsed$channel_228,L0DataParsed$channel_229,L0DataParsed$channel_230,
                                 L0DataParsed$channel_231,L0DataParsed$channel_232,L0DataParsed$channel_233,L0DataParsed$channel_234,L0DataParsed$channel_235,L0DataParsed$channel_236,L0DataParsed$channel_237,L0DataParsed$channel_238,L0DataParsed$channel_239,L0DataParsed$channel_240,
                                 L0DataParsed$channel_241,L0DataParsed$channel_242,L0DataParsed$channel_243,L0DataParsed$channel_244,L0DataParsed$channel_245,L0DataParsed$channel_246,L0DataParsed$channel_247,L0DataParsed$channel_248,L0DataParsed$channel_249,L0DataParsed$channel_250,
                                 L0DataParsed$channel_251,L0DataParsed$channel_252,L0DataParsed$channel_253,L0DataParsed$channel_254,L0DataParsed$channel_255,L0DataParsed$channel_256,sep=";")

#' Checks that each data burst is complete (Right now only checks whether last column is a value or not)
  L0DataParsed$error_missing_data<-NA
  for(i in 1:nrow(L0DataParsed)){if(is.na(L0DataParsed[i,which(colnames(L0DataParsed)=="check_sum")])){L0DataParsed[i,which(colnames(L0DataParsed)=="error_missing_data")]=TRUE}
    else{L0DataParsed[i,which(colnames(L0DataParsed)=="error_missing_data")]=FALSE}}

#' Create additional header columns needed to match avro schema
  L0DataParsed$header_manufacturer<-"SATS"
  L0DataParsed$header_serial_number<-NA  #' Can leave this blank for now 
  
#' Re-orders columns so they match the avro schema
  L0DataParsed<-L0DataParsed[,c("source_id","site_id","readout_time","header_manufacturer","header_serial_number","header_light_frame","year_and_day","time","nitrate_concentration",
                      "nitrogen_in_nitrate","absorbance_254nm","absorbance_350nm","bromide_trace","spectrum_average","dark_value_used_for_fit","integration_time_factor",
                      "spectrum_channels","internal_temperature","spectrometer_temperature","lamp_temperature","lamp_on_time","relative_humidity","main_voltage","lamp_voltage",
                      "internal_voltage","main_current","fit_aux_1","fit_aux_2","fit_base_1","fit_base_2","fit_rmse","ctd_time","ctd_salinity","ctd_temperature","ctd_pressure",
                      "check_sum","error_missing_data")]
  
#' Determine whether to use logged or streamed data.  
  #' Preference is to use logged data if available
  if(!is.null(logData)){dataOut<-logData}
  if(is.null(logData) & !is.null(L0DataParsed)){dataOut<-L0DataParsed}
  if(is.null(logData) & is.null(L0DataParsed)){dataOut<-L0DataParsed}
  
#' Write out data  
  
  

  
  
  
  
  
  
  #write out data
  fileOutSplt <- base::strsplit(DirInStream,'[/]')[[1]] # Separate underscore-delimited components of the file name
  asset<-tail(x=fileOutSplt,n=1)
  csv_name <-paste0('sunav2_',asset,'_',format(timeBgn,format = "%Y-%m-%d"),'_filled')
  
  rptOut <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                               NameFile = base::paste0(DirOutData,'/',csv_name,".parquet"),
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
                                               NameFile = base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),
                                               Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',base::paste0(DirOutFlags,'/',csv_name_flags,".parquet"),'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', base::paste0(DirOutFlags,'/',csv_name_flags,".parquet")))
  }

}
















