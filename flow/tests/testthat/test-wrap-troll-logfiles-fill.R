##############################################################################################
#' @title Unit test for Wrapper for Wrapper for Troll Log File Comparison and Gap Filling
#' 
#' @description Wrapper function. Compares logged data to streamed data and fills gaps.
#'
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
# DirInLogs<-'~/pfs/logjam_clean_troll_files/leveltroll500/2022/04/04/21115' #cleaned log data
# DirInStream<-'~/pfs/leveltroll500_data_source_trino/leveltroll500/2022/04/04/21115' #streamed L0 data
# DirIn<-'~/pfs/logjam_clean_troll_files/leveltroll500/2022/04/08/21115'
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# wrap.troll.logfiles.fill <- function(DirInLogs=DirInLogs,
#                               DirInStream=DirInStream,
#                               DirIn=DirIn,
#                               DirOutBase="~/pfs/out",
#                               SchmDataOut=NULL,
#                               SchmFlagsOut=NULL,
#                               log=log)
#'
# changelog and author contributions 
#   Mija Choi (2024-04-08)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.logfiles.fill.R\n")

# Unit test of wrap.troll.logfiles.R
test_that("Unit test of wrap.troll.logfiles.fill.R", {
  
  source('../../flow.troll.logfiles.fill/wrap.troll.logfiles.fill.R')
  library(stringr)
  
  workingDirPath <- getwd()
  testDirOut = file.path(workingDirPath, 'pfs/out')
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  WndwAgr_1 <- base::as.difftime(1,units="mins")
  WndwAgr_5 <- base::as.difftime(5,units="mins")
  timeBgnDiff <- list()
  timeEndDiff <- list()
  timeBinDiff_1 <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr_1,WndwTime=base::as.difftime(1,units='days'))
  timeBgnDiff_1 <- timeBinDiff_1$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff_1 <- timeBinDiff_1$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  timeBgnDiff_5 <- list()
  timeEndDiff_5 <- list()
  timeBinDiff_5 <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr_5,WndwTime=base::as.difftime(1,units='days'))
  timeBgnDiff_5 <- timeBinDiff_5$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff_5 <- timeBinDiff_5$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  #
  # Test 1. Leveltroll500, only the input of directories, resistance and voltage, and output directry are passed in
  #
  DirInLogs<-file.path(workingDirPath, 'pfs/logjam_clean_troll_files/leveltroll500/2022/04/04/21115') 
  DirInStream<-file.path(workingDirPath, 'pfs/leveltroll500_data_source_trino/leveltroll500/2022/04/04/21115')
  DirIn<-'pfs/logjam_clean_troll_files/leveltroll500/2022/04/08/21115'
  subDirParts = strsplit(DirInLogs,split = "/")[[1]][12:16]
  subDir = paste0(subDirParts, collapse = '', sep='/')
  subDirPath = file.path(subDir)
  testOutputDirPath <- base::paste0(testDirOut,"/", subDirPath)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                       DirInStream=DirInStream,
                                       DirIn=DirIn,
                                       DirOutBase=testDirOut,
                                       SchmDataOut=NULL,
                                       SchmFlagsOut=NULL,
                                       timeBgnDiff_1= timeBgnDiff_1,
                                       timeBgnDiff_5= timeBgnDiff_5,
                                       log=log)
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  #
  # Test 2. Leveltroll500, empty DirInStream
  # is.null(L0Data) & !is.null(LogData))
  #
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  DirInStreamEmpty<-file.path(workingDirPath, 'pfs/leveltroll500_data_source_trino/leveltroll500/2022/04/04/21115_empty')
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStreamEmpty,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  #
  # Test 3. Leveltroll500, empty DirInLogs = NULL
  # (!is.null(L0Data) & is.null(LogData))
  #
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }

  DirInLogs_empty<-file.path(workingDirPath, 'pfs/logjam_clean_troll_files/leveltroll500/2022/04/04/21115_empty') 
  subDirParts_empty = strsplit(DirInLogs_empty,split = "/")[[1]][12:16]
  subDir_empty = paste0(subDirParts_empty, collapse = '', sep='/')
  subDirPath_empty = file.path(subDir_empty)
  testOutputDirPath_empty <- base::paste0(testDirOut,"/", subDirPath_empty)
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs_empty,
                                  DirInStream=DirInStream,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  #
  # Test 4. Leveltroll500, more params are passed in
  #
  
  SchmDataOut<-base::paste0(base::readLines('pfs/leveltroll500_avro_schemas/leveltroll500/leveltroll500_log_data.avsc'),collapse='')
  SchmFlagsOut<-base::paste0(base::readLines('pfs/leveltroll500_avro_schemas/leveltroll500/leveltroll500_log_flags.avsc'),collapse='')
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStream,
                                  DirIn=NULL,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=SchmDataOut,
                                  SchmFlagsOut=SchmFlagsOut,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  #
  # Test 5. aquatroll200, only the input of directories, resistance and voltage, and output directry are passed in
  # SchmDataOut=NULL and SchmFlagsOut=NULL
  #
  DirInLogs<-file.path(workingDirPath, 'pfs/logjam_clean_troll_files/aquatroll200/2022/03/09/23646') 
  DirInStream<-file.path(workingDirPath, 'pfs/aquatroll200_data_source_trino/aquatroll200/2022/03/09/23646')
  DirIn<-'pfs/logjam_clean_troll_files/aquatroll200/2022/03/16/23646'
  subDirParts = strsplit(DirInLogs,split = "/")[[1]][12:16]
  subDir = paste0(subDirParts, collapse = '', sep='/')
  subDirPath = file.path(subDir)
  testOutputDirPath <- base::paste0(testDirOut,"/", subDirPath)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStream,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  #
  # Test 6. aquatroll200, empty DirInStream
  # is.null(L0Data) & !is.null(LogData))
  #
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  DirInStreamEmpty<-file.path(workingDirPath, 'pfs/aquatroll200_data_source_trino/aquatroll200/2022/03/09/23646_empty')
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStreamEmpty,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  #
  # Test 7. aquatroll200, empty DirInLogs = NULL
  # (!is.null(L0Data) & is.null(LogData))
  #
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  DirInLogs_empty<-file.path(workingDirPath, 'pfs/logjam_clean_troll_files/aquatroll200/2022/03/09/23646_empty') 
  subDirParts_empty = strsplit(DirInLogs_empty,split = "/")[[1]][12:16]
  subDir_empty = paste0(subDirParts_empty, collapse = '', sep='/')
  subDirPath_empty = file.path(subDir_empty)
  testOutputDirPath_empty <- base::paste0(testDirOut,"/", subDirPath_empty)
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs_empty,
                                  DirInStream=DirInStream,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  #testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  #
  #
  # Test 8. aquatroll200,  DirIn = NULL, SchmDataOut=NULL and SchmFlagsOut=NULL
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStream,
                                  DirIn=NULL,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)

  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  #
  # Test 9. aquatroll200,  DirIn = NOT NULL, SchmDataOut=aquatroll200_log_data_noConductivity.avsc and
  # SchmFlagsOut= NOT NULL
  
  SchmDataOut<-base::paste0(base::readLines('pfs/aquatroll200_avro_schemas/aquatroll200/aquatroll200_log_data_noConductivity.avsc'),collapse='')
  SchmFlagsOut<-base::paste0(base::readLines('pfs/aquatroll200_avro_schemas/aquatroll200/aquatroll200_log_flags.avsc'),collapse='')
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStream,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=SchmFlagsOut,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  
  #
  # Test 10. aquatroll200,  DirIn = NOT NULL, SchmDataOut=NULL and SchmFlagsOut= NOT NULL
  
  SchmDataOut<-base::paste0(base::readLines('pfs/aquatroll200_avro_schemas/aquatroll200/aquatroll200_log_data.avsc'),collapse='')
  SchmFlagsOut<-base::paste0(base::readLines('pfs/aquatroll200_avro_schemas/aquatroll200/aquatroll200_log_flags.avsc'),collapse='')
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                  DirInStream=DirInStream,
                                  DirIn=DirIn,
                                  DirOutBase=testDirOut,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=SchmFlagsOut,
                                  timeBgnDiff_1= timeBgnDiff_1,
                                  timeBgnDiff_5= timeBgnDiff_5,
                                  log=log)
  
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "data")))
  testthat::expect_true(file.exists(file.path(testOutputDirPath, "flags")))
  })
