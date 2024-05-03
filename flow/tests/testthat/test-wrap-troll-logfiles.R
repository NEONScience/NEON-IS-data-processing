##############################################################################################
#' @title Unit test for Wrapper for Troll Log File Processing
#' 
#' @description Wrapper function. Validates, cleans, and formats troll log files into daily parquets.
#'
#'
#' @param FileIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id/<file-name>, where file-name can be "12345678345678.csv". 
#' The source-id folder may have multiple csv log files. 
#' The source-id is the unique identifier of the sensor. \cr#'
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of FileIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
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
#' FileIn<-'~/pfs/logjam_load_files/21115/7a5c6a2adb01c935f8dc87a3fcc25316.csv'
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.logfiles <- function(FileIn="~/pfs/logjam_load_files/21115/7a5c6a2adb01c935f8dc87a3fcc25316.csv",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#'                               
# changelog and author contributions 
#   Mija Choi (2024-02-20)
#     Original Creation
#   Mija Choi (2024-05-03)
#     Updated after a change in one of the input parameters of wrap.troll.logfiles.R, 
#     DirIn to FileIn.
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.logfiles.R\n")

# Unit test of wrap.troll.logfiles.R
test_that("Unit test of wrap.troll.logfiles.R", {
  
  source('../../flow.troll.logfiles/wrap.troll.logfiles.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  #
  # Test 1. Only the input of directories, resistance and voltage, and output directry are passed in
  
  workingDirPath <- getwd()
  testFileIn = file.path(workingDirPath, 'pfs/logjam_load_files/21115/7a5c6a2adb01c935f8dc87a3fcc25316.csv')
  testDirOut = file.path(workingDirPath, 'pfs/out')
  #
  fileData <- base::list.files(testFileIn,full.names=TRUE)
  log_file  <- base::try(read.table(paste0(testFileIn), header = FALSE, sep = ",", 
                         col.names = paste0("V",seq_len(6)),encoding = 'utf-8',
                         stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
  #log_file$V1[52]  "11/5/2019 19:50"
  #log_file$V1[53]  "11/19/2019 19:11"
  #log_file$V2[13] "Level TROLL 500"

  sensor = tolower(gsub(" ", "", paste(log_file$V2[13])))
  #datePattern = "\d{1,2}\/\d{1,2}\/\d{2,4}"
  #format(as.Date(df1$Date, format="%d/%m/%Y"),"%Y")
  yr = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%Y")
  mo = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%m")
  startDate = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%d")
  endDate = format(as.Date(log_file$V1[53], format="%m/%d/%Y"),"%d")
  testDirOutDir = file.path(testDirOut, sensor, yr, mo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.logfiles (FileIn=testFileIn,
                    DirOut=testDirOut,
                    SchmDataOut=NULL,
                    log=log)
  
  for (iDate in startDate:endDate){
    #need to keep leading 0 in the directory
    expect_true (file.exists(file.path(testDirOutDir, str_pad(iDate, 2, pad = "0"))))
    }
  #
  # Test 2. Not NULL Schema is passed in
  # 
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  # 
  #generate schema of "source_id", "readout_time", "pressure", "temperature", "logFlag" and "logDateErrorFlag")
  #from data frame
  source_id <- c("21115")
  readout_time <- c("2019-01-03T00:00:00Z")
  pressure <- c("0.000000000")
  temperature <- c("191428.000000000")
  logFlag <- c("1.0")
  logDateErrorFlag <- c("0.1")
  
  df1 <- data.frame(source_id, readout_time, pressure, temperature, logFlag,  logDateErrorFlag)
  schm = NEONprocIS.base::def.schm.parq.from.df (df = df1, log=NULL)
  
  wrap.troll.logfiles (FileIn=testFileIn,
                      DirOut=testDirOut,
                      SchmDataOut=schm,
                      log=log)
  
  # Test 3. The input file has date < 2018
  
  testFileIn = file.path(workingDirPath, 'pfs/logjam_load_files_before2018/21115/7a5c6a2adb01c935f8dc87a3fcc25316.csv')
  fileData <- base::list.files(testFileIn,full.names=TRUE)
  
  log_file  <- base::try(read.table(paste0(testFileIn), header = FALSE, sep = ",", 
                                    col.names = paste0("V",seq_len(6)),encoding = 'utf-8',
                                    stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
  sensor = tolower(gsub(" ", "", paste(log_file$V2[13])))
  yr = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%Y")
  mo = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%m")
  startDate = format(as.Date(log_file$V1[52], format="%m/%d/%Y"),"%d")
  endDate = format(as.Date(log_file$V1[53], format="%m/%d/%Y"),"%d")
  testDirOutDir = file.path(testDirOut, sensor, yr, mo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  wrap.troll.logfiles (FileIn=testFileIn,
                       DirOut=testDirOut,
                       SchmDataOut=NULL,
                       log=log)
  
  expect_true (file.exists(file.path(testDirOutDir)))

})
