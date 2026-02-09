##############################################################################################
#' @title Unit test for Wrapper for Troll Log File Processing
#' 
#' @description Wrapper function. Validates, cleans, and formats troll log files into daily parquets.
#'
#'
#' @param FileIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/source-id/<file-name>, where file-name can be "12345678345678.csv".
#' The source-id is the unique identifier of the sensor and may have multiple csv log files. 
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
#
##############################################################################################
context("\n                       Unit test of wrap.sunav2.logfiles.R\n")

test_that("Unit test of wrap.sunav2.logfiles.R", {
  
  source('../../flow.sunav2.logfiles/wrap.sunav2.logfiles.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Test 1: process a typical file and expect daily output directories created
  workingDirPath <- getwd()
  file.path(workingDirPath, 'pfs/logjam_load_files/21115/7a5c6a2adb01c935f8dc87a3fcc25316.csv')
  testDirOut = file.path(workingDirPath, 'pfs/out_sunav2')
  
  # Demo read for info (format and index below may be modified for real SunAV2 data)
  log_file <- base::try(read.table(testFileIn, header=FALSE, sep=",", col.names=paste0("V",seq_len(8)),
                                   encoding="utf-8", stringsAsFactors=FALSE, fill=TRUE, strip.white=TRUE, na.strings=c(-1,"")), silent=TRUE)
  sensor_id <- tolower(gsub(" ", "", as.character(log_file$V2[5])))
  yr <- format(as.Date(log_file$V1[11], format="%m/%d/%Y"),"%Y")
  mo <- format(as.Date(log_file$V1[11], format="%m/%d/%Y"),"%m")
  startDate <- format(as.Date(log_file$V1[11], format="%m/%d/%Y"),"%d")
  endDate <- format(as.Date(log_file$V1[12], format="%m/%d/%Y"),"%d")
  testDirOutDir <- file.path(testDirOut, sensor_id, yr, mo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  
  wrap.sunav2.logfiles(FileIn=testFileIn, DirOut=testDirOut, SchmDataOut=NULL, log=log)
  
  for (iDate in as.numeric(startDate):as.numeric(endDate)){
    expect_true(file.exists(file.path(testDirOutDir, str_pad(iDate, 2, pad="0"))))
  }
  
  # Test 2: Not NULL Schema is passed in
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  # Example dummy data for schema (modify columns as appropriate for SunAV2!)
  source_id <- c("10001")
  log_time <- c("2020-01-02T00:00:00Z")
  voltage <- c("12.3")
  temperature <- c("22.5")
  logFlag <- c("1")
  logDateErrorFlag <- c("0")
  df1 <- data.frame(source_id, log_time, voltage, temperature, logFlag, logDateErrorFlag)
  schm <- NEONprocIS.base::def.schm.parq.from.df(df=df1, log=NULL)
  
  wrap.sunav2.logfiles(FileIn=testFileIn, DirOut=testDirOut, SchmDataOut=schm, log=log)
  
  # Test 3: input file has all dates before 2018 (should still process)
  testFileIn_past <- file.path(workingDirPath,'pfs/sunav2_logfiles_before2018/10001/abcd1234.csv')
  log_file_past  <- base::try(read.table(testFileIn_past, header=FALSE, sep=",", col.names=paste0("V",seq_len(8)),
                                         encoding="utf-8", stringsAsFactors=FALSE, fill=TRUE, strip.white=TRUE, na.strings=c(-1,"")), silent=TRUE)
  sensor_id_past <- tolower(gsub(" ", "", as.character(log_file_past$V2[5])))
  yr_past <- format(as.Date(log_file_past$V1[11], format="%m/%d/%Y"),"%Y")
  mo_past <- format(as.Date(log_file_past$V1[11], format="%m/%d/%Y"),"%m")
  startDate_past <- format(as.Date(log_file_past$V1[11], format="%m/%d/%Y"),"%d")
  endDate_past <- format(as.Date(log_file_past$V1[12], format="%m/%d/%Y"),"%d")
  testDirOutDir_past <- file.path(testDirOut, sensor_id_past, yr_past, mo_past)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  wrap.sunav2.logfiles(FileIn=testFileIn_past, DirOut=testDirOut, SchmDataOut=NULL, log=log)
  expect_true(file.exists(testDirOutDir_past))
  
  # Additional negative/robustness tests could be added, e.g. missing column, corrupt file, etc
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  
})
