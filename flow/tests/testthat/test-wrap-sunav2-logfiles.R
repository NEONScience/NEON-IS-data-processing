##############################################################################################
#' @title Unit test for wrap.sunav2.logfiles module for NEON IS data processing
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#'
#' @description  Unit test for wrap.sunav2.logfiles
#'
#' Tests include normal operation, expected output file/directory creation, handling of bad input, and missing files/fields.
#'
#'#' @param FileIn Character value. The input path to the data from a single source ID, structured as:
#' #/pfs/BASE_REPO/source-id/<file-name>. The source-id is the unique identifier of the sensor. 
#' 
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of FileIn.
#' 
#' @param SchmDataOut (optional), the full path to the avro schema for the output data file.
#' If not provided, output schema will match the input file columns.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return Cleaned SunaV2 log files in daily parquets.
#' 
#' #' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.logfiles <- function(FileIn="~/pfs/sunav2_logjam_load_files/20349/logjam_prod_20349.csv",
#'                               DirOut="~/pfs/out",
#'                               SchmDataOut=NULL,
#'                               log=log)
#' 
#'# changelog and author contributions 
#   Nora Catolico (2026-02-06)
#     Original Creation
#
##############################################################################################
context("\n                       Unit test of wrap.sunav2.logfiles.R\n")

test_that("Unit test of wrap.sunav2.logfiles.R", {
  
  source('../../flow.sunav2.logfiles/wrap.sunav2.logfiles.R')
  library(stringr)
  library(lubridate)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  #setwd('~/R/NEON-IS-data-processing/flow/tests/testthat')
  
  # Test 1: process a typical file and expect daily output directories created
  workingDirPath <- getwd()
  testFileIn = file.path(workingDirPath, 'pfs/sunav2_logjam_load_files/20349/logjam_prod_20349b.csv')
  Asset<-"20349"
  fileName<-basename(testFileIn)
  testDirOut = 'pfs/out'
  
  
  # Read in file
  log_file  <-
    base::try(read.table(paste0(testFileIn), header = FALSE, sep = ",", 
                         col.names = paste0("V",seq_len(286)),encoding = 'utf-8',
                         stringsAsFactors = FALSE,fill = TRUE,strip.white = TRUE,na.strings=c(-1,'')))
  start<-which(grepl('Zeiss Coefficient',log_file$V2))+1
  # Separate data and metadata
  logData<-log_file[start:(nrow(log_file)),]
  #' Calculates the readout date and time in POSIXct format 
  logData$readout_time<-lubridate::parse_date_time(as.character(logData[,2]),order="yj") 
  startDate <- min(logData$readout_time)
  endDate <- max(logData$readout_time)
  date_obj <- as.POSIXct(startDate, format = "%Y-%m-%d", tz = "UTC")
  y<-format(date_obj, "%Y")  # "2024"
  m<-format(date_obj, "%m")  # "09"
  d<-format(date_obj, "%d")  # "02"
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  
  testDirOutDir<-paste(testDirOut,"sunav2",y,m,d,Asset,sep="/")
  
  # Test 1: runs without error
  wrap.sunav2.logfiles(FileIn=testFileIn, DirOut=testDirOut, SchmDataOut=NULL, log=log)
  testthat::expect_true(file.exists(file.path(testDirOutDir, "data")))

  
  # Test 2: Not NULL Schema is passed in
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  schm<-file.path(workingDirPath, 'pfs/sunav2_avro_schemas/sunav2_logfilled.avsc')
  SchmDataOut <- base::paste0(base::readLines(schm),collapse='')
  wrap.sunav2.logfiles(FileIn=testFileIn, DirOut=testDirOut, SchmDataOut=SchmDataOut, log=log)
  testthat::expect_true(file.exists(file.path(testDirOutDir, "data")))

  # Additional negative/robustness tests could be added, e.g. missing column, corrupt file, etc
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive=TRUE)
  }
  
})
