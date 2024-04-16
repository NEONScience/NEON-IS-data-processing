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
  #
  # Test 1. Only the input of directories, resistance and voltage, and output directry are passed in
  
  DirInLogs<-file.path(workingDirPath, 'pfs/logjam_clean_troll_files/leveltroll500/2022/04/04/21115') 
  DirInStream<-file.path(workingDirPath, 'pfs/leveltroll500_data_source_trino/leveltroll500/2022/04/04/21115')
  DirIn<-'pfs/logjam_clean_troll_files/leveltroll500/2022/04/08/21115'
 
  tt <- wrap.troll.logfiles.fill (DirInLogs=DirInLogs,
                                       DirInStream=DirInStream,
                                       DirIn=DirIn,
                                       DirOutBase=testDirOut,
                                       SchmDataOut=NULL,
                                       SchmFlagsOut=NULL,
                                       log=log)
 
})
