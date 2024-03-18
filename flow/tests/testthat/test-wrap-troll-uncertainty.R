##############################################################################################
#' @title Unit test for Wrapper for Level Troll 500 and Aqua Troll 200 Science Computations
#' 
#' @description Wrapper function. Calculate elevation and derive uncertainty for surface and groundwater troll data products.
#'
#' @param DirInTroll Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#' 
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /flags
#' 
#' @param DirInUcrt Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#' 
#' Nested within this path are the folders:
#'         /uncertainty_coef
#'         /uncertainty_data
#' 
#' @param DirIn 
#' Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor.
#' DirIn is needed one if this script is run after filter joiner module. \cr
#' 
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' The data folder holds 1 data file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD.parquet
#' 
#' The location folder holds 2 location json files with the naming formats:
#' SOURCETYPE_SOURCEID_locations.json
#' CFGLOC.json
#' 
#' The uncertainty_coef folder holds 1 file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyData.parquet
#' 
#' The uncertainty_data folder holds 1 file with the naming format:
#' SOURCETYPE_CFGLOC_YYYY-MM-DD_uncertaintyCoef.json
#' 
#' 
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param Context String. Required. The value must be designated as either "SW" or "GW" for surface or groundwater.
#' 
#' @param WndwAgr (optional) where value is the aggregation interval for which to compute uncertainty. 
#' Formatted as a 3 character sequence, typically representing the number of minutes over which to compute uncertainty 
#' For example, "WndwAgr=001" refers to a 1-minute aggregation interval, while "WndwAgr=030" refers to a 
#' 30-minute aggregation interval. Multiple aggregation intervals may be specified by delimiting with a pipe 
#' (e.g. "WndwAgr=001|030|060"). Note that a separate file will be output for each aggregation interval. 
#' It is assumed that the length of the file is one day. The aggregation interval must divide one day into 
#' complete intervals. No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param WndwInst (optional) set to TRUE to include instantaneous uncertainty data output. The defualt value is FALSE. 
#' No uncertainty data will be output if both "WndwAgr" and "WndwInst" are NULL.
#' 
#' @param SchmDataOut (optional) A json-formatted character string containing the schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmUcrtOutAgr (optional) A json-formatted character string containing the schema for the output aggregate uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmUcrtOutInst (optional) A json-formatted character string containing the schema for the output instantaneous uncertainty data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#' 
#' @param SchmStatsOut (optional) A json-formatted character string containing the schema for the output statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#' 
#' @param SchmSciStatsOut (optional) A json-formatted character string containing the schema for the output science statistics
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the merged and filtered Kafka output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples 
#' # NOT RUN
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' DirInTroll<-"~/pfs/surfacewaterPhysical_analyze_pad_and_qaqc_plau/2022/03/03/surfacewater-physical_BARC130100/aquatroll200/CFGLOC113600",
#' DirInUcrt<-"~/pfs/surfacewaterPhysical_group_path/2022/03/03/surfacewater-physical_BARC130100/aquatroll200/CFGLOC113600",
#' SchmStatsOut<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_aquatroll200_dp01_stats.avsc'),collapse='')
#' SchmDataOut<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_data.avsc'),collapse='')
#' SchmUcrtOutAgr<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt.avsc'),collapse='')
#' SchmUcrtOutInst<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt_inst.avsc'),collapse='')
#' SchmSciStatsOut<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_troll_specific_sci_stats.avsc'),collapse='')
#' SchmStatsOut<-base::paste0(base::readLines('~/pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_aquatroll200_dp01_stats.avsc'),collapse='')
#' wrap.troll.uncertainty(DirInTroll=DirInTroll,
#'                               DirInUcrt=DirInUcrt,
#'                               DirIn=NULL,
#'                               DirOutBase="~/pfs/out",
#'                               Context='SW',
#'                               WndwInst=TRUE,
#'                               WndwAgr='030',
#'                               timeBgnDiff = timeBgnDiff,
#'                               timeEndDiff =timeEndDiff,
#'                               SchmDataOut=SchmDataOut,
#'                               SchmUcrtOutAgr=SchmUcrtOutAgr,
#'                               SchmUcrtOutInst=SchmUcrtOutInst,
#'                               SchmSciStatsOut=SchmSciStatsOut,
#'                               SchmStatsOut=SchmStatsOut,
#'                               log=log)
#'                               
#' @seealso Currently none
#' 
# changelog and author contributions 
#   Mija Choi (2024-02-13)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.uncertainty.R\n")

# Unit test of wrap.troll.uncertainty.R
test_that("Unit test of wrap.troll.uncertainty.R", {
  
  source('../../flow.troll.uncertainty/wrap.troll.uncertainty.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  #
  # Test 1. Only the input of directories, resistance and voltage, and output directry are passed in
  
  workingDirPath <- getwd()
  testDirOut = file.path(workingDirPath, 'pfs/out')
  
  DirInTroll<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_analyze_pad_and_qaqc_plau/2022/03/02/surfacewater-physical_ARIK101100/leveltroll500/CFGLOC101669')
  DirInUcrt<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_group_path/2022/03/02/surfacewater-physical_ARIK101100/leveltroll500/CFGLOC101669')
  
  testStatsDir<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_leveltroll500_dp01_stats.avsc')
  SchmStatsOut<-base::paste0(base::readLines(testStatsDir),collapse='')
  
  testDataDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_data.avsc')
  SchmDataOut<-base::paste0(base::readLines(testDataDir),collapse='')
  
  testAgrDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_ucrt.avsc')
  SchmUcrtOutAgr<-base::paste0(base::readLines(testAgrDir),collapse='')
  
  testInstDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_leveltroll500_specific_ucrt_inst.avsc')
  SchmUcrtOutInst<-base::paste0(base::readLines(testInstDir),collapse='')
  
  testSciStatsDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_troll_specific_sci_stats.avsc')
  SchmSciStatsOut<-base::paste0(base::readLines(testSciStatsDir),collapse='')
  
  # testStatsDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_leveltroll500_dp01_stats.avsc')
  # SchmStatsOut<-base::paste0(base::readLines(testStatsDir),collapse='')
  
  # get sub directory 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInTroll)
  testDirRepo <- InfoDirIn$dirRepo
  
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  WndwAgr <- base::as.difftime(base::as.numeric('030'),units="mins")
  if(length(WndwAgr)>0){
    timeBgnDiff <- list()
    timeEndDiff <- list()
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],WndwTime=base::as.difftime(1,units='days'))
      timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
      timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
    } # End loop around aggregation intervals
  }
 
  # source_id = leveltroll500
   wrap.troll.uncertainty(DirInTroll=DirInTroll,
                         DirInUcrt=DirInUcrt,
                         DirIn=NULL,
                         DirOutBase=testDirOut,
                         Context='SW',
                         WndwInst=TRUE,
                         WndwAgr='030',
                         timeBgnDiff =timeBgnDiff,
                         timeEndDiff =timeEndDiff,
                         SchmDataOut=SchmDataOut,
                         SchmUcrtOutAgr=SchmUcrtOutAgr,
                         SchmUcrtOutInst=SchmUcrtOutInst,
                         SchmSciStatsOut=SchmSciStatsOut,
                         SchmStatsOut=SchmStatsOut,
                         log=log)
  # 
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  #
  # Test 2.  source_id = aquatroll200
  
  workingDirPath <- getwd()
  testDirOut = file.path(workingDirPath, 'pfs/out')
  
  DirInTroll<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_analyze_pad_and_qaqc_plau/2022/03/02/surfacewater-physical_BARC130100/aquatroll200/CFGLOC113600')
  DirInUcrt<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_group_path/2022/03/02/surfacewater-physical_BARC130100/aquatroll200/CFGLOC113600')
  
  testStatsDir<-file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_aquatroll200_dp01_stats.avsc')
  SchmStatsOut<-base::paste0(base::readLines(testStatsDir),collapse='')
  
  testDataDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_data.avsc')
  SchmDataOut<-base::paste0(base::readLines(testDataDir),collapse='')
  
  testAgrDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt.avsc')
  SchmUcrtOutAgr<-base::paste0(base::readLines(testAgrDir),collapse='')
  
  testInstDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_aquatroll200_specific_ucrt_inst.avsc')
  SchmUcrtOutInst<-base::paste0(base::readLines(testInstDir),collapse='')
  
  testSciStatsDir= file.path(workingDirPath, 'pfs/surfacewaterPhysical_avro_schemas/surfacewaterPhysical/surfacewaterPhysical_dp01_troll_specific_sci_stats.avsc')
  SchmSciStatsOut<-base::paste0(base::readLines(testSciStatsDir),collapse='')
  
  # get sub directory 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirInTroll)
  testDirRepo <- InfoDirIn$dirRepo
  
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  wrap.troll.uncertainty(DirInTroll=DirInTroll,
                         DirInUcrt=DirInUcrt,
                         DirIn=NULL,
                         DirOutBase=testDirOut,
                         Context='SW',
                         WndwInst=TRUE,
                         WndwAgr='030',
                         timeBgnDiff =timeBgnDiff,
                         timeEndDiff =timeEndDiff,
                         SchmDataOut=SchmDataOut,
                         SchmUcrtOutAgr=SchmUcrtOutAgr,
                         SchmUcrtOutInst=SchmUcrtOutInst,
                         SchmSciStatsOut=SchmSciStatsOut,
                         SchmStatsOut=SchmStatsOut,
                         log=log)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  
  # Test 3
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  try(wrap.troll.uncertainty(DirInTroll=NULL,
                         DirInUcrt=NULL,
                         DirIn=DirInTroll,
                         DirOutBase=testDirOut,
                         Context='SW',
                         WndwInst=TRUE,
                         WndwAgr='030',
                         timeBgnDiff =timeBgnDiff,
                         timeEndDiff =timeEndDiff,
                         SchmDataOut=SchmDataOut,
                         SchmUcrtOutAgr=SchmUcrtOutAgr,
                         SchmUcrtOutInst=SchmUcrtOutInst,
                         SchmSciStatsOut=SchmSciStatsOut,
                         SchmStatsOut=SchmStatsOut,
                         log=log), silent = TRUE)
  
 # expect_true (file.exists(testDirOutPath, recursive = TRUE))
  

})