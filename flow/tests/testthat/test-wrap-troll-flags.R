##############################################################################################
#' @title Unit test for Wrapper for Below Zero Pressure Flag
#' 
#' @description Wrapper function. Flags all sensor streams for the Level Troll 500 and Aqua Troll 200 when pressure is below zero.
#' when pressure is below zero.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr#'
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' For example:
#' Input path = pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681 with nested folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. Note that you will need to distinguish between the aquatroll200 (outputs conductivity) and the 
#' leveltroll500 (does not output conductivity) in your schema.
#'
#' @param SchmQf (optional) A json-formatted character string containing the schema for the flags output
#' by this function. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Corrected conductivity data and associated flags for Below Zero Pressure data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' SchmQfOut <- base::paste0(base::readLines('~/pfs/troll_shared_avro_schemas/troll_shared/flags_troll_specific.avsc'),collapse='')
#' wrap.troll.flags <- function(DirIn="~/pfs/aquatroll200_data_source_trino/aquatroll200/2020/01/02/10721",
#'                               DirOutBase="~/pfs/out",
#'                               SchmQf=SchmQfOut,
#'                               DirSubCopy=NULL,
#'                               log=log)
#'                               
# changelog and author contributions 
#   Mija Choi (2024-02-07)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.flags.R\n")

# Unit test of wrap.troll.flags.R
test_that("Unit test of wrap.troll.flags.R", {
  
  source('../../flow.troll.flags/wrap.troll.flags.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  #
  # Test 1. Only the input of directories, resistance and voltage, and output directry are passed in
  
  workingDirPath <- getwd()
  testDirIn = file.path(workingDirPath, 'pfs/aquatroll200_data_source_trino/aquatroll200/2020/01/02/1285')
  testDirOut = file.path(workingDirPath, 'pfs/out')
  testSchmQfDir= file.path(workingDirPath, 'pfs/troll_shared_avro_schemas/troll_shared/flags_troll_specific.avsc')
  testSchmQf <- base::paste0(base::readLines(testSchmQfDir),collapse='')
  # get sub directory 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- InfoDirIn$dirRepo
  
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.flags (DirIn=testDirIn,
                        DirOutBase=testDirOut)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  #
  # Test 2. Not NULL Schema is passed in
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.flags (DirIn=testDirIn,
                        DirOutBase=testDirOut,
                        SchmQf=testSchmQf,
                        DirSubCopy=NULL,
                        log=log)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  
  # Test 3. DirSubCopy is not NULL
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.flags (DirIn=testDirIn,
                        DirOutBase=testDirOut,
                        SchmQf=testSchmQf,
                        DirSubCopy='aaa',
                        log=log)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  
  #Test 4 repo is not aquatroll200
  
  testDirInWrong = file.path(workingDirPath, 'pfs/bquatroll200_data_source_trino/bquatroll200/2020/01/02/1285')
  InfoDirInWrong <- NEONprocIS.base::def.dir.splt.pach.time(testDirInWrong)
  testDirRepoWrong <- InfoDirInWrong$dirRepo
  testDirOutPathWrong <- base::paste0(testDirOut, testDirRepoWrong)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
 wrap.troll.flags (DirIn=testDirInWrong,
                    DirOutBase=testDirOut,
                    SchmQf=testSchmQf,
                    DirSubCopy=NULL,
                    log=log)
  
 expect_true (file.exists(testDirOutPathWrong, recursive = TRUE))
  
})
