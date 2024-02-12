##############################################################################################
#' @title Unit test for wrapper of Missing Temp Flag and Conductivity Conversion
#' 
#' @description Wrapper function. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
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
#' THE INPUT DATA.
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
#' @return Corrected conductivity data and associated flags for missing temperature data.
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
#' wrap.troll.cond.conv <- function(DirIn="~/pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681",
#'                               DirOutBase="~/pfs/out",
#'                               SchmDataOut="~/R/NEON-IS-avro-schemas/dp0p/aquatroll200_cond_corrected.avsc",
#'                               SchmQf="~/R/NEON-IS-avro-schemas/dp0p/flags_troll_specific_temp.avsc",
#'                               DirSubCopy=NULL,
#'                               log=log)
#'                               
#' 
#' @seealso None currently
#' 
#' 
# changelog and author contributions 
#   Mija Choi (2024-01-09)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.troll.cond.conv.R\n")

# Unit test of wrap.troll.cond.conv.R
test_that("Unit test of wrap.troll.cond.conv.R", {
  
  source('../../flow.troll.cond.conv/wrap.troll.cond.conv.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  #
  # Test 1. Only the input of directories, resistance and voltage, and output directry are passed in
 
  workingDirPath <- getwd()
  testDirIn = file.path(workingDirPath, 'pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/02/1285')
  testDirOut = file.path(workingDirPath, 'pfs/out')
  testSchmDataOut = file.path(testDirOut, 'dp0p/aquatroll200_cond_corrected.avsc')
  testSchmQfDir=file.path(workingDirPath, 'pfs/dp0p/flags_troll_specific_temp.avsc')
  testSchmQf <- base::paste0(base::readLines(testSchmQfDir),collapse='')
  # get sub directory 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(testDirIn)
  testDirRepo <- InfoDirIn$dirRepo
  
  testDirOutPath <- base::paste0(testDirOut, testDirRepo)
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.cond.conv (DirIn=testDirIn,
                        DirOutBase=testDirOut)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  #
  # Test 2. 

  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.cond.conv (DirIn=testDirIn,
                        DirOutBase=testDirOut,
                        SchmDataOut=NULL,
                        SchmQf=testSchmQf,
                        DirSubCopy=NULL,
                        log=log)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
 
  # Test 3. DirSubCopy is not NULL
  
  if (dir.exists(testDirOut)) {
    unlink(testDirOut, recursive = TRUE)
  }
  
  wrap.troll.cond.conv (DirIn=testDirIn,
                        DirOutBase=testDirOut,
                        SchmDataOut=NULL,
                        SchmQf=testSchmQf,
                        DirSubCopy='aaa',
                        log=log)
  
  expect_true (file.exists(testDirOutPath, recursive = TRUE))
  
   # 
  # testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
})
