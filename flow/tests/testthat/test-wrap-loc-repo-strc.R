##############################################################################################
#' @title Unit test for a flow script, wrap.loc.repo.strc.R, restructure repository from a sensor focus to a location focus. 

#' @author 
#' Mija Choi \email{choim@battelleecology.org} \cr

#' @description
#' Run unit tests for wrap.loc.repo.strc.R. 
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to wrap.loc.repo.strc.R for the details.
#'      
#' @param DirIn Character value. The input path to the data from a single sensor ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, for example, for a source-id of 27134 and additional (optional) data directory:
#' Input path = /scratch/pfs/proc_group/prt/2019/01/01/27134/ with nested folders:\cr
#'    location/\cr
#'    data/\cr
#'    
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param Comb Boolean. If TRUE, the location name will replace the source id in the repo structure. If FALSE, 
#' the location name will be inserted in the directory path above the level of source-id. Defaults to FALSE. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A restructured repository in DirOutBase, for example, 
#' if Comb is FALSE, DirOutBase is /pfs/out, and following the example in the DirIn argument, the output repo would contain: 
#' /pfs/out/prt/2019/1/01/LOCATION_ID/27134/
#'                                         location/
#'                                         data/
#' where LOCATION_ID is a location ID found in the location file. 
#' If Comb=TRUE, the output repo would contain:
#' /pfs/out/prt/2019/1/01/LOCATION_ID/
#'                                   location/
#'                                   data/
#'
# 
# changelog and author contributions / copyrights
#   Mija Choi (2021-01-11)
#     Original Creation
#   Mija Choi (2021-05-04)
#     added comments
#   Mija Choi (2021-05-07)
#     added test for data/
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.loc.repo.strc.R\n")

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.loc.repo.strc.R", {
  source('../../flow.loc.repo.strc/wrap.loc.repo.strc.R')
  library(stringr)
  
  wk_dir <- getwd()

  testInputDir <- base::paste0(wk_dir, '/', 'pfs/proc_group/prt/2019/01/01/3119')
  
  dirInLoc <- base::paste0(testInputDir, '/location')
  dirInData <- base::paste0(testInputDir, '/data')
  fileLoc <- base::dir(dirInLoc)
  fileData <- base::dir(dirInData)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc))
 
  testOutputDir = "pfs/out"
  installdate <- str_replace_all(loc$install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir, "/",loc$source_type,"/",installdate,collapse = '/')
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 1::
  # if Comb = FALSE, default when Comb is not passed in to wrap.loc.repo.strc.
  # for example, pfs/proc_group/prt/2019/01/01/3119/ copied to 
  #              pfs/out/prt/2019/01/01/CFGLOC100241/3119/location/
  #                                                      / data/
  #
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", loc$name, "/", loc$source_id, "/location")
  testOutputDirData <- base::paste0(testOutputDirPath, "/",loc$name, "/", loc$source_id, "/data")
  expect_true ((file.exists(testOutputDirnamedLoc,fileLoc, recursive = TRUE)) && 
               (file.exists(testOutputDirData,fileData, recursive = TRUE)))
 
  # Test scenario 2::
  # If Comb=TRUE, the output repo would contain:
  # /pfs/out/prt/2019/1/01/LOCATION_ID/
  #                                   location/
  #                                   data/
  # for example, pfs/out/2019/01/01/CFGLOC100241/location/
  #               
  # Remove the test output dirs and file recursively
  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  wrap.loc.repo.strc(DirIn = testInputDir,DirOutBase = testOutputDir,Comb = TRUE)
  
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", loc$name, "/", "/location")
  testOutputDirData <- base::paste0(testOutputDirPath, "/", loc$name, "/", "/data")
  expect_true ((file.exists(testOutputDirnamedLoc,fileLoc, recursive = TRUE)) && 
                 (file.exists(testOutputDirData,fileData, recursive = TRUE)))
  # Test scenario 3::
  # If there is no location file, skip

  testInputDir <- base::paste0(wk_dir, '/', 'pfs/proc_group/prt_noFiles/2019/01/01/3119')
  # Remove the test output dirs and file recursively
  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  expect_true (!dir.exists(testOutputDir))
  
  # Test scenario 4::
  # If there are more than one location file AND Comb is not passed in to wrap.loc.repo.strc 
  # use the first and copy all in the output directory
  # then pfs/proc_group/source_type/2019/01/01/source_id/ copied to 
  #      pfs/out/source_type/2019/01/01/CFGLOC100241/3119/location/ & 
  #                                                       data/
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/proc_group/prt_moreThanOneFile/2019/01/01/16247')
  dirInLoc <- base::paste0(testInputDir, '/location')
  fileLoc <- base::dir(dirInLoc)
  dirInData <- base::paste0(testInputDir, '/data')
  fileData <- base::dir(dirInData)

  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc[1]))

  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)

  expect_true (file.exists(testOutputDir,fileLoc, recursive = TRUE) && 
                 file.exists(testOutputDir,fileData, recursive = TRUE))
  
})
