##############################################################################################
#' @title Unit test for a flow script, wrap.loc.data.trnc.comb.R, truncate and combine/merge sensor-based by location.

#' @author
#' Mija Choi \email{choim@battelleecology.org} \cr

#' @description
#' Run unit tests for wrap.loc.data.trnc.comb.R.
#' The tests include positive and negative scenarios.
#' The positive test is for a case when all the params to the function are valid
#' The negative tests are when a param(s) is empty or does not have invalid values

#' Refer to Wrapper function, wrap.loc.data.trnc.comb.R.
#' Before running this function, data should already be grouped by location, meaning that data
#' and metadata from multiple sensors installed at a location during the relevant
#' time period (e.g. before/after a sensor swap) are located in the same directory for each data
#' type. This grouping is done in flow.loc.repo.strc. This function will read the location files,
#' truncate and/or merge the contents of multiple data and metadata files based on
#' the time period that each sensor was installed at the location.
#'
#' @param DirIn Character value. The input path to the data from a single location ID, structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id,
#' where # indicates any number of parent and child directories of any name, so long as they are not
#' 'pfs', 'location',or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year,
#' 2-digit month, and 2-digit day of the data contained in the folder. The data day is identified from
#' the input path. The location-id is the unique identifier of the location. \cr
#'
#' Nested within the input path path is (at a minimum) the folder:
#'         location/
#' The location folder holds json files with the location data corresponding to the data files in the data
#' directory. The source-ids for which data are expected are gathered from the location files.
#'
#' Other folders at the same level as location can hold data or metadata (mergeable data types are listed below).
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#' @param DirSubCombData (optional) Character vector. The names of subfolders holding timeseries files (e.g. data, flags)
#' to be truncated and/or merged. These directories must be at the same level as the location directory. Within
#' each folder are timeseries files from one or more source-ids. The source-id is the identifier for the sensor
#' that collected the data in the file, and must be included somewhere in the file name (and must match one of the
#' source-ids gathered from the location files). The data folders listed here may contain files holding different
#' types of data (e.g. one file holds calibration flags, another holds sensor diagnostic flags). However, the file
#' names for each type must be identical with the exception of the source ID. An attempt will be made to merge/truncate
#' the files that have the same naming convention. For example, prt_12345_sensorFlags.parquet will be merged with
#' prt_678_sensorFlags.parquet, and prt_12345_calFlags.parquet will be merged with prt_678_calFlags.parquet, since
#' the file names in each of these two groups are identical with the exception of the source ID.
#' @param DirSubCombUcrt (optional) Character vector. The names of subfolders holding uncertainty coefficient
#' json files to be merged. These directories must be at the same level as the location directory. Within each
#' subfolder are uncertainty json files, one for each source-id. The source-id is the identifier for the sensor
#' pertaining to the uncertainty info in the file, and must be somewhere in the file name.
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders, separated by pipes, at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @return A repository structure in DirOut, where DirOut replaces the input directory
#' structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure
#' of the input path. A single merged file will replace the originals in each of the subdirectories
#' specified in the input arguments. The merged files will be written with the same schema as returned from
#' reading the input file(s). The characters representing the source-id in the merged filenames will be replaced
#' by the location ID. All other subdirectories indicated in DirSubCopy will be carried through unmodified.
#'
#
# changelog and author contributions / copyrights
#   Mija Choi (2021-05-25)
#     Original Creation
##############################################################################################
# Define test context

context("\n                       Unit test of wrap.loc.data.trnc.comb.R\n")

# Unit test of wrap.loc.data.trnc.comb.R
test_that("Unit test of wrap.loc.data.trnc.comb.R", {
  source('../../flow.loc.data.trnc.comb/wrap.loc.data.trnc.comb.R')
  library(stringr)
  
  testInputDir <- 'pfs/proc_group/prt/2019/01/01/CFGLOC101670'
  dirInLoc <- base::paste0(testInputDir, '/', base::dir(testInputDir))
  fileLoc <- base::dir(dirInLoc)
  testOutputDir = "pfs/out"
  
  # Load the location json and get the location name to verify the test
  # this conforms to locations-sensor-schema.json
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc[3], '/', fileLoc[4]))
  
  installdate <- str_replace_all(loc$install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir,"/", loc$source_type, "/", installdate, collapse = '/')
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  #
  # Test scenario 1: Minimun parameters are passed in.
  # There will be no output dir
  #
  wrap.loc.data.trnc.comb(DirIn = testInputDir, 
                          DirOutBase = testOutputDir)
  
  testthat::expect_true (!(dir.exists(testOutputDir)))
  
  # Test scenario 2:: only DirSubCopy passed in, DirSubCombData, DirSubCombUcrt and DirSubCopy are not passed in.
  # The output dir will have location folder only copied
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  wrap.loc.data.trnc.comb(DirIn = testInputDir,
                          DirOutBase = testOutputDir,
                          DirSubCopy = 'location')
  
  dirLoc <- c('location')
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", loc$name, "/", dirLoc)
  testthat::expect_true (file.exists(testOutputDirnamedLoc))
  
  # Test scenario 3:: only DirSubCombData passed in
  # The output dir will have data and flags folder only
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  wrap.loc.data.trnc.comb(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    DirSubCombData = c('data', 'flags')
  )
  
  dirLoc <- c('data', 'flags')
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", loc$name, "/", dirLoc)
  testthat::expect_true (all(file.exists(testOutputDirnamedLoc)))
  
  # Test scenario 4::  only DirSubCombUcrt, DirSubCopy and DirSubCombData passed in, not DirSubCopy
  # The output dir will ahve data, flags and uncertainty_coef copied
  #
  # Remove the test output dirs and file recursively
  
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  wrap.loc.data.trnc.comb(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    DirSubCombData = c('data', 'flags'),
    DirSubCombUcrt = 'uncertainty_coef'
  )
  
  dirLoc <- c('data', 'flags', 'uncertainty_coef')
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", loc$name, "/", dirLoc)
  testthat::expect_true (all(file.exists(testOutputDirnamedLoc)))
  
  # Test scenario 5:: All the params are passed in
  # The output dir will have data, flags, location and uncertainty_coef copied
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  wrap.loc.data.trnc.comb(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    DirSubCombData = c('data', 'flags'),
    DirSubCombUcrt = 'uncertainty_coef',
    DirSubCopy = 'location'
  )
  
  dirLoc <- c('data', 'flags', 'location', 'uncertainty_coef')
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath,"/",loc$name,"/",dirLoc)
  testthat::expect_true (all(file.exists(testOutputDirnamedLoc)))
  
  # Test scenario 6:: source_id is in the sub dir instead of location name, 
  # No matching location information for location',nameLoc,' and source id
  # so numLoc == 0
  # this conforms to locations-namedLocations-schema.json
  #
  # Remove the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  testInputDir <- 'pfs/proc_group/prt_14491/2019/01/01/14491'
  dirInLoc <- base::paste0(testInputDir, '/', base::dir(testInputDir))
  fileLoc <- base::dir(dirInLoc)
  
  # Load the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc[3],'/',fileLoc[4]))
  
  installdate <- str_replace_all(loc$install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"_",loc$source_id,"/",installdate,collapse = '/')
  wrap.loc.data.trnc.comb(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    DirSubCombData = c('data', 'flags'),
    DirSubCombUcrt = 'uncertainty_coef',
    DirSubCopy = 'location'
  )
  
  dirLoc <- c('data', 'flags', 'location', 'uncertainty_coef')
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath,"/",loc$source_id,"/",dirLoc)
  testthat::expect_true (all(file.exists(testOutputDirnamedLoc)))
  
})
