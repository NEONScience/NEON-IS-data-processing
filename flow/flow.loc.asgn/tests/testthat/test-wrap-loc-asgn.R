##############################################################################################
#' @title Unit test for wrap.loc.asgn.R, which assign and filter the location file(s) for a sensor ID to each data day for which each applies

#' @author
#' Mija Choi\email{choim@battelleecology.org} \cr

#' @description Assign the location file(s) for an asset or named location to each data day which it
#' applies over 1 or more data years. When assigning the location file to each data day, the location
#' information is filtered to exclude information not relevant to the data day. This includes truncating
#' any applicable dates in the locations file to the start or end of the data day.
#' Original dates falling within the data day will not be modified. This code works for
#' both asset location files as well as named-location location files.
#'
#' @param DirIn Character value. The input path to the location files from a single asset or location ID,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/SOURCE_TYPE/ID \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#'
#' There may be no further subdirectories of ID.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/prt/27134 \cr
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#' @param TimeBgn POSIX. The minimum date for which to assign calibration files.
#' @param TimeEnd POSIX. The maximum date for which to assign calibration files.
#' @param TypeFile String value. The type of location file that is being distributed and filtered.
#' Options are 'asset' and 'namedLocation'. Only one may be specified. 'asset' corresponds to a
#' location file for a particular asset, which includes information about where and for how long
#' the asset was installed, including its geolocation history. 'namedLocation' corresponds to a
#' location file specific to a named location, including the properties of that named location and
#' the dates over which it was active (should have been producing data).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#'
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/ID,
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the
#' terminal path (ID) is populated with the filtered location files applicable to the year, month, and
#' day indicated in the path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.asgn(DirIn="pfs/location_asset/ptb330a/10312",
#'              DirOutBase="pfs/out",
#'              TimeBgn=as.POSIXct('2019-01-01',tz='GMT),
#'              TimeEnd=as.POSIXct('2019-06-01',tz='GMT),
#'              TypeFile='asset'
#'              )

#' @seealso None
#'
# changelog and author contributions / copyrights
#   Mija Choi (2021-04-19)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.loc.asgn.R\n")

# Unit test of wrap.loc.asgn.R
test_that("Unit test of wrap.loc.asgn.R", {
  source('../../wrap.loc.asgn.R')
  library(stringr)
  
  wk_dir <- getwd()
  testOutputDir = "pfs/out"
  
  # Test scenario 1:: within the valid time range
  # 10312 does not have "active_periods" pass TypeFile = 'asset'
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/location_asset/ptb330a/10312')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir,'/',fileLoc))
  
  installdate <- str_replace_all(loc$install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"/",installdate,"/",loc$source_id,collapse='/','/location')
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'asset'
  )
  
  testthat::expect_true (dir.exists(testOutputDirPath))
  #
  # Test scenario 2:: within the valid time range
  # 100959 has "active_periods" pass TypeFile = 'namedLocation'
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/location_asset/ptb330a/100959')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir, '/', fileLoc))
  
  installdate <- str_replace_all(as.Date(loc$install_date), "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"/",installdate,"/",loc$source_id,collapse = '/')
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'namedLocation'
  )
  testthat::expect_true (dir.exists(testOutputDirPath))
  #
  # Test scenario 3:: within the valid time range
  #
  # 10754 does not have "active_periods" pass TypeFile = 'asset'
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/locations/prt/24688')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir, '/', fileLoc))
  
  installdate <- str_replace_all(loc$install_date, "-", "/")
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"/",installdate,"/",loc$source_id,collapse = '/')
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'asset'
  )
  testthat::expect_true (dir.exists(testOutputDirPath))
  #
  # Test scenario 4:: pass invalid TypeFile
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- try(wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'InvalidValue'
  ),silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  #
  # Test scenario 5:: not within the valid time range
  #
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- try(wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2018-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2018-01-10', tz = 'GMT'),
    TypeFile = 'asset'
  ),silent = TRUE)
  
  testthat::expect_true (!(dir.exists(testOutputDir)))
  
  # Test scenario 6: more than 1 file
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/locations_2files/hmp155/10267')
  returnedOutputDir <- try(wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'namedLocation'
  ),silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
  # Test scenario 7:: no files in the input dir
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/locations_nofiles')
  returnedOutputDir <- wrap.loc.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'asset'
  )
  
  testthat::expect_true (!dir.exists(testOutputDir))
  
})
