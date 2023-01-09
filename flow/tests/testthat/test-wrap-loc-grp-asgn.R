##############################################################################################
#' @title Unit test for wrap.loc.grp.asgn.R, which assign and filter the location file(s) for a sensor ID to each data day for which each applies

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
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/ID/location/,
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the
#' terminal path (ID) is populated with the filtered location files applicable to the year, month, and
#' day indicated in the path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.grp.asgn(DirIn="pfs/location_asset/ptb330a/10312",
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
context("\n                       Unit test of wrap.loc.grp.asgn.R\n")

# Unit test of wrap.loc.grp.asgn.R
test_that("Unit test of wrap.loc.grp.asgn.R", {
  source('../../flow.loc.grp.asgn/wrap.loc.grp.asgn.R')
  library(stringr)
  
  wk_dir <- getwd()
  testOutputDir = "pfs/out"
  
  
  
  ### Test group A - location assignment ###
  
  # Test scenario A1:: within the valid time range
  # 10312 does not have "active_periods" pass TypeFile = 'asset'
  #
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/location_asset/ptb330a/10312')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir,'/',fileLoc))

  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT')
  returnedOutputDir <- wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn =   TimeBgn,
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'asset'
  )
  
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"/",format(TimeBgn,format="%Y"),"/",format(TimeBgn,format="%m"),"/")
  fileLocPath <- base::paste0("location/", fileLoc)
  testthat::expect_true (any(file.exists(testOutputDir,fileLocPath, recursive = TRUE)))
  #
  # Test scenario A2:: within the valid time range
  # 100959 has "active_periods" pass TypeFile = 'namedLocation'
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/location_asset/ptb330a/100959')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir, '/', fileLoc))

  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = TimeBgn,
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'namedLocation'
  )
  testOutputDirPath <- base::paste0(testOutputDir,"/",loc$source_type,"/",format(TimeBgn,format="%Y"),"/",format(TimeBgn,format="%m"),"/",collapse='/')
  fileLocPath <- base::paste0("location/", fileLoc)
  testthat::expect_true (any(file.exists(testOutputDir,fileLocPath, recursive = TRUE)))
  #
  # Test scenario A3:: within the valid time range
  # pass TypeFile = 'namedLocation', but the json does not have "active_periods" 
  # Errs out
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/location_asset/ptb330a/10312')
  fileLoc <- base::dir(testInputDir)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(testInputDir, '/', fileLoc))

  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- try (wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = TimeBgn,
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'namedLocation'
  ), silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
  #
  # Test scenario A4:: pass invalid TypeFile
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- try(wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'InvalidValue'
  ),silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  #
  # Test scenario A5:: not within the valid time range
  #
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- try(wrap.loc.grp.asgn(
    DirIn = 'pfs/location_asset/ptb330a/10312',
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2018-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2018-01-10', tz = 'GMT'),
    TypeFile = 'asset'
  ),silent = TRUE)
  
  testthat::expect_true (!(dir.exists(testOutputDir)))
 
  # Test scenario A6: more than 1 file
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/locations_2files/hmp155/10267')
  returnedOutputDir <- try(wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'namedLocation'
  ),silent = TRUE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
  # Test scenario A7:: no files in the input dir
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/locations_nofiles/prt/10312/')
  returnedOutputDir <- wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    TypeFile = 'asset'
  )
  
  testthat::expect_true (!dir.exists(testOutputDir))

  
  ### Test group B - group assignment ###
  
  # Test scenario B1:: within the valid time range
  # pass TypeFile = 'group'
  #
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/group_loader/test-group/CFGLOC100016')
  fileGrp <- base::dir(testInputDir)
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  TimeBgn = as.POSIXct('2022-10-27', tz = 'GMT')
  TimeEnd = as.POSIXct('2022-11-05', tz = 'GMT')
  returnedOutputDir <- wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = TimeBgn,
    TimeEnd = TimeEnd,
    TypeFile = 'group'
  )
  
  testthat::expect_true (all(file.exists(paste0(testOutputDir,'/test-group/2022/',c('10/27','10/28','10/29','10/30','10/31','11/01','11/02','11/03','11/04'),'/CFGLOC100016'), recursive = TRUE)))
  testthat::expect_false (any(file.exists(paste0(testOutputDir,'/test-group/2022/',c('10/26','11/05')), recursive = TRUE)))
  
  
  # Test scenario B2:: partial valid time range
  # pass TypeFile = 'group'
  #
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/group_loader/test-group/rel-humidity_CPER000040')
  fileGrp <- base::dir(testInputDir)
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  TimeBgn = as.POSIXct('2022-10-27', tz = 'GMT')
  TimeEnd = as.POSIXct('2022-11-05', tz = 'GMT')
  returnedOutputDir <- wrap.loc.grp.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = TimeBgn,
    TimeEnd = TimeEnd,
    TypeFile = 'group'
  )
  
  testthat::expect_true (all(file.exists(paste0(testOutputDir,'/test-group/2022/',c('10/29','10/30'),'/rel-humidity_CPER000040'), recursive = TRUE)))
  testthat::expect_false (any(file.exists(paste0(testOutputDir,'/test-group/2022/',c('10/28','10/31')), recursive = TRUE)))
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
})
