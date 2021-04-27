
##############################################################################################
#' @title Unit test of def.wrap.cal.asgn.R, assign the calibration file(s) for a sensor ID to each data day for which each should be used

#' @author 
#' Mija Choi\email{choim@battelleecology.org} \cr
#' 
#' @description Assign the calibration file(s) for a sensor ID to each data day which it applies between a 
#' given start and end date.
#' Valid date ranges and certificate numbers in calibration files are used to determine the most applicable
#' calibration for each data day. The most applicable cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#' Note that calibrations with a valid date range beginning after the data day of interest are treated
#' as if they don't exist, which expired calibrations are considered applicable after the valid date
#' range if no other valid calibration exists.
#'     
#' @param DirIn Character value. The input path to the calibration files from a single sensor ID and term,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/SOURCE_TYPE/SOURCE_ID/TERM \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' The TERM folder holds any number of calibration files pertaining to the SOURCE_ID and TERM combination.  
#' There may be no further subdirectories of TERM.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/prt/27134/resistenace \cr
#'     
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param TimeBgn POSIX. The minimum date for which to assign calibration files.
#' @param TimeEnd POSIX. The maximum date for which to assign calibration files.
#' @param PadDay (optional). 2-element difftime object with units of days indicating the days to include applicable 
#' calibration files before/after a given data day. A negative value will copy in the calibration file(s) 
#' that are applicable to the given data day AND # number of days before the data day. A positive value 
#' will copy in the calibration file(s) applicable to the given data day AND # number of days after the data day. 
#' Default is 0. For example, if the current data day is 2019-01-15, "PadDay=-2" will copy in any calibration file(s)
#' that are applicable between 2019-01-13 00:00 and 2019-01-15 24:00. "PadDay=2" will copy in calibration file(s) 
#' that are applicable between 2019-01-15 00:00 and 2019-01-17 24:00. To provide both negative and positive pads 
#' (a window around a given day), separate the values with pipes (e.g. "PadDay=-2|2"). 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/SOURCE_ID/calibration/TERM, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (TERM) is populated with the calibration files applicable to the year, month, day, source_id, 
#' and term indicated in the path (plus any additional calibration files according to the PadDay input). 
#'  

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.cal.asgn(DirIn="/pfs/proc_group/prt/27134/resistenace",
#'              DirOutBase="/pfs/out",
#'              TimeBgn=as.POSIXct('2019-01-01',tz='GMT),
#'              TimeEnd=as.POSIXct('2019-06-01',tz='GMT),
#'              PadDay=as.difftime(c(-1,1),units='days')
#'              )
# changelog and author contributions / copyrights
#   Mija Choi (2021-04-22)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.cal.asgn.R\n")

# Unit test of wrap.cal.asgn.R
test_that("Unit test of wrap.cal.asgn.R", {
  source('../../wrap.cal.asgn.R')
  library(stringr)
  
  wk_dir <- getwd()
  testOutputDir = "pfs/out"
  
  # Test scenario 1:: within the valid time range
  # 17596 does not have "active_periods" pass TypeFile = 'asset'
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/calibration/prt/17596/resistance/')
  fileCal <- base::dir(testInputDir)
  fileCalPath <- base::paste0(testInputDir, fileCal)
  
  # Load in the location json and get the location name to verify the test
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal = fileCalPath,log = log)
  
  #==========================================================================================
  # The metadata of test calibration files
  #==========================================================================================
  # #                                 file |       timeValiBgn |        timeValiEnd |    id |
  #------------------------------------------------------------------------------------------
  # 1     30000000009997_WO7799_122595.xml |2019-01-14 21:22:11| 2055-12-31 00:00:00| 122595|
  # 2 30000000009997_WO7799_122595_dup.xml |2018-01-14 21:22:11| 2055-12-31 00:00:00| 122595|
  # 3      30000000009997_WO7799_60924.xml |2019-01-01 21:22:11| 2020-08-21 21:22:11|  60924|
  # 4  30000000009997_WO7799_60924_dup.xml |2016-03-14 21:22:11| 2017-08-21 21:22:11|  60924|
  #==========================================================================================
  #
  # Happy path 1: 30000000009997_WO7799_122595_dup.xml and 30000000009997_WO7799_60924_dup.xml
  #               have timeValid. 30000000009997_WO7799_122595_dup.xml higher ID. 
  #               This will be written to the output directory
  #
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.cal.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-06', tz = 'GMT'),
    PadDay=base::as.difftime(c(0,0),units='days')
  )
  fileCalExpected <- fileCal[2]
  fileCalInfo <- NEONprocIS.cal::def.read.cal.xml(NameFile = base::paste0(testInputDir, fileCalExpected),log = log)
  fileCalexpectedPath <- base::paste0(fileCalInfo$file$DATA$MetaData$MaximoID,"/","calibration/resistance/",fileCalExpected)
  testthat::expect_true (any(file.exists(testOutputDir,fileCalexpectedPath, recursive = TRUE)))
  #
  # Happy path 2: 30000000009997_WO7799_122595.xmland 30000000009997_WO7799_60924.xml
  #               have timeValid with starting pad= 13 days, 30000000009997_WO7799_122595.xml higher ID. 
  #               This will be written to the output directory
  #
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  returnedOutputDir <- wrap.cal.asgn(
    DirIn = testInputDir,
    DirOutBase = testOutputDir,
    TimeBgn = as.POSIXct('2019-01-01', tz = 'GMT'),
    TimeEnd = as.POSIXct('2019-01-30', tz = 'GMT'),
    PadDay=base::as.difftime(c(13,0),units='days')
  )
 
  fileCalExpected <- fileCal[1]
  fileCalInfo <- NEONprocIS.cal::def.read.cal.xml(NameFile = base::paste0(testInputDir, fileCalExpected),log = log)
  fileCalexpectedPath <- base::paste0(fileCalInfo$file$DATA$MetaData$MaximoID,"/","calibration/resistance/",fileCalExpected)
  testthat::expect_true (any(file.exists(testOutputDir,fileCalexpectedPath, recursive = TRUE)))
  #
  
})
