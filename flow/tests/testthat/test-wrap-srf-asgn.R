
##############################################################################################
#' @title Unit test of wrap.srf.asgn.R, 
#' assign and filter the science review flag file(s) for a GROUP ID to each data day for which each applies

#' @author 
#' Mija Choi\email{choim@battelleecology.org} \cr
#' 
#' @description Assign the science review flag file(s) for a group to each data day 
#' that it applies. When assigning the file to each data day, the information is filtered to exclude 
#' information not relevant to the data day. This includes truncating
#' any applicable dates in the file to the start and end of the data day, and removes things
#' like create/update dates and user comment that may change without changing the flagging behavior
#' Original start/end dates falling within the data day will not be modified. 
#'     
#' @param DirIn Character value. The input path to the location or group files,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/GROUP_ID \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' There may be no further subdirectories of GROUP_ID.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/surfacewater-physical_PRLA130100 \cr
#'     
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param TimeBgn POSIX. The minimum date for which to assign location files.
#' @param TimeEnd POSIX. The maximum date for which to assign location files.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/YEAR/MONTH/DAY/GROUP_ID, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (GROUP_ID) is populated with the filtered SRF file(s) applicable to the year, month, and 
#' day indicated in the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.srf.asgn(DirIn="pfs/surfacewater-physical_ARIK101100/",
#'               DirOutBase="pfs/out",
#'               TimeBgn=as.POSIXct('2020-01-01',tz='GMT'),
#'               TimeEnd=as.POSIXct('2020-13-31',tz='GMT'),
#'               )

# changelog and author contributions / copyrights
#   Mija Choi (2023-03-06)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.srf.asgn.R\n")

# Unit test of wrap.cal.asgn.R
test_that("Unit test of wrap.srf.asgn.R", {
  source('../../flow.srf.asgn/wrap.srf.asgn.R')
  library(stringr)
  #
  wk_dir <- getwd()
  testOutputBase = base::paste0(wk_dir, '/', 'pfs/out')
  group_id = 'surfacewater-physical_ARIK130100'
  sr_flags = 'science_review_flags'
  TimeBgn = as.POSIXct('2019-12-28',tz='GMT')
  TimeEnd = as.POSIXct('2020-12-31',tz='GMT')
  inputTimeEnd = as.POSIXct('2020-01-03',tz='GMT') 
  
  # 2 periods to process
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/surfacewaterPhysical_testSRF/surfacewater-physical_ARIK130100/')

  wrap.srf.asgn(DirIn=testInputDir, DirOutBase=testOutputBase, TimeBgn= TimeBgn, TimeEnd=TimeEnd)
  yearBgn = format(TimeBgn, format="%Y")
  monBgn = format(TimeBgn, format="%m")
  dayBgn = format(TimeBgn, format="%d")
  yearEnd = format(inputTimeEnd, format="%Y")
  monEnd = format(inputTimeEnd, format="%m")  
  # If the assignment period ends at 00:00 on a day, remove that day
  dayEndMinus1 = format(inputTimeEnd-1, format="%d")
  testOutputDirBgn <-  base::paste0(testOutputBase,'/',yearBgn,'/',monBgn,'/',dayBgn,'/',group_id, '/',sr_flags, '/')
  testOutputDirEnd <-  base::paste0(testOutputBase,'/',yearEnd,'/',monEnd,'/',dayEndMinus1,'/',group_id, '/',sr_flags, '/')
  
  testthat::expect_true(file.exists(testOutputDirBgn))
  testthat::expect_true(file.exists(testOutputDirEnd))
  
  # no periods to process
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  wrap.srf.asgn(DirIn=testInputDir, DirOutBase=testOutputBase, TimeBgn=as.POSIXct('2021-01-01',tz='GMT'),
                TimeEnd=as.POSIXct('2021-12-31',tz='GMT'))
  
  # no srf files as input
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/surfacewaterPhysical_testSRF/surfacewater-physical_noFiles/')
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  wrap.srf.asgn(DirIn=testInputDir, DirOutBase=testOutputDir, TimeBgn=as.POSIXct('2019-12-01',tz='GMT'),
                TimeEnd=as.POSIXct('2020-12-31',tz='GMT'))
 
  # 2 srf files as input
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/surfacewaterPhysical_testSRF/surfacewater-physical_2Files/')
  wrap.srf.asgn(DirIn=testInputDir, DirOutBase=testOutputBase, TimeBgn=as.POSIXct('2019-12-01',tz='GMT'),
                TimeEnd=as.POSIXct('2020-12-31',tz='GMT'))
  
  
})
