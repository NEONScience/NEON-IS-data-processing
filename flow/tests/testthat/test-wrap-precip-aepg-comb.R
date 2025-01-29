##############################################################################################
#' @title Test of wrap.precip.aepg.comb.R which is wrapper function

#' @description wrap.precip.aepg.comb.R is to compute average precipitation computed for the
#' Belfort AEPG600m sensor that was generated from different central days of the smoothing algorithm
#'

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path is the folder:
#'         /stats
#'         /flags
#' The stats and flags folders hold the output from flow.precip.aepg.smooth.R
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the the average computed precipitation for each data day,
#' where DirOutBase replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory
#' structure of the input path. Only the stats terminal directory is output. (Flags do not need averaging
#' across computation days.)
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # NOT RUN
#' DirIn <- '/scratch/pfs/precipWeighing_ts_pad_smoother/2022/07/28'
#' DirOutBase <- '/scratch/pfs/out'
#' wrap.precip.aepg.comb(DirIn,DirOutBase)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Mija Choi (2025-01-07)
#     Initial creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.precip.aepg.comb.R\n")

# Unit test of wrap.precip.aepg.comb.R
test_that("Unit test of wrap.precip.aepg.comb.R", {
  source('../../flow.precip.aepg.comb/wrap.precip.aepg.comb.R')
  library(stringr)
  #
  testOutputBase = "pfs/out"
  testInputDir <-
    'pfs/pW_ts_pad_smoother/2022/07/28/pW_BLUE900000/aepg600m_heated/CFGLOC103882'
  
  dirParts <- NEONprocIS.base::def.dir.splt.pach.time(testInputDir)
  testOutputDir <- paste0(testOutputBase, dirParts$dirRepo)
  
  #
  # Test 1. DirSubCopy is Null
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <-
    wrap.precip.aepg.comb(DirIn = testInputDir, DirOutBase = testOutputBase)
  
  testOutputStatsDir <- paste0(testOutputDir, '/stats')
  testthat::expect_true(file.exists( testOutputStatsDir, recursive = TRUE))
  
  # Test 2. DirSubCopy not Null, for example, DirSubCopy = 'aaa'. 
  # The input files are copied over under DirSubCopy along with the output. So, 
  # pfs/out/2022/07/28/pW_BLUE900000/aepg600m_heated/CFGLOC103882/ will have DirSubCopy, 'aaa', and the output directory, stats 
  # pfs/out/2022/07/28/pW_BLUE900000/aepg600m_heated/CFGLOC103882/aaa/ will have stats and flags, copied from the input directory, 
  # pfs/pW_ts_pad_smoother/2022/07/28/pW_BLUE900000/aepg600m_heated/CFGLOC103882/
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  DirSub = 'aaa'
  
  returnedOutputDir <-
    wrap.precip.aepg.comb(DirIn = testInputDir,
                          DirOutBase = testOutputBase,
                          DirSubCopy = DirSub)
  
  testOutputCopyDirSub <- paste0(testOutputDir, '/aaa')
  testOutputCopyDirSubStats <- paste0(testOutputCopyDirSub, '/stats')
  testOutputCopyDirSubFlags <- paste0(testOutputCopyDirSub, '/flags') 
  testthat::expect_true(file.exists(testOutputCopyDirSub, recursive = TRUE))
  testthat::expect_true(file.exists(testOutputCopyDirSubStats, recursive = TRUE))
  testthat::expect_true(file.exists(testOutputCopyDirSubFlags, recursive = TRUE))
  
})