##############################################################################################
#' @title Unit test of Route datum errors to specified location

#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Definition function. Route datum paths that errored to a specified error directory.
#' The input path to the erroring datum will be created in the error directory.
#' Optionally, remove any partial output from the errored datum.

#' @param DirDatm Character value. The input path to the datum, structured as follows:
#' #/pfs/BASE_REPO/#, where # indicates any number of parent and child directories
#' of any name, so long as they are not 'pfs'. Note that the path should terminate at
#' the directory containing all the data and metadata (nested in further subdirectories
#' as needed) considered to be one complete entity for processing.
#' For example:
#' DirDatm = /scratch/pfs/proc_group/prt/2019/01/01/27134
#' @param DirErrBase Character value. The path that will replace the #/pfs/BASE_REPO portion of DirDatm
#' @param RmvDataOut Logical. TRUE to remove any partial output for the datum from the output repo. NOTE:
#' Removing partial output only works if the output datum path matches the typical structure where
#' DirOutBase replaces the #/pfs/BASE_REPO portion of DirDatm, but otherwise is the same as DirDatm.
#' If this is not the case, set RmvDataOut to FALSE (which is the default).
#' @param DirOutBase Character value. The path that will replace the #/pfs/BASE_REPO portion of
#' DirIn when writing successful output for the datum.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return The action of creating the path structure to the datum within DirErrBase, having replaced
#' the #/pfs/BASE_REPO portion of DirDatm with DirErrBase. If RmvDataOut is set to true, any partial
#' output for DirDatm will be removed. The output directory is the same as DirDatm but with the
#' #/pfs/BASE_REPO portion of DirDatm replaced with DirOutBase.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run:
#' DirDatm <- 'pfs/proc_group/prt/2019/01/01/27134'
#' DirErrBase <- 'pfs/proc_group_output/errored_datums'
#' RmvDatmOut <- TRUE
#' DirOutBase <- 'pfs/proc_group_output'
#' tryCatch(stop('error!'),error=function(err) def.err.datm(DirDatm=DirDatm,DirErrBase=DirErrBase,RmvDatmOut=RmvDatmOut,DirOutBase=DirOutBase))

#' @seealso tryCatch

# changelog and author contributions / copyrights
#   Mija Choi (2021-01-03)
#     original creation
##############################################################################################

# Define test context
#context("\n       | Route datum errors to specified location\n")

# Test Route datum errors to specified location
test_that("   Testing Route datum errors to specified location", {
  ##########
  ##########  Tests Route datum errors to specified location
  DirDatm <- 'pfs/proc_group/prt/2019/01/01/27134'
  DirDatm_notExist <- 'pfs/not_exist'
  DirErrBase <- 'pfs/proc_group_output/errored_datums'
  DirOutBase <- 'pfs/proc_group_output'
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # Test 1, RmvDatmOut = TRUE
  dirOut <- 'pfs/proc_group_output/prt/2019/01/01/27134'
  dirOutErr <- 'pfs/proc_group_output/errored_datums/prt/2019/01/01/27134'
  dir.create(dirOut,recursive=TRUE)
  returned_err_datm <- NEONprocIS.base::def.err.datm(
    DirDatm = DirDatm,
    DirErrBase = DirErrBase,
    RmvDatmOut = TRUE,
    DirOutBase = DirOutBase
  )
  testthat::expect_false(dir.exists(dirOut))
  testthat::expect_true(dir.exists(dirOutErr))
  
  # Test 2,  DirErrBase = NULL
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  returned_err_datm <- try(NEONprocIS.base::def.err.datm(DirDatm = DirDatm,
                                                         DirErrBase = NULL,
                                                         DirOutBase = DirOutBase),
                           silent = TRUE)
  testthat::expect_equal(base::class(returned_err_datm),'try-error')
  
  # Test 3,  DirOutBase not passed in, default to NULL
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  returned_err_datm <- NEONprocIS.base::def.err.datm(DirDatm = DirDatm,
                                                     DirErrBase = DirErrBase,
                                                     RmvDatmOut = TRUE)
  testthat::expect_null(returned_err_datm)
  testthat::expect_true(dir.exists(dirOutErr))
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # Test 4,  DirDatm is not an existing dir
  #          AND DirOutBase not passed in, default to NULL
  # 
  # if (dir.exists(DirErrBase)) {
  #   unlink(DirErrBase, recursive = TRUE)
  # }
  # returned_err_datm <- NEONprocIS.base::def.err.datm(DirDatm = DirDatm_notExist,
  #                                                    DirErrBase = DirErrBase,
  #                                                    DirOutBase = DirOutBase,
  #                                                    RmvDatmOut = TRUE)
  # testthat::expect_null(returned_err_datm)
  # testthat::expect_false(dir.exists(dirOutErr))
  # 
})
