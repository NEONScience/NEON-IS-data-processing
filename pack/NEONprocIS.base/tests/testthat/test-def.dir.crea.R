##############################################################################################
#' @title Unit test for def.dir.crea.R, create output directories

#' @description
#' Definition function. Create one or more output directories given a starting path and 1 or more relative paths

#' @param DirBgn String value. Starting path. Defaults to NULL, in which case DirSub contains the full absolute or relative path.
#' @param DirSub String value or vector. Path to subdirectories to create relative to starting
#' directory
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A logical vector, where TRUE indicates the directory at the index was created. FALSE if it was not created for any reason.

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' NEONprocIS.base::def.dir.crea(DirBgn='/scratch/pfs/proc_group/prt',DirSub=c('relative/path/to/new/dir','relative/path2/to/new/dir2')

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2022-08-02)
#     Add more tests after the original test was written
##############################################################################################
test_that("Data directory created without subpath",
# Test 1, a happy path with valid params
          {
            dirBgn <- "def.dir.crea"
            dirSub <- "dirSub"
            NEONprocIS.base::def.dir.crea(DirBgn = dirBgn, DirSub = dirSub)
            testthat::expect_true(dir.exists(dirBgn))
            if (dir.exists(dirBgn)) {
              unlink(dirBgn, recursive = TRUE)
            }
# Test 2, input directory, DirBgn, is not passed in, the default, NULL, will be assigned
            
            NEONprocIS.base::def.dir.crea(DirSub = dirSub)
            testthat::expect_true(dir.exists(dirSub))
 
# Test 3, input directory, DirBgn, is not passed in and DirSub is NULL, the default, NULL, will be assigned
            
            NEONprocIS.base::def.dir.crea(DirSub = dirSub)
            testthat::expect_true(dir.exists(dirSub))
            if (dir.exists(dirSub)) {
              unlink(dirSub, recursive = TRUE)
            }           
          }
)
