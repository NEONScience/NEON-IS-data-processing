##############################################################################################
#' @title Unit test for Copy a directory with a symbolic link

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Definition function. Copy a source directory to a destination directory with a symbolic link
#'
#' LnkSubObj Logical (default FALSE). TRUE: Instead of symbolically linking the entire directory DirSrc
#' to DirDest, create the 'child' directory in DirDest and link each individual object.
#' For example, if DirSrc='/parent/child/' and DirDest='/newparent/', the 'child' directory will be created in
#' DirDest ('/newparent/child') and each individual object in '/parent/child/' will be symbolically linked in
#' '/newparent/child'. Note that the output directory structure will be the same regardless of the choice here,
#' but TRUE is useful for situations in which you want to write additional objects in '/newparent/child/'
#' either prior to executing this function or afterward. TRUE will allow you to do this, whereas
#' FALSE will not.
#'
#' @examples
#' def.dir.copy.symb(DirSrc='/scratch/pfs/proc_group/prt/27134/2019/01/01',DirDest='pfs/out/prt/27134/2019/01')

# changelog and author contributions / copyrights
#   Mija Choi (2022-03-22)
#     add Test of LnkSubObj=TRUE
##############################################################################################
# Define test context
context("\n                      Unit test of def.dir.copy.symb.R\n")
#
test_that("Unit test of def.dir.copy.symb.R", {
  # Test 1
  
  inputDir <- c('def.dir.copy.symb/test1', 'def.dir.copy.symb/test2')
  
  outputDir <- c(
    'def.dir.copy.symb/output/test1',
    'def.dir.copy.symb/output/test2',
    'def.dir.copy.symb/output/test3'
  )
  
  report <- try(NEONprocIS.base::def.dir.copy.symb(DirSrc = inputDir, DirDest = outputDir), silent = TRUE)
  testthat::expect_true((class(report)[1] == "try-error"))
  
  # Test 2
  # pass LnkSubObj=TRUE
  # If DirSrc='/parent/child/' and DirDest='/newparent/', the 'child' directory will be created in
  # DirDest ('/newparent/child') and each individual object in '/parent/child/' will be symbolically linked in
  # '/newparent/child'
  #
  inputDir <- c('def.dir.copy.symb/test1')
  
  outputDir <- c('output')
  
  report <- NEONprocIS.base::def.dir.copy.symb(DirSrc = inputDir,
                                               DirDest = outputDir,
                                               LnkSubObj = TRUE)
  
  testthat::expect_true (any(file.exists(outputDir, recursive = TRUE)))
  
  if (dir.exists(outputDir)) {
    unlink(outputDir, recursive = TRUE)
  }
  
  # Test 3 when all source directories don't exist
  
  inputDir <- c('def.dir.copy.symb/test1', 'def.dir.copy.symb/test3h')
  outputDir <- 'def.dir.copy.symb/output'
  report <- try(NEONprocIS.base::def.dir.copy.symb(inputDir, outputDir),silent = TRUE)
  testthat::expect_false((class(report)[1] == "try-error"))
  base::suppressWarnings(base::unlink(outputDir, recursive = TRUE))# For some reason it works best to do a complete rewrite
  
})
