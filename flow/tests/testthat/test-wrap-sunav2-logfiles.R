##############################################################################################
#' @title Unit test for wrap.sunav2.logfiles module for NEON IS data processing
#'
#' @author
#' Adapted from template by Mija Choi; updated for sunav2 logfile processing
#'
#' @description  Unit test for wrap.sunav2.logfiles, modeled after test-wrap-qaqc-plau.R.
#'
#' Tests include normal operation, expected output file/directory creation, handling of bad input, and missing files/fields.
#'
##############################################################################################

library(testthat)
context("\n       | Unit test of wrap.sunav2.logfiles module for NEON IS data processing \n")

test_that("Unit test of wrap.sunav2.logfiles.R", {
  # Path to the implementation
  source('../../flow.sunav2.logfiles/wrap.sunav2.logfiles.R')
  library(stringr)
  
  DirIn = "pfs/sunav2_logfiles_analyzer/sample/2020/01/02/SUNAV201234"
  DirOutBase = "pfs/out_sunav2"
  VarAddFileQf = 'logState'
  
  # You may need to adapt these parameters, based on the function's signature
  ParaTest <- list(
    voltage = list(
      term = 'voltage',
      test = c("null", "gap", "range"),
      rmv = c(FALSE, FALSE, TRUE)
    )
    # Add more test variables as appropriate
  )
  
  # Test 1 — Happy path
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  returned_wrap_sunav2_logfiles <- wrap.sunav2.logfiles(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    ParaTest = ParaTest,
    VarAddFileQf = VarAddFileQf
  )
  dirInData <- file.path(DirIn, 'data')
  dirInFlags <- file.path(DirIn, 'flags')
  fileData <- dir(dirInData)
  fileFlags <- dir(dirInFlags)
  dirOutData <- gsub("sunav2_logfiles_analyzer", "out_sunav2", dirInData)
  dirOutFlags <- gsub("sunav2_logfiles_analyzer", "out_sunav2", dirInFlags)
  
  expect_true((file.exists(dirOutData, fileData, recursive = TRUE)) &&
                (file.exists(dirOutFlags, fileFlags, recursive = TRUE)))
  rm(returned_wrap_sunav2_logfiles)
  
  # Test 2 — Directory copy feature (if supported)
  DirSubCopy = "threshold"
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  returned_wrap_sunav2_logfiles <- wrap.sunav2.logfiles(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    ParaTest = ParaTest,
    DirSubCopy = DirSubCopy,
    VarAddFileQf = VarAddFileQf
  )
  dirOutSub <- gsub("sunav2_logfiles_analyzer", "out_sunav2", file.path(DirIn, DirSubCopy))
  expect_true((file.exists(dirOutData, fileData, recursive = TRUE)) &&
                (file.exists(dirOutFlags, fileFlags, recursive = TRUE)) &&
                dir.exists(dirOutSub))
  rm(returned_wrap_sunav2_logfiles)
  
  # Test 3 — Bad data file (wrong format)
  badDatagDirIn = "pfs/sunav2_logfiles_analyzer/sample_wrongData/2020/01/02/SUNAV201234"
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  returned_wrap_sunav2_logfiles <- try(wrap.sunav2.logfiles(
    DirIn = badDatagDirIn,
    DirOutBase = DirOutBase,
    ParaTest = ParaTest,
    DirSubCopy = DirSubCopy,
    VarAddFileQf = VarAddFileQf
  ), silent=TRUE)
  expect_true('try-error' %in% class(returned_wrap_sunav2_logfiles))
  rm(returned_wrap_sunav2_logfiles)
  
  # Test 4 — Missing required column in data
  badDataDirIn = "pfs/sunav2_logfiles_analyzer/sample_missingLogTime/2020/01/02/SUNAV201234"
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  returned_wrap_sunav2_logfiles <- try(wrap.sunav2.logfiles(
    DirIn = badDataDirIn,
    DirOutBase = DirOutBase,
    ParaTest = ParaTest,
    DirSubCopy = DirSubCopy,
    VarAddFileQf = VarAddFileQf
  ), silent=TRUE)
  expect_true('try-error' %in% class(returned_wrap_sunav2_logfiles))
  rm(returned_wrap_sunav2_logfiles)
  
  # Test 5 — More than one threshold config file, if applicable
  badDataDirIn = "pfs/sunav2_logfiles_analyzer/sample_morethanOneThreshold/2020/01/02/SUNAV201234"
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  returned_wrap_sunav2_logfiles <- try(wrap.sunav2.logfiles(
    DirIn = badDataDirIn,
    DirOutBase = DirOutBase,
    ParaTest = ParaTest,
    DirSubCopy = DirSubCopy,
    VarAddFileQf = VarAddFileQf
  ), silent=TRUE)
  expect_true('try-error' %in% class(returned_wrap_sunav2_logfiles))
  rm(returned_wrap_sunav2_logfiles)
  
  # ...Add additional error-condition and special case tests here according to the wrap.sunav2.logfiles requirements...
  
  # Cleanup
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
})