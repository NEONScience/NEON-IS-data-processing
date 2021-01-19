#
# unit test Flow scripts
# changelog and author contributions / copyrights
#   Mija Choi (2021-01-11)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.loc.repo.strc.R\n")

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.loc.repo.strc.R", {
  source('../../wrap.loc.repo.strc.R')
  
  testInputDir = "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt/2019/01/01/3119"
  
  testOutputDir = "pfs/out"
  # Test scenario 1::
  # if Comb = FALSE, default when Comb is not passed in to wrap.loc.repo.strc,
  # then pfs/prt/2019/01/01/3119 copied to pfs/out/2019/01/01/CFGLOC100241/3119/location/
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 2::
  # if Comb = TRUE then 3119 is replaced with the NAME, CFGLOC100241.
  # pfs/prt/2019/01/01/3119/location/ copied to pfs/out/2019/01/01/CFGLOC100241/location/
  #                   =====                                       ==============
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  # clean out the test output dirs and file recursively
  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # If there is no location file, skip
  testInputDir = "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_noFiles/2019/01/01/3119"
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  testInputDir = "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_moreThanOneFile/2019/01/01/3119"
  
  # If there is more than one location file, use the first
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
})
