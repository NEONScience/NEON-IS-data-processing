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
  
  testInputDir= "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt/2019/01/01/3119"
  
  testOutputDir = "pfs/out"
  # Comb = FALSE
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)
  
  if (dir.exists(testOutputDir)) { unlink(testOutputDir, recursive = TRUE)}

  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir,Comb = TRUE)
  
  if (dir.exists(testOutputDir))  { unlink(testOutputDir, recursive = TRUE)}
  
  testInputDir= "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_noFiles/2019/01/01/3119"
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir,Comb = TRUE)
  
  testInputDir= "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_moreThanOneFile/2019/01/01/3119"
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir,Comb = TRUE)
  })
