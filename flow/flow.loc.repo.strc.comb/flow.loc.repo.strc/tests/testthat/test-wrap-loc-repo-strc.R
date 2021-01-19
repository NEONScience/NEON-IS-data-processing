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
  #  source('C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/wrap.loc.repo.strc.R')
  
  source('../../wrap.loc.repo.strc.R')
  
  #testInputDir = "/scratch/pfs/prt_location_filter/2019/01/01/14491"
  
  testInputDir= "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt/2019/01/01/3119"
  
  testOutputDir = "pfs/out"
  
  wlrs_returned <- wrap.loc.repo.strc(DirIn = testInputDir,
                       DirOutBase = testOutputDir,
                       Comb = TRUE)
  
})
