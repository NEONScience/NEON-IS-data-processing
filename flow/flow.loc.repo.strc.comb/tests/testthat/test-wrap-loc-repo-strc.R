
# unit test Flow scripts
# changelog and author contributions / copyrights
#   Mija Choi (2021-01-11)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.loc.repo.strc.R\n")

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.loc.repo.strc.R", {

  source('C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/wrap.loc.repo.strc.R')
#  source('../../flow.loc.repo.strc/wrap.loc.repo.strc.R')
   
#  source('./flow.loc.repo.strc/wrap.loc.repo.strc.R')
  
  wlrs_returned <-
    wrap.loc.repo.strc(DirIn="/pfs/tempSoil_context_group/prt/2018/01/01",DirOutBase="/pfs/out",Comb=TRUE)
  
}
)



