#coverage report of unit tests. change directories to local repo as needed. 

library (covr)
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")
baseDir = "~/GitHub/NEON-IS-data-processing/flow"
cov <- covr::file_coverage(
  source_files = c(paste0(baseDir,"/flow.rad.flags.cstm/wrap.rad.flags.cstm.R"),
                   paste0(baseDir,"/flow.rad.flags.cstm/def.rad.shadow.flags.R"),
                   paste0(baseDir, "/flow.rad.flags.cstm/def.cmp22.heater.flags.R")),
  test_files = "~/GitHub/NEON-IS-data-processing/flow/tests/testthat/test-wrap-rad-flags-cstm.R"
)

covr::report(cov) 
