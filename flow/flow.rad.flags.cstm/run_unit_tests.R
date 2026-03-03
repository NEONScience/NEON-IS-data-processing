#coverage report of unit tests. Run from flow.rad.flags.cstm directory.

library(covr)

# Change to the test directory where tests expect to run from
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")

# Use relative paths from test directory
cov <- covr::file_coverage(
  source_files = c("../../flow.rad.flags.cstm/wrap.rad.flags.cstm.R",
                   "../../flow.rad.flags.cstm/def.rad.shadow.flags.R",
                   "../../flow.rad.flags.cstm/def.cmp22.heater.flags.R"),
  test_files = "test-wrap-rad-flags-cstm.R"
)

covr::report(cov) 
