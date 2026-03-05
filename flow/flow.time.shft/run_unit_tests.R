#coverage report of unit tests. Run from flow.envscn.temp.flags directory.

library(covr)

# Change to the test directory where tests expect to run from
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")

# Use relative paths from test directory
cov <- covr::file_coverage(
  source_files = c("../../flow.time.shft/wrap.time.shft.R"),
  test_files = "test-wrap-time-shft.R"
)

covr::report(cov)