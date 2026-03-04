#coverage report of unit tests. Run from flow.precip.bucket directory.

library(covr)

# Change to the test directory where tests expect to run from
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")

# Use relative paths from test directory
cov <- covr::file_coverage(
  source_files = c("../../flow.precip.bucket/wrap.precip.bucket.R"),
  test_files = "test-wrap-precip-bucket.R"
)

covr::report(cov)
