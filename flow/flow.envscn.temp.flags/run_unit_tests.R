#coverage report of unit tests. Run from flow.envscn.temp.flags directory.

library(covr)

# Change to the test directory where tests expect to run from
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")

# Use relative paths from test directory
cov <- covr::file_coverage(
  source_files = c("../../flow.envscn.temp.flags/def.sort.qf.cols.R",
                   "../../flow.envscn.temp.flags/def.find.temp.sensor.R",
                   "../../flow.envscn.temp.flags/def.apply.temp.flags.R",
                   "../../flow.envscn.temp.flags/wrap.envscn.temp.flags.R",
                   "../../flow.envscn.temp.flags/def.calc.temp.flags.R",
                   "../../flow.envscn.temp.flags/def.load.temp.sensors.R"),
  test_files = "test-wrap-envscn-temp-flags.R"
)

covr::report(cov)
