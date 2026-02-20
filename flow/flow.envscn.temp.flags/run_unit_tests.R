#coverage report of unit tests. change directories to local repo as needed. 

library (covr)
setwd("~/GitHub/NEON-IS-data-processing/flow/tests/testthat")
baseDir = "~/GitHub/NEON-IS-data-processing/flow"
cov <- covr::file_coverage(
  source_files = c(paste0(baseDir,"/flow.envscn.temp.flags/def.sort.qf.cols.R"),
                   paste0(baseDir,"/flow.envscn.temp.flags/def.find.temp.sensor.R"),
                   paste0(baseDir, "/flow.envscn.temp.flags/def.apply.temp.flags.R"),
                   paste0(baseDir,"/flow.envscn.temp.flags/wrap.envscn.temp.flags.R"),
                   paste0(baseDir,"/flow.envscn.temp.flags/def.calc.temp.flags.R"),
                   paste0(baseDir,"/flow.envscn.temp.flags/def.load.temp.sensors.R")),
  test_files = "~/GitHub/NEON-IS-data-processing/flow/tests/testthat/test-wrap-envscn-temp-flags.R"
)

covr::report(cov)
