test_that("Flow Calibration Filter", {
  DirEXTENSION <- "prt/2019/01/01"
  DIRIN <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
  DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output"
  DirSubCopy <- "data"
  filter_calibration_files(DirIn,DirOut, DirSubCopy)
  expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())

})

# test_that("Most recent file is copied", {
#   DirIn <- "/test_input/prt/2018/Nov/"
#   DirOut <- "/test_output"
#   DirSubCopy <- "/data"
#   expect_error(filter_calibration_files(DirIn,DirOut, DirSubCopy))
# })debugSource('C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/testthat/test_flow_calibration_filter.R')


# 
# test_that("valide Data range is copied", {
#   DirIn <- "/test_input/prt/2018/Nov/"
#   DirOut <- "/test_output"
#   DirSubCopy <- "/data"
#   expect_error(filter_calibration_files(DirIn,DirOut, DirSubCopy))
# })
# 
# test_that("No Valid Data range exists", {
#   DirIn <- "/test_input/prt/2018/Nov/"
#   DirOut <- "/test_output"
#   DirSubCopy <- "/data"
#   expect_error(filter_calibration_files(DirIn,DirOut, DirSubCopy))
# })
# 
# test_that("No Valid Data range exists but copied the lates expired file", {
#   DirIn <- "/test_input/prt/2018/Nov/"
#   DirOut <- "/test_output"
#   DirSubCopy <- "/data"
#   expect_error(filter_calibration_files(DirIn,DirOut, DirSubCopy))
# })
getwd()
