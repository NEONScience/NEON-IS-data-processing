library(testthat)

test_that(
  "Calibration file exists in the output folder", 
  {
    DirEXTENSION <- "prt/2019/01/01"
    DirIn <- file.path("/Users/rmarkel/gitcom/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
    DirOut <- "/Users/rmarkel/gitcom/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output"
    DirSubCopy <- "data"
    filter_calibration_files(DirIn,DirOut, DirSubCopy)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)