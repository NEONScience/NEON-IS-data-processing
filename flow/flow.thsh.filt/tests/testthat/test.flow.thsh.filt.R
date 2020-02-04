library(testthat)

test_that(
  "Calibration Conversion files first test",
  {
    DirIn <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_input/pfs/threshold/"
    DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_output/pfs/out"
    Term <- "temp"
    Ctxt <- "aspirated-single"
    filter_threshold(DirIn,DirOut, Term,Ctxt)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)
