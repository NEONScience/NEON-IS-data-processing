library(testthat)

test_that(
   "Threshold Filter test expect 2 thresholds in the output files",
   {
     DirIn <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_input/pfs/threshold"
     DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_output/pfs/out"
     Term <- "temp"
     Ctxt <- "aspirated-single"
     OutPutFile <- file.path(DirOut, "thresholds.json")
     filter_threshold(DirIn,DirOut, Term,Ctxt)
     result <- fromJSON(file = OutPutFile)
     print(result$thresholds)
     expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
   }
 )


test_that(
  "Threshold Filter test expect zero thresholds in the input file",
  {
    DirIn <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_input/pfs/threshold_fail"
    DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.thsh.filt/tests/test_output/pfs/threshold_fail"
    Term <- "temp"
    Ctxt <- "aspirated-single"
    OutPutFile <- file.path(DirOut, "thresholds.json")
    filter_threshold(DirIn,DirOut, Term,Ctxt)
    result <- fromJSON(file = OutPutFile)
    result <- base::do.call(base::rbind,result)
    print(result$thresholds)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)