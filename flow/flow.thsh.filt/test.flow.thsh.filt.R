library(testthat)
source("threshold_filter.R")
test_that(
   "Threshold Filter test expect 2 thresholds in the output file",
   {
     DirIn <- "tests/test_input/pfs/threshold"
     DirOut <- "tests/test_output/pfs/out"
     Term <- "temp"
     Ctxt <- "aspirated-single"
     OutPutFile <- file.path(DirOut, "thresholds.json")
     filter_threshold(DirIn,DirOut, Term,Ctxt)
     result <- rjson::fromJSON(file=OutPutFile,simplify=TRUE)
     expect_true(length(result$thresholds) ==2)

   }
 )


test_that(
  "Threshold Filter test expect zero thresholds in the input file",
  {
    DirIn <- "tests/test_input/pfs/threshold_fail"
    DirOut <- "tests/test_output/pfs/threshold_fail"
    Term <- "temp"
    Ctxt <- "aspirated-single"
    OutPutFile <- file.path(DirOut, "thresholds.json")
    filter_threshold(DirIn,DirOut, Term,Ctxt)
    result <- rjson::fromJSON(file = OutPutFile,simplify=TRUE)
    expect_true(length(result$thresholds) ==0)
  }
)