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

test_that(
  "Threshold Filter test expect a thresholds with only Term and no Context in the input file",
  {
    DirIn <- "tests/test_input/pfs/threshold_with_only_term"
    DirOut <- "tests/test_output/pfs/threshold_with_only_term"
    Term <- "dewPoint"
    Ctxt <- "aspirated-single"
    OutPutFile <- file.path(DirOut, "thresholds.json")
    filter_threshold(DirIn, DirOut, Term=Term)
    result <- rjson::fromJSON(file = OutPutFile,simplify=TRUE)
    expect_true(length(result$thresholds) ==3)
  }
)

test_that(
  "Threshold Filter test expect a thresholds with only context and no term in the input file",
  {
    DirIn <- "tests/test_input/pfs/threshold_with_only_context"
    DirOut <- "tests/test_output/pfs/threshold_with_only_context"
  # Term <- "temp"
    Ctxt <- "aspirated-single"
    OutPutFile <- file.path(DirOut, "thresholds.json")
    filter_threshold(DirIn,DirOut,Ctxt = Ctxt)
    result <- rjson::fromJSON(file = OutPutFile,simplify=TRUE)
    expect_true(length(result$thresholds) ==2)
  }
)
