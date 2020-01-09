DirEXTENSION <- "prt/2019/01/01"
DirIn <- file.path("/Users/rmarkel/gitcom/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
DirOut <- "/Users/rmarkel/gitcom/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output"
DirSubCopy <- "data"
#source("flow.cal.filt.R")
filter_calibration_files(DirIn,DirOut, DirSubCopy)