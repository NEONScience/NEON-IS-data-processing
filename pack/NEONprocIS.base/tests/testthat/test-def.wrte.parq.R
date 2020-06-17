# library(testthat)
# source("R/def.read.parq.R")
# test_that("Parquet file reader considering only first file in the list",
#           {
#             inputfilepaths <- c('def.rcd.miss.na/third_test/incorrecttestdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
#             returnData <- NEONprocIS.base::def.read.parq(NameFile = inputfilepaths)
#             expect_true (length(returnData) == 2)
#             testthat::expect_null(returnData$readout_time)
#     
#           })
