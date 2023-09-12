#library(testthat)
#source("R/def.read.parq.ds.R")
test_that("Read parquet dataset",
          {
            # Successful: read in dataset as class "arrow_dplyr_query"
            inputPath <- c('def.read.parq.ds/')
            ds <- NEONprocIS.base::def.read.parq.ds(fileIn = inputPath)
            testthat::expect_true ("arrow_dplyr_query" %in% class(ds))
            testthat::expect_true(names(ds)[3]=='readout_time')
    
            # Successful: read in dataset as data frame
            ds <- NEONprocIS.base::def.read.parq.ds(fileIn = inputPath,
                                                    Df=TRUE)
            testthat::expect_true ("data.frame" %in% class(ds))
            testthat::expect_true(names(ds)[3]=='readout_time')
            readout_time_tail <- base::as.POSIXct(utils::tail(ds$readout_time,1),format="%Y-%m-%dT%H:%M",tz='GMT')
            readout_time_1 <- base::as.POSIXct(ds$readout_time[1],format="%Y-%m-%dT%H:%M",tz='GMT')
            testthat::expect_true(readout_time_tail <= readout_time_1)
            
            # Successful: read in dataset as data frame and sort time
            ds <- NEONprocIS.base::def.read.parq.ds(fileIn = inputPath,
                                                    VarTime='readout_time',
                                                    Df=TRUE)
            testthat::expect_true ("data.frame" %in% class(ds))
            testthat::expect_true(names(ds)[3]=='readout_time')
            testthat::expect_true(utils::tail(ds$readout_time,1) > ds$readout_time[1])
            
            # Successful: read in dataset and filter columns
            ds <- NEONprocIS.base::def.read.parq.ds(fileIn = inputPath,
                                                    Var=c('readout_time','voltage'),
                                                    VarTime='readout_time',
                                                    Df=TRUE)
            testthat::expect_true ("data.frame" %in% class(ds))
            testthat::expect_true(ncol(ds)==2)
            testthat::expect_true(names(ds)[1]=='readout_time')
            testthat::expect_true(names(ds)[2]=='voltage')
            testthat::expect_true(utils::tail(ds$readout_time,1) > ds$readout_time[1])
            
          })
