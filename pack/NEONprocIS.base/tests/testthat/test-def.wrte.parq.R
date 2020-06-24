#library(testthat)
#source("R/def.wrte.parq.R")


test_that("write parquet file with basic parameter",
     {
       data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
       NameFile <- 'out.parquet'
       NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile)
       expect_true (file.exists(NameFile))
       if (file.exists(NameFile)) { file.remove(NameFile)}

     })

test_that("write parquet file with dict length of one is sent as a parameter",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict)
            expect_true (file.exists(NameFile))
            if (file.exists(NameFile)) { file.remove(NameFile)}
          })

test_that("when dict exists and length is not 1 and not equal to number of columns in data, throw an exception",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE, FALSE, TRUE)
            returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict), silent = TRUE)
            testthat::expect_true((class(returnClass)[1] == "try-error"))
          })

test_that("when schme exists, write the file",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            Schm <- "def.wrte.parq/prt_calibrated.avsc"
            returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict, Schm = Schm), silent = TRUE)
            testthat::expect_true((class(returnClass)[1] == "try-error"))
            if (file.exists(NameFile)) { file.remove(NameFile)}

          })

test_that("when NameFileSchm exists, write the file",
          {
            time1 <- base::as.POSIXct('2019-01-01',tz='GMT')
            time2 <- base::as.POSIXct('2019-01-02',tz='GMT')
            time3 <- base::as.POSIXct('2019-01-03',tz='GMT')
            data <- data.frame(z=c('test1','test2','test3'), l=c(4345, 5342, 6345), x=c(time1, time2, time3), y=c(7.0, 8.0, 9.0), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            NameFileSchm <- "def.wrte.parq/prt_calibrated.avsc"
            rpt <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict, NameFileSchm = NameFileSchm), silent = TRUE)
            testthat::expect_false((class(rpt)[1] == "try-error"))
            testthat::expect_true((length(rpt) == 4))
        #    testthat::expect_true(names(rpt), "source_id" "site_id" "readout_time" "temp")
            
            if (file.exists(NameFile)) { file.remove(NameFile)}
            
          })