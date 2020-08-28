#library(testthat)
#source("R/def.schm.parq.pars.R")

test_that("when fileSchm and schm are both null, throw an exceptionr",
          {
            returnData <- try(NEONprocIS.base::def.schm.parq.pars(schm = NULL), silent = TRUE)
            testthat::expect_null((returnData))
          })

test_that("when parquest schm is passed, return all the elements in the schema",
          {
            schm <- arrow::read_parquet(file='def.rcd.fix.miss.na/testdata.parquet',as_data_frame=FALSE)$schema
            returnData <- try(NEONprocIS.base::def.schm.parq.pars(schm = schm), silent = TRUE)
            testthat::expect_false((class(returnData)[1] == "try-error"))
            testthat::expect_true(is.list(returnData))
            testthat::equals(class(returnData$suspectCalQF, "string"))
            testthat::equals(class(returnData$readout_time, "POSIXct"))
          
          })

