library(testthat)
source("R/def.data.mapp.schm.avro.R")

test_that("make sure missing data column is added",
          {
            
            inputfilepaths <- c('def.rcd.miss.na/valid_files/testdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            
            returnList <-NEONprocIS.base::def.data.mapp.schm.avro(fileData=inputfilepaths)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(length(returnList$timeAll$readout_time), 6)
            testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 07:00:05 GMT")
            
          }
          
)

test_that("Data type is converted when ConvType is sent as true",
          {
            
            inputfilepaths <- c('def.rcd.miss.na/second_test/textformat.txt', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.data.mapp.schm.avro(fileData=inputfilepaths),
                             silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }
          
)

test_that("make sure data is not converted when ConvType is not passed as a parameter",
          {
            
            inputfilepaths <- c('def.rcd.miss.na/second_test/textformat.txt', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.data.mapp.schm.avro(fileData=inputfilepaths),
                             silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }
          
)

