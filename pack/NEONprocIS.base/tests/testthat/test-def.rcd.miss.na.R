#library(testthat)
#source("R/def.rcd.miss.na.R")

test_that("retrun extra timestamp in the second file",
          {

            inputfilepaths <- c('def.rcd.miss.na/valid_files/testdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')

            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(length(returnList$timeAll$readout_time), 6)
            testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 07:00:05 GMT")

          }

)

test_that("wrong file format",
          {

            inputfilepaths <- c('def.rcd.miss.na/second_test/textformat.txt', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths),
                             silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }

)

test_that("missing readout_time in one of the files",
          {

            inputfilepaths <- c('def.rcd.miss.na/third_test/incorrecttestdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-try(NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths),
                                    silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
          }
)

test_that("extra timestampe in the second file",
          {

            inputfilepaths <- c( 'def.rcd.miss.na/valid_files/testflagsdata.parquet', 'def.rcd.miss.na/valid_files/testdata.parquet')

            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(length(returnList$timeAll$readout_time), 6)
            testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 07:00:05 GMT")

          }
)

test_that("NA in one of the columns",
           {

             inputfilepaths <- c( 'def.rcd.miss.na/NA_test/testdataWithNA.parquet', 'def.rcd.miss.na/valid_files/testdata.parquet')
 
             returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
             testthat::expect_true(is.list(returnList))
             expect_true (length(returnList) == 2)
             testthat::equals(length(returnList$timeAll$readout_time), 6)
             testthat::equals(returnList$timeBad$readout_time[1], "2019-01-01 00:00:02 GMT")
 
           }

)
