#library(testthat)
#source("R/def.rcd.fix.miss.na.R")

test_that("fixing the badtime values",
          {
            data <- NEONprocIS.base::def.read.parq(NameFile ="def.rcd.fix.miss.na/testdata.parquet")
            inputfilepaths <- c('def.rcd.fix.miss.na/testdata.parquet', 'def.rcd.miss.na/valid_files/testflagsdata.parquet')
            returnList <-NEONprocIS.base::def.rcd.miss.na(fileData=inputfilepaths)
            output_filename <- NEONprocIS.base::def.rcd.fix.miss.na(data=data, timeBad=returnList$timeBad,valuBad='test')
            expect_true (length(output_filename$readout_time) == 6)
            testthat::equals(length(output_filename$suspectCalQF[6]), "test")
          }

)
