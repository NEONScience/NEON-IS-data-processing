test_that("valid files to mergee",
          {
            file <- c('def.file.comb.ts/validFiles/testdata.parquet', 'def.file.comb.ts/validFiles/testflagsdata.parquet')
           
            returnedData <- def.file.comb.ts(file = file, nameVarTime='readout_time')
            
            expect_true (length(returnedData) == 6)
            testthat::expect_true(is.list(returnedData))
            if (!(length(returnedData) == 0)) {
              testthat::expect_true (returnedData$source_id[2] == '16247')
              testthat::equals (returnedData$temp[5], 0.007209014)
              testthat::equals (returnedData$validCalQF[1], 0)
              #testthat::expect_null(returnedData$time[1])
            }
            
          })

test_that("wrong file format",
          {
            file <- c('def.file.comb.ts/validFiles/testdata.txt', 'def.file.comb.ts/validFiles/testflagsdata.parquet')
            
            returnedData <- try(def.file.comb.ts(file = file, nameVarTime='readout_time'),
                                silent = TRUE)
            
            testthat::expect_true((class(returnedData)[1] == "try-error"))
            
          })

test_that("duplicate columns in the input files",
          {
            file <- c('def.file.comb.ts/invalidFiles/testdata.parquet', 'def.file.comb.ts/invalidFiles/testflagsdatadup.parquet')
            
            returnedData <- def.file.comb.ts(file = file, nameVarTime='readout_time')
            
            expect_true (length(returnedData) == 6)
            testthat::expect_true(is.list(returnedData))
            if (!(length(returnedData) == 0)) {
              testthat::expect_true (returnedData$source_id[2] == '16247')
              testthat::equals (returnedData$temp[5], 0.007209014)
              testthat::equals (returnedData$validCalQF[1], 0)
              #testthat::expect_null(returnedData$time[1])
            }
            
          })



