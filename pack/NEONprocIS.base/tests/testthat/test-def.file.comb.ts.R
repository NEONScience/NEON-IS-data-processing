library(testthat)
source("R/def.file.comb.ts.R")


test_that("valid files to mergee",
          {
            file <- c('tests/testthat/def.file.comb.ts/validFiles/testdata.parquet', 'tests/testthat/def.file.comb.ts/validFiles/testflagsdata.parquet')
           
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

# test_that(" without date in the directory structue",
#           {
#             nameFile <-
#               "tests/testthat/def.dir.splt.pach.time/test_input/pfs/testFolder/prt/prt_16247_location.json"
#             
#             rpt <- NEONprocIS.base::def.dir.splt.pach.time(nameFile)
#             print(typeof(rpt))
#             expect_true (length(rpt) == 5)
#             testthat::expect_true  (is.list(rpt))
#             
#             if (!(length(rpt) == 0)) {
#               testthat::expect_true (rpt$dirSplt[2] == 'testthat')
#               testthat::expect_true (rpt$repo[1] == 'testFolder')
#               testthat::expect_true (rpt$dirRepo[1] == "/prt/prt_16247_location.json")
#               testthat::expect_null(rpt$time[1])
#             }
#             
#             
#             
#           })
