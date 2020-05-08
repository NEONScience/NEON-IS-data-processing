library(testthat)
source("R/def.file.comb.ts.R")


test_that("valid files to mergee",
          {
            file <- c('tests/testthat/def.file.comb.ts/validFiles/testdata.parquet', 'tests/testthat/def.file.comb.ts/validFiles/testflagsdata.parquet')
            nameFile <- "tests/testthat/def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"

            NEONprocIS.base::def.file.comb.ts(file = file, nameVarTime='readout_time')
            

            if (!(length(rpt) == 0)) {
              testthat::expect_true (rpt$dirSplt[1] == 'tests')
              testthat::expect_true (rpt$repo[1] == 'prt_calibrated_location_group')
              testthat::expect_true (rpt$dirRepo[1] == "/prt/2019/01/01/16247/prt_16247_location.json")
              testthat::expect_true (rpt$time[1] == timeInPath)
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
