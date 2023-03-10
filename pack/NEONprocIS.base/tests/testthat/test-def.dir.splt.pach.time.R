
test_that("correct date format",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"

            rpt <- NEONprocIS.base::def.dir.splt.pach.time(nameFile)
            timeInPath = base::as.POSIXct('2019-01-01 00:00:00', tz = 'GMT')
            print(typeof(rpt))
            expect_true (length(rpt) == 5)
            testthat::expect_true  (is.list(rpt))

            if (!(length(rpt) == 0)) {
              testthat::expect_true (rpt$dirSplt[1] == 'def.loc.meta')
              testthat::expect_true (rpt$repo[1] == 'prt_calibrated_location_group')
              testthat::expect_true (rpt$dirRepo[1] == "/prt/2019/01/01/16247/prt_16247_location.json")
              testthat::expect_true (rpt$time[1] == timeInPath)
            }

          })

test_that(" without date in the directory structue",
          {
            nameFile <-
              "def.dir.splt.pach.time/test_input/pfs/testFolder/prt/prt_16247_location.json"
            
            rpt <- NEONprocIS.base::def.dir.splt.pach.time(nameFile)
            print(typeof(rpt))
            expect_true (length(rpt) == 5)
            testthat::expect_true  (is.list(rpt))
            
            if (!(length(rpt) == 0)) {
              testthat::expect_true (rpt$dirSplt[2] == 'test_input')
              testthat::expect_true (rpt$repo[1] == 'testFolder')
              testthat::expect_true (rpt$dirRepo[1] == "/prt/prt_16247_location.json")
              testthat::expect_null(rpt$time[1])
            }
            
            
            
          })

test_that(" Path structure does not conform to expectations",
          {
            # No pfs in path structure
            nameFile <-
              "def.dir.splt.pach.time/test_input/testFolder/prt/prt_16247_location.json"
            
            rpt <- try(NEONprocIS.base::def.dir.splt.pach.time(nameFile),silent=FALSE)
            expect_true ('try-error' %in% class(rpt))

            # Path not long enough to determine repo
            nameFile <- "pfs"
            
            rpt <- try(NEONprocIS.base::def.dir.splt.pach.time(nameFile),silent=FALSE)
            expect_true ('try-error' %in% class(rpt))
            
            
            
          })