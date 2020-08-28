#library(testthat)
#source("R/def.time.bin.diff.R")

test_that("divide one day into 30 minutes",
          {
            WndwBin <- base::as.difftime(30,units="mins")
            WndwTime <- base::as.difftime(1,units='days')

            returnList <-NEONprocIS.base::def.time.bin.diff(WndwBin = WndwBin,
                                                            WndwTime = WndwTime)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(returnList$timeBgnDiff[1], 0)
            testthat::equals(returnList$timeBgnDiff[48], 84600)
            testthat::equals(length(returnList$timeBgnDiff), 48)
            testthat::equals(returnList$timeEndDiff[1], 30)
            testthat::equals(returnList$timeEndDiff[24], 720)
            testthat::equals(length(returnList$timeEndDiff), 48)

          }

)

test_that("divide one day into 1 hour bins",
          {
            WndwBin <- base::as.difftime(1,units="hours")
            WndwTime <- base::as.difftime(1,units='days')

            returnList <-NEONprocIS.base::def.time.bin.diff(WndwBin = WndwBin,
                                                            WndwTime = WndwTime)
            testthat::expect_true(is.list(returnList))
            expect_true (length(returnList) == 2)
            testthat::equals(returnList$timeBgnDiff[1], 0)
            testthat::equals(returnList$timeBgnDiff[24], 82800)
            testthat::equals(length(returnList$timeBgnDiff), 24)
            testthat::equals(returnList$timeEndDiff[1], 1)
            testthat::equals(returnList$timeEndDiff[24], 24)
            testthat::equals(length(returnList$timeEndDiff), 24)


          }

)

test_that("divide one day into 37 minutes bins",
          {
            WndwBin <- base::as.difftime(37,units="mins")
            WndwTime <- base::as.difftime(1,units='days')
            
            returnList <- try(NEONprocIS.base::def.time.bin.diff(WndwBin = WndwBin,
                                                            WndwTime = WndwTime), silent = TRUE)
            testthat::expect_true((class(returnList)[1] == "try-error"))
            
          }
          
)