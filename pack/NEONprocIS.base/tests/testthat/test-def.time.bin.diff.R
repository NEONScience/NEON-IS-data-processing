#library(testthat)
#source("R/def.time.bin.diff.R")

test_that("divide one day into 30 minutes",
          {
            WndwBin <- base::as.difftime(30,units="mins")
            WndwTime <- base::as.difftime(1,units='days')

            returnList <-NEONprocIS.base::def.time.bin.diff(WndwBin = WndwBin,
                                                            WndwTime = WndwTime)
            testthat::expect_true(is.list(returnList))
            testthat::expect_true (length(returnList) == 2)
            testthat::expect_equal(as.numeric(returnList$timeBgnDiff[1]), 0)
            testthat::expect_equal(as.numeric(returnList$timeBgnDiff[48]), 1410)
            testthat::expect_equal(length(returnList$timeBgnDiff), 48)
            testthat::expect_equal(as.numeric(returnList$timeEndDiff[1]), 30)
            testthat::expect_equal(as.numeric(returnList$timeEndDiff[24]), 720)
            testthat::expect_equal(length(returnList$timeEndDiff), 48)

          }

)

test_that("divide one day into 1 hour bins",
          {
            WndwBin <- base::as.difftime(1,units="hours")
            WndwTime <- base::as.difftime(1,units='days')

            returnList <-NEONprocIS.base::def.time.bin.diff(WndwBin = WndwBin,
                                                            WndwTime = WndwTime)
            testthat::expect_true(is.list(returnList))
            testthat::expect_true (length(returnList) == 2)
            testthat::expect_equal(returnList$timeBgnDiff[1], as.difftime(0,units='hours'))
            testthat::expect_equal(returnList$timeBgnDiff[24], as.difftime(23,units='hours'))
            testthat::expect_equal(length(returnList$timeBgnDiff), 24)
            testthat::expect_equal(returnList$timeEndDiff[1], as.difftime(1,units='hours'))
            testthat::expect_equal(returnList$timeEndDiff[24], as.difftime(24,units='hours'))
            testthat::expect_equal(length(returnList$timeEndDiff), 24)


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
