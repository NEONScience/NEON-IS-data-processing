#library(testthat)
#source("R/def.spk.mad.R")

test_that("method A, basic test",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            # Method A
            qfSpk <- def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=5)
            testthat::expect_equal(length(qfSpk), 20)
          })
test_that("when data vector is not valid, stop processing",
          {
            data <- c()
            # Method A
            qfSpk <- try(def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=5), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when method length is greater than 1, stop processing",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('A', 'B'), ThshMad=7, Wndw=5), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when ThshMad length is greater than 1, stop processing",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=c(7,8), Wndw=5), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when Wndw length is greater than 1, stop processing",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=7, Wndw=c(5,6)), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when Wndw length is even",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=7, Wndw=6), silent = TRUE)
            testthat::expect_false((class(qfSpk)[1] == "try-error"))
            testthat::equals(qfSpk[2], -1)
            testthat::equals(qfSpk[1], -1)
            testthat::equals(qfSpk[3], -1)
            testthat::equals(qfSpk[4],  0)
            testthat::equals(qfSpk[5], 0)
            testthat::equals(qfSpk[6], 0)
            testthat::equals(qfSpk[18], -1)
            testthat::equals(qfSpk[19], -1)
            testthat::equals(qfSpk[20], -1)
            testthat::equals(qfSpk[11], 1)
          })

test_that("when Wndw length is odd",
          {
            data <- c(1,2,3,4,3,2)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('B'), ThshMad=7, Wndw=6), silent = TRUE)
            testthat::expect_false((class(qfSpk)[1] == "try-error"))
            testthat::equals(qfSpk[2], 0)
            testthat::equals(qfSpk[1], 0)
            testthat::equals(qfSpk[3], 0)
            testthat::equals(qfSpk[4], 0)
            testthat::equals(qfSpk[5], 0)
            testthat::equals(qfSpk[6], 0)

          })
test_that("when Wndw length is odd and Method B and WndwStep is not a integer, stop processing",
          {
            data <- c(1,2,3,4,3,2)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('B'), ThshMad=7, Wndw=6, WndwStep = c("A")), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when WndwFracSpkMin is greater than 1 stop processing",
          {
            data <- c(1,2,3,4,3,2)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('B'), ThshMad=7, Wndw=6, WndwFracSpkMin=2), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when NumGrp is greater than numData, stop processing",
          {
            data <- c(1,2,3,4,3,2)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('B'), ThshMad=7, Wndw=6, NumGrp=8), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })
test_that("when NaFracMac is greater than 1, stop processing",
          {
            data <- c(1,2,3,4,3,2)
            qfSpk <- try(def.spk.mad(data=data, Meth=c('B'), ThshMad=7, Wndw=6, NaFracMac=8), silent = TRUE)
            testthat::expect_true((class(qfSpk)[1] == "try-error"))
          })