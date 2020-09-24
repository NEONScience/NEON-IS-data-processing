#library(testthat)
#source("R/def.spk.mad.R")

test_that("method A, basic test",
          {
            data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
            # Method A
            qfSpk <- def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=5)
            testthat::expect_equal(length(qfSpk), 20)
          })
# test_that("when data vector is not valid, stop processing",
#           {
#             data <- c()
#             # Method A
#             qfSpk <- try(def.spk.mad(data=data,Meth='A',ThshMad=7,Wndw=5), silent = TRUE)
#             testthat::expect_true((class(qfSpk)[1] == "try-error"))
#           })
# test_that("when method length is greater than 1, stop processing",
#           {
#             data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
#             qfSpk <- try(def.spk.mad(data=data, Meth=c('A', 'B'), ThshMad=7, Wndw=5), silent = TRUE)
#             testthat::expect_true((class(qfSpk)[1] == "try-error"))
#           })
# test_that("when ThshMad length is greater than 1, stop processing",
#           {
#             data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
#             qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=c(7,8), Wndw=5), silent = TRUE)
#             testthat::expect_true((class(qfSpk)[1] == "try-error"))
#           })
# test_that("when Wndw length is greater than 1, stop processing",
#           {
#             data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
#             qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=7, Wndw=c(5,6)), silent = TRUE)
#             testthat::expect_true((class(qfSpk)[1] == "try-error"))
#           })
# test_that("when Wndw length is even",
#           {
#             data <- c(1,2,3,4,3,2,1,2,3,4,50,3,2,1,2,3,4,3,2,1)
#             qfSpk <- try(def.spk.mad(data=data, Meth=c('A'), ThshMad=, Wndw=6), silent = TRUE)
#             testthat::expect_false((class(qfSpk)[1] == "try-error"))
#             testthat::equals(qfSpk[2], -1)
#             testthat::equals(qfSpk[1], -1)
#             testthat::equals(qfSpk[3], -1)
#             testthat::equals(qfSpk[4],  0)
#             testthat::equals(qfSpk[5], 0)
#             testthat::equals(qfSpk[6], 0)
#             testthat::equals(qfSpk[18], -1)
#             testthat::equals(qfSpk[19], -1)
#             testthat::equals(qfSpk[20], -1)
#             testthat::equals(qfSpk[11], 1)
#           })