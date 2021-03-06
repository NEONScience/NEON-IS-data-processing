#library(testthat)
#source("R/def.validate.list.R")

test_that("If the input parameter is not a list, return false",
          {
            input <- c('key1','1.5', 'key3')
            isList <- try(NEONprocIS.base::def.validate.list(listIn = input), silent = TRUE)
            testthat::expect_false(isList)

          })


test_that("when input paramet is a list and empty , return false",
          {
            input <- list()
            isList <-  def.validate.list(listIn = input)
            testthat::expect_false(isList)
          })


test_that("when input parameter is a list and not empty , return true",
          {

            input <- list("a" = 2.5, "b" = TRUE, "c" = 1:3)
            isList <-  def.validate.list(listIn = input)
            testthat::expect_true(isList)
          })