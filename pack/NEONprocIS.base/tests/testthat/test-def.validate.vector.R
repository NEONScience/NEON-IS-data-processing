#library(testthat)
#source("R/def.validate.vector.R")

test_that("if not vector, return falseh",
          {
            inputList <- list("a" = "2.5", "b" = TRUE, "c" = 1:3)
            returnValue <- def.validate.vector(vectIn = inputList, TestEmpty=FALSE, TestNumc=FALSE)
            testthat::expect_false(returnValue)
          })

test_that("vector elements are not numeric and TestNumc is sent as True, return false",
          {
            inputList <- c("test1", "test2", "test3" )
            returnValue <- def.validate.vector(vectIn = inputList, TestEmpty=FALSE, TestNumc=TRUE)
            testthat::expect_false(returnValue)
          })

test_that("empty vector and TestEmpty is sent as true, return false",
          {
            inputVect <- numeric(0)
            returnValue <- def.validate.vector(vectIn = inputVect, TestEmpty=TRUE, TestNumc=FALSE)
            testthat::expect_false(returnValue)
          })

test_that("Non empty vector and TestEmpty is sent as true and TestNumc is sent as true, return true",
          {
            inputVect <- c(1, 2, 3)
            returnValue <- def.validate.vector(vectIn = inputVect, TestEmpty=TRUE, TestNumc=TRUE)
            testthat::expect_true(returnValue)
          })