#library(testthat)
#source("R/def.validate.dataframe.R")

test_that("if not dataframe, return false",
          {
            validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = "test")
            testthat::expect_false(validDataFrame)
          })

test_that("vector elements are not numeric and TestNumc is sent as True, return false",
          {
            df <- data.frame(var1=double(), var2=character(), stringsAsFactors=FALSE)
            validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = df, TestNa = FALSE, TestNumc = FALSE)
            testthat::expect_false(validDataFrame)
          })

test_that("when there is an NA in the data when TestNa is sent as true, return false",
          {
            df <- data.frame("test", NA, 3, stringsAsFactors=FALSE)
            validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = df, TestNa = TRUE, TestNumc = FALSE)
            testthat::expect_false(validDataFrame)
          })
 
test_that("when nonnumeric values are there in the data frame and TestNumc is sent as true, return false",
           {
              df <- data.frame("test", "test2", 3, stringsAsFactors=FALSE)
              validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = df, TestNa = TRUE, TestNumc = TRUE)
              testthat::expect_false(validDataFrame)
           })

test_that("when TestNameCol is not empty and values are not in data frame, return false",
          {
            df <- data.frame(var1 = "test", var2="test2", var3=3, stringsAsFactors=FALSE)
            testNameCol <- c("var1", "var4")
            validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = df, TestNa = TRUE, TestNameCol = testNameCol)
            testthat::expect_false(validDataFrame)
          })

test_that("when TestNameCol is not empty and values are in data frame, return false",
          {
            df <- data.frame(var1 = "test", var2="test2", var4=3, stringsAsFactors=FALSE)
            testNameCol <- c("var1", "var4")
            validDataFrame <- NEONprocIS.base::def.validate.dataframe(dfIn = df, TestNa = TRUE, TestNameCol = testNameCol)
            testthat::expect_true(validDataFrame)
          })