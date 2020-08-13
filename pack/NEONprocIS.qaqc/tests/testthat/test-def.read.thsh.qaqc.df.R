#library(testthat)
#source("R/def.read.thsh.qaqc.df.R")

test_that("if not valid dataframe, return false",
          {

            returnValue <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = 'tests/testthat/def.read.thsh.qaqc.df/thresholds.json')
            testthat::expect_false((class(returnValue)[1] == "try-error"))
            testthat::expect_equal(returnValue$context[1], "NA")
            testthat::expect_equal(returnValue$context[3], "soil|water")

          })

test_that("when threshold is empty",
          {
            returnValue <- try(NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = NULL), silent = TRUE)
            testthat::expect_true((class(returnValue)[1] == "try-error"))

          })
