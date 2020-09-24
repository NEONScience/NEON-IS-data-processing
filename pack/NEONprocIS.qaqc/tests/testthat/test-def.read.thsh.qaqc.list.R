#library(testthat)
#source("R/def.read.thsh.qaqc.list.R")

test_that("if valid dataframe, return false",
          {

            returnValue <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = 'def.read.thsh.qaqc.df/thresholds.json')
            testthat::expect_false((class(returnValue)[1] == "try-error"))
            testthat::expect_true(is.list(returnValue[[1]]$context))
            testthat::expect_equal(returnValue[[3]]$context[1], "soil")
            testthat::expect_equal(returnValue[[3]]$context[2], "water")
            testthat::expect_false(is.null(returnValue[[1]]$end_date))

          })
test_that("if threshold file is not reachable, return false",
          {

            returnValue <- try(NEONprocIS.qaqc::def.read.thsh.qaqc.list(NameFile = "NameFile"), silent = TRUE)
            testthat::expect_true((class(returnValue)[1] == "try-error"))

          })
test_that("when threshold is empty",
          {
            returnValue <- try(NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh = NULL), silent = TRUE)
            testthat::expect_true((class(returnValue)[1] == "try-error"))

          })
test_that("if listThsh is sent as a parameter, returnValue should be same",
          {
            threshold_name <- list("Time dependent", "Range Threshold Soft Max", "Range Threshold Hard Min", "Range Threshold Hard Min")
            term_name <- list("TFPrecipBulk", "rawVSWC7","temp","rawBarometricPressure")
            location_name = list("ABBY","ABBY","YELL","ABBY")
            my_list <- list(threshold_name, term_name, location_name)

            returnValue <- try(NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh = my_list), silent = TRUE)
            testthat::expect_false((class(returnValue)[1] == "try-error"))
            testthat::expect_equal(returnValue, my_list)

          })
test_that("if strJson is sent as a parameter, returnValue should be same",
          {
            thshRaw <- base::try(paste(base::readLines(con='def.read.thsh.qaqc.df/thresholds.json'), collapse = ""), silent=FALSE)
            returnValue <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(strJson = thshRaw)
            testthat::expect_false((class(returnValue)[1] == "try-error"))
            testthat::expect_true(is.list(returnValue[[1]]$context))
            testthat::expect_equal(returnValue[[3]]$context[1], "soil")
            testthat::expect_equal(returnValue[[3]]$context[2], "water")
            testthat::expect_false(is.null(returnValue[[1]]$end_date))


          })