#' @title Unit test for def.map.char.gsub called in wrap.schm.map.char.gsub
#' @author Guy Litt
#' @description Unit tests for def.map.char.gsub
#' @seealso flow.tsdl.comb.splt

# unit test Flow scripts
# changelog and author contributions / copyrights
#   Guy Litt (2021-05-20)
#     Original Creation
##############################################################################################
# Define test context
library(testthat)
# setwd("./flow/flow.tsdl.comb.splt/")
setwd("./tests/testthat/")

testthat::context("\n  Unit test of def.map.char.gsub.R\n")


# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of def.map.char.gsub.R", {
  source('../../def.map.char.gsub.R')
  wk_dir <- getwd()
  
  # ----------------------------------------------------------------
  # TEST SCENARIO #1.0 it all works and data return in expected format
  
  rslt <- testthat::evaluate_promise(def.map.char.gsub(pattFind = "hur",
                    replStr = "now",
                    obj = "testingthisrighthur",
                    log = NULL))
  
  testthat::expect_identical(rslt$result,"testingthisrightnow")
  testthat::expect_true(length(rslt$warnings) ==0)
  testthat::expect_true(length(rslt$messages) ==0)
  
  # TEST SCENARIO 1.1 Warning generated when mutliple pattFind exist:
  rsltEdge <- testthat::evaluate_promise(def.map.char.gsub(pattFind = "hur",
                                                           replStr = "now",
                                                           obj = "testingthisrighthurhur",
                                                           log = NULL))
  testthat::expect_true(base::grepl("multiple", rsltEdge$warnings))  
  
  # TEST SCENARIO 1.2 Multiple pattFinds and replStr
  rsltMult <- testthat::evaluate_promise(def.map.char.gsub(pattFind = c("this","hur"),
                                                           replStr = c("that", "now"),
                                                           obj = "testingthisrighthurhur",
                                                           log = NULL))
  
  testthat::expect_equal(1, base::length(rsltMult$result))
  
  
  # TEST SCENARIO 1.3 Multiple objs pattFinds and replStr
  rsltMult <- testthat::evaluate_promise(def.map.char.gsub(pattFind = c("this","hur"),
                                                           replStr = c("that", "now"),
                                                           obj = c("testingthisrighthurhur","thishurstring"),
                                                           log = NULL))
  
  testthat::expect_equal(2, base::length(rsltMult$result))
  testthat::expect_true(base::grepl("multiple", rsltMult$warnings))
  testthat::expect_false(base::grepl("thishurstring", rsltMult$warnings))
  
  
  # TEST SCENARIO #2 Incorrect inputs:
  rsltNotEnufRepl <- try(testthat::evaluate_promise(def.map.char.gsub(pattFind = c("this","hur"),
                                                           replStr = c("that"),
                                                           obj = c("testingthisrighthurhur","thishurstring"),
                                                           log = NULL)))
  testthat::expect_identical(class(rsltNotEnufRepl), "try-error")
  
})