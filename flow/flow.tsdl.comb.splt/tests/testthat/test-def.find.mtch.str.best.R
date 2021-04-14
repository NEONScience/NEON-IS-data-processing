#' @title Unit test for def.find.mtch.str.best.R called in wrap.file.comb.tsdl.splt
#' @author Guy Litt
#' @description 
#' @seealso flow.tsdl.comb.splt

# unit test Flow scripts
# changelog and author contributions / copyrights
#   Guy Litt (2021-04-13)
#     Original Creation
##############################################################################################
# Define test context
library(testthat)
# setwd("~/R/NEON-IS-data-processing-glitt/flow/flow.tsdl.comb.splt/")
setwd("./tests/testthat/")

testthat::context("\n  Unit test of def.find.mtch.str.best.R\n")


test_that("Unit test of wrap.loc.repo.strc.R", {
  source('../../def.find.mtch.str.best.R')
  # Test all good
  obj <- base::c("depth1tsdWaterTempMean", "depth2tsdWaterTempMean","depth9tsdWaterTempMean","depth10tsdWaterTempMean","depth11tsdWaterTempMean",
           "depth1tsdWaterTempMax", "depth2tsdWaterTempMax","depth9tsdWaterTempMax","depth10tsdWaterTempMax","depth11tsdWaterTempMax")
  subFind <- base::c("depth1","depth2","depth3","depth10","depth11")
  
  rsltGood <-  testthat::evaluate_promise(def.find.mtch.str.best(obj, subFind, log=NULL))
  
  testthat::expect_identical(base::names(rsltGood$result), base::c("obj","mtchGrp","idxMtch"))
  
  objSub <- base::gsub(pattern="tsdWaterTempMax",replacement = "", rsltGood$result$obj)
  objSub <- base::gsub(pattern="tsdWaterTempMean",replacement = "", objSub)
  testthat::expect_identical(objSub, rsltGood$result$mtchGrp)
  testthat::expect_identical(rsltGood$output,"")
  testthat::expect_equal(nrow(rsltGood$result),6)
  
  # Shuffle the order of strings:
  obj2 <- base::c("depth13tsdWaterTempMean", "depth2tsdWaterTempMean","depth9tsdWaterTempMean","depth10tsdWaterTempMean","depth1tsdWaterTempMean",
                 "depth13tsdWaterTempMax", "depth2tsdWaterTempMax","depth9tsdWaterTempMax","depth10tsdWaterTempMax","depth1tsdWaterTempMax")
  subFind2 <- base::c("depth11","depth2","depth3","depth10","depth1")
  
  rsltGood2 <-  testthat::evaluate_promise(def.find.mtch.str.best(obj = obj2, subFind = subFind2, log=NULL))
  testthat::expect_identical(rsltGood2$output,"")
  objSub2 <- base::gsub(pattern="tsdWaterTempMax",replacement = "", rsltGood2$result$obj)
  objSub2 <- base::gsub(pattern="tsdWaterTempMean",replacement = "", objSub2)
  
  testthat::expect_identical(objSub2, rsltGood2$result$mtchGrp)
  
  # Test a single subFind string
  rsltSnglRpt <- testthat::evaluate_promise(def.find.mtch.str.best(obj = obj2, subFind = "depth1"))
  testthat::expect_equal(base::nrow(rsltSnglRpt$result),2)
  
  # TESTS on things that don't work:
  # Test a single obj and multiple subFinds
  rsltSnglObjRpt <- testthat::evaluate_promise(try(def.find.mtch.str.best(obj = "depth1tsdWaterTempMean", subFind = subFind2)))
  testthat::expect_true("try-error" %in% base::class(rsltSnglObjRpt$result))
  testthat::expect_true(base::grepl("ERROR",rsltSnglObjRpt$output))
  
  # Test non-character inputs
  rsltNonCharObj <- testthat::evaluate_promise(try(def.find.mtch.str.best(obj = c(3, 2), subFind = subFind2)))
  testthat::expect_true(base::grepl("ERROR",rsltNonCharObj$output ))
  
  rsltNonCharFind <- testthat::evaluate_promise(try(def.find.mtch.str.best(obj = objSub2, subFind = c(NULL,"depth2"))))
  testthat::expect_true(base::grepl("ERROR",rsltNonCharObj$output ))

  
  
  
})
  