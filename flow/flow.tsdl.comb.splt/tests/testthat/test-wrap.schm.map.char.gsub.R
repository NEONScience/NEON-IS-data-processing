#' @title Unit test for wrap.schm.map.char.gsub
#' @author Guy Litt
#' @description Unit tests for wrap.schm.map.char.gsub
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

testthat::context("\n  Unit test of wrap.schm.map.char.gsub.R\n")

test_that("Unit test of wrap.schm.map.char.gsub.R", {
  source('../../wrap.schm.map.char.gsub.R')
  source('../../def.map.char.gsub.R')
  source("../../def.schm.avro.pars.map.R")
  wk_dir <- getwd()
  
  
  FileSchm <- "./pfs/schemas/tsdl_col_term_subs.avsc"
  
  # ----------------------------------------------------------------
  # TEST SCENARIO #1.0 it all works and data return in expected format
  objTest1 <-  base::c("WaterTemp_ucrtExpn_QFNAQM_thermistorDepth")
  rslt <- testthat::evaluate_promise(wrap.schm.map.char.gsub(obj =objTest1,
                                                            FileSchm=FileSchm,
                                                            Schm=NULL,
                                                            log = NULL) )
  
  testthat::expect_true("map" %in% base::names(rslt$result))
  testthat::expect_true("obj" %in% base::names(rslt$result))  
  
  # The terms in objTest1 that are mapped to new strings using FileSchm 
  idxsMap <- base::unlist(base::lapply(rslt$result$map$term1, function(x) base::grepl(x, objTest1)))
  trmsToSub <- rslt$result$map$term1[idxsMap]
  trmsSbbd <- rslt$result$map$term2[idxsMap]
  # And all these mapped terms should be found in the resulting object...
  findAllSubBool <- base::unique(base::unlist(lapply(trmsSbbd, function(x) base::grepl(x, rslt$result$obj))))
  testthat::expect_true(findAllSubBool)
  testthat::expect_equal(1, base::length(findAllSubBool))
  
  
  # TEST SCENARIO #2: Wrong file input
  rsltWrngFile <- testthat::evaluate_promise(try(wrap.schm.map.char.gsub(obj =objTest1,
                                                                     FileSchm="./pfs/tchain.avsc",
                                                                     Schm=NULL,
                                                                     log = NULL) ) )
  testthat::expect_true('try-error' %in% base::class(rsltWrngFile$result))
})