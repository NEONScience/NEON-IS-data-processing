#' @title Unit test for def.schm.avro.pars.map.R called in wrap.file.comb.tsdl.splt
#' @author Guy Litt
#' @seealso flow.tsdl.comb.splt

# changelog and author contributions / copyrights
#   Guy Litt (2021-04-09)
#     Original Creation
#   Guy Litt (2021-05-20)
#     Added file does not exist test logic
##############################################################################################
# Define test context
library(testthat)
# setwd("~/R/NEON-IS-data-processing-glitt/flow/flow.tsdl.comb.splt/")
setwd("./tests/testthat/")

testthat::context("\n  Unit test of def.schm.avro.pars.map.R\n")


test_that("Unit test of def.schm.avro.pars.map.R", {
  source('../../def.schm.avro.pars.map.R')
  
  Para <- base::list()
  Para$FileSchmMapDepth <- "./pfs/schemas/tsdl_map_loc_names.avsc"
  Para$FileSchmMapCols <-  "./pfs/schemas/tsdl_col_term_subs.avsc"
  Para$FileSchmNotMap <- "./pfs/schemas/tchain.avsc"
  
  
  # ----------------------------------------------------------------
  # TEST SCENARIO #1 it all works and data return in expected format
  # ----------------------------------------------------------------
  rsltGoodEval <- testthat::evaluate_promise(def.schm.avro.pars.map(FileSchm=Para$FileSchmMapDepth,
                          Schm=NULL,
                          log=NULL)
                          )
  rsltGood <- rsltGoodEval$result
  
  # No warnings should have been generated:
  testthat::expect_equal(0, base::length(rsltGoodEval$warnings))
  
  # Log INFO should have been provided
  testthat::expect_true(base::grepl("", rsltGoodEval$output))
  
  # The results should be list with objects schmJson, schmList, and map (for writing output file name)
  testthat::expect_identical(base::names(rsltGood), c("schmJson","schmList","map"))
  
  # The mapping should contain expected column names:
  testthat::expect_identical(base::names(rsltGood$map), c("term1","term2","desc","type"))
  
  # ---------------------------------------------------------------
  # TEST SCENARIO #2, INPUT FAILURE
  # ---------------------------------------------------------------
  # A file that is not a mapping avro schema
  rsltBadInpt <- testthat::evaluate_promise(try(def.schm.avro.pars.map(FileSchm=Para$FileSchmNotMap,
                                                                   Schm=NULL,
                                                                   log=NULL)
                                                                   ))
  
  testthat::expect_identical("try-error", base::class(rsltBadInpt$result))
  testthat::expect_true(base::grepl("format", rsltBadInpt$output))
  
  # A file that doesn't exist:
  rsltNonExst <- testthat::evaluate_promise(try(def.schm.avro.pars.map(FileSchm="./tchain.avsc",
                                                                       Schm=NULL,
                                                                       log=NULL)
                                                ))
  testthat::expect_true(base::grepl("FATAL", rsltNonExst$output) )
  testthat::expect_true(base::grepl("does not exist", rsltNonExst$output) )
  
})
