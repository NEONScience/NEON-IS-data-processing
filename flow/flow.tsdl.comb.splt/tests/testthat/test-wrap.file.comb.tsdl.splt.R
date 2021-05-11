#' @title Unit test for wrap.file.comb.tsdl.splt called in flow.tsdl.comb.splt
#' @author Guy Litt
#' @description Unit tests for wrap.file.comb.tsdl.splt
#' @seealso flow.tsdl.comb.splt

# unit test Flow scripts
# changelog and author contributions / copyrights
#   Guy Litt (2021-03-31)
#     Original Creation
##############################################################################################
# Define test context
library(testthat)
# setwd("~/R/NEON-IS-data-processing-glitt/flow/flow.tsdl.comb.splt/")
setwd("./tests/testthat/")

testthat::context("\n  Unit test of wrap.file.comb.tsdl.splt.R\n")


# TODO consider calling a pachyderm data pull/refresh to detect format change?

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.file.comb.tsdl.splt.R", {
  source('../../wrap.file.comb.tsdl.splt.R')
  source('../../def.schm.avro.pars.map.R')
  source('../../wrap.schm.map.char.gsub.R')
  source('../../def.map.char.gsub.R')
  source("../../def.find.mtch.str.best.R")
  wk_dir <- getwd()
  
  # ----------------------------------------------------------------
  # TEST SCENARIO #1 it all works and data return in expected format
  Para <- base::list()
  Para$LocDir <- "location"
  Para$StatDir <- "stats"
  Para$QmDir <- "quality_metrics"
  Para$FileSchmMapDepth <- base::paste0(wk_dir,"/pfs/schemas/tsdl_map_loc_names.avsc")
  Para$FileSchmMapCols <-  base::paste0(wk_dir,"/pfs/schemas/tsdl_col_term_subs.avsc")
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/tempSpecificDepthLakes_level1_group/tchain/2019/01/10/CFGLOC110702/')
  nameVarTime <- c("001","030")
  dirInLoc <- base::paste0(testInputDir, '/', Para$LocDir)
  fileLoc <- base::dir(dirInLoc)
  # Combine the location, stats, and quality_metrics directory
  Para$DirComb <- base::c(Para$LocDir,Para$StatDir,Para$QmDir)
  
  
  DirIn <-
    NEONprocIS.base::def.dir.in(DirBgn = testInputDir,
                                nameDirSub =  Para$DirComb)
 
  file <-
    base::list.files(base::paste0(DirIn, '/',Para$DirComb))
  filePath <-
    base::list.files(base::paste0(DirIn, '/', Para$DirComb), full.names =
                       TRUE)
  
  rsltGoodEval <- testthat::evaluate_promise(wrap.file.comb.tsdl.splt(filePths = filePath,
                          nameVarTime = nameVarTime, 
                          mrgeCols = c("startDateTime", "endDateTime"),
                          locDir = "location",
                          statDir = "stats",
                          qmDir = "quality_metrics",
                          nameSchmMapDpth = Para$FileSchmMapDepth,
                          nameSchmMapCols = Para$FileSchmMapCols,
                          log = NULL))
  rsltGood <- rsltGoodEval$result
  

  # No warnings should have been generated:
  testthat::expect_equal(0, base::length(rsltGoodEval$warnings))
  
  # Log INFO should have been provided
  testthat::expect_true(base::grepl("INFO", rsltGoodEval$output))
  
  # The results should be list with nameVarTime (for writing output file name)
  testthat::expect_identical(base::names(rsltGood), nameVarTime)
  
  # Check specific case to ensure expected HOR.VER naming
  if(base::grepl("CFGLOC110702", testInputDir)){
    dfNamz <- (base::lapply(base::names(rsltGood), function(t) base::names(rsltGood[[t]])))
    for(varTimeIdx in 1:base::length(dfNamz)){
      testthat::expect_identical(dfNamz[[varTimeIdx]],c("103.501", "103.502", "103.503", "103.504", "103.505", "103.506", "103.507"))
    }
  }
  
  # The last dataframe corresponding to the last HOR.VER should contain data (non-empty)
  testthat::expect_gt(base::nrow(rsltGood[[1]][[base::length(rsltGood)]]), 1) # otherwise problem with excluding dataframes without a HOR.VER
  
  # All nameVarTimes should have the same # of HOR.VER locs
  testthat::expect_equal(1, base::length(base::unique(base::unlist(lapply(names(rsltGood), function(i) length(rsltGood[[i]]))))))
  
  # The non-instantaneous time (030) should have more data cols than the instantaneous data
  testthat::expect_true(base::ncol(rsltGood[["030"]][[1]]) > base::ncol(rsltGood[["001"]][[1]]))
  
  
  # The 030 QM cols should not contain the 'QF' when corrColNams=TRUE
  cols30 <- base::colnames(rsltGood[["030"]][[1]])
  colsQM <- cols30[base::grep("QM", cols30)]
  testthat::expect_false(base::any(base::grepl("QF",colsQM)))
  
  # Data columns should be named 'tsdWaterTemp'
  colsWat30 <- cols30[base::grep("WaterTemp", cols30)]
  testthat::expect_false(base::any(!base::grepl("tsdWaterTemp", colsWat30)))
  
  # --------------------------------------------
  # TEST SCENARIO #2, INPUT FAILURE
  # --------------------------------------------
  
  # ---------- Test an incorrect time interval
  nameVarTimeBad <- c("002","030")
  
  badTimeEval <- testthat::evaluate_promise(try(wrap.file.comb.tsdl.splt(file = filePath,
                             nameVarTime = nameVarTimeBad, 
                             mrgeCols = c("startDateTime", "endDateTime"),
                             locDir = "location",
                             statDir = "stats",
                             qmDir = "quality_metrics",
                             nameSchmMapDpth = Para$FileSchmMapDepth,
                             nameSchmMapCols = Para$FileSchmMapCols,
                             log = NULL)))
  
  badTime <- badTimeEval$result

  testthat::expect_true(base::class(badTime) == "try-error")
  # An ERROR log should have generated
  testthat::expect_true(base::grepl("ERROR",badTimeEval$output))
  
  # --------- Test a bad file path:
  filePathBad <- file
  badFilePathEval <- testthat::evaluate_promise(try(wrap.file.comb.tsdl.splt(file = filePathBad,
                                             nameVarTime = nameVarTime, 
                                             mrgeCols = c("startDateTime", "endDateTime"),
                                             locDir = "location",
                                             statDir = "stats",
                                             qmDir = "quality_metrics",
                                             nameSchmMapDpth = Para$FileSchmMapDepth,
                                             nameSchmMapCols = Para$FileSchmMapCols,
                                             log = NULL)))
  
  badFilePath <- badFilePathEval$result
  
  # The result should be a try-error:
  testthat::expect_true(class(badFilePath) == "try-error")
  # An ERROR log should have generated:
  testthat::expect_true(base::grepl("ERROR",badFilePathEval$output))
  
  
  # ---------- Test an incorrect location directory
  badLocEval <- testthat::evaluate_promise(try(wrap.file.comb.tsdl.splt(file = filePathBad,
                                          nameVarTime = nameVarTime, 
                                          mrgeCols = c("startDateTime", "endDateTime"),
                                          locDir = "location1",
                                          statDir = "stats",
                                          qmDir = "quality_metrics",
                                          nameSchmMapDpth = Para$FileSchmMapDepth,
                                          nameSchmMapCols = Para$FileSchmMapCols,
                                          log = NULL)) )
  
  badLoc <- badLocEval$result
  # The result should be a try-error:
  testthat::expect_true(class(badLoc) == "try-error")
  
  # An ERROR log should have generated:
  testthat::expect_true(base::grepl("ERROR",badLocEval$output))
  

  # ------------------ Test a single merge column
  # Merging R dataframes with non-unique column names can yield a ".x" and a ".y" in the filename
  rsltSnglColEval <- testthat::evaluate_promise(wrap.file.comb.tsdl.splt(file = filePath,
                                                                     nameVarTime = nameVarTime, 
                                                                     mrgeCols = c("endDateTime"),
                                                                     locDir = "location",
                                                                     statDir = "stats",
                                                                     qmDir = "quality_metrics",
                                                                     nameSchmMapDpth = Para$FileSchmMapDepth,
                                                                     nameSchmMapCols = Para$FileSchmMapCols,
                                                                     log = NULL))
  rsltSnglCol <- rsltGoodEval$result
  
  # Ensure no duplicate column names generated
  chckMrgeName <- base::lapply(1:base::length(rsltSnglCol), function(i) 
    base::lapply(1:base::length(rsltSnglCol[[i]]), function(j) base::grep("\\.x", base::names(rsltSnglCol[[i]][[j]])) )   )
  testthat::expect_equal(0, base::length(base::unlist(chckMrgeName) ) )

  
})
