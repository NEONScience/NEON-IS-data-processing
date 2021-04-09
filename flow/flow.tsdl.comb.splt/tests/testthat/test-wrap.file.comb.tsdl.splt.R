#' @title Unit test for def.file.comb.tsdl.splt called in flow.tsdl.comb.splt
#' @author Guy Litt
#' @description 
#' @seealso flow.tsdl.comb.splt




# unit test Flow scripts
# changelog and author contributions / copyrights
#   Guy Litt (2021-03-31)
#     Original Creation
##############################################################################################
# Define test context
library(testthat)
setwd("./tests/testthat/")

testthat::context("\n  Unit test of def.file.comb.tsdl.ong.R\n")


# TODO consider calling a pachyderm data pull/refresh to detect format change?

# Unit test of wrap.loc.repo.strc.R
test_that("Unit test of wrap.loc.repo.strc.R", {
  source('../../def.file.comb.tsdl.splt.R')
  
  wk_dir <- getwd()
  
  # ----------------------------------------------------------------
  # TEST SCENARIO #1 it all works and data return in expected format
  Para <- base::list()
  Para$LocDir <- "location"
  Para$StatDir <- "stats"
  Para$QmDir <- "quality_metrics"
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
  
  rsltGoodEval <- testthat::evaluate_promise(def.file.comb.tsdl.splt(file = filePath,
                          nameVarTime = nameVarTime, 
                          mrgeCols = c("startDateTime", "endDateTime"),
                          locDir = "location",
                          statDir = "stats",
                          qmDir = "quality_metrics",
                          corrColNams = TRUE,
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
  
  badTimeEval <- testthat::evaluate_promise(try(def.file.comb.tsdl.splt(file = filePath,
                             nameVarTime = nameVarTimeBad, 
                             mrgeCols = c("startDateTime", "endDateTime"),
                             locDir = "location",
                             statDir = "stats",
                             qmDir = "quality_metrics",
                             corrColNams = TRUE,
                             log = NULL)))
  
  badTime <- badTimeEval$result

  testthat::expect_true(base::class(badTime) == "try-error")
  # An ERROR log should have generated
  testthat::expect_true(base::grepl("ERROR",badTimeEval$output))
  
  # --------- Test a bad file path:
  filePathBad <- file
  badFilePathEval <- testthat::evaluate_promise(try(def.file.comb.tsdl.splt(file = filePathBad,
                                             nameVarTime = nameVarTime, 
                                             mrgeCols = c("startDateTime", "endDateTime"),
                                             locDir = "location",
                                             statDir = "stats",
                                             qmDir = "quality_metrics",
                                             corrColNams = TRUE,
                                             log = NULL)))
  
  badFilePath <- badFilePathEval$result
  
  # The result should be a try-error:
  testthat::expect_true(class(badFilePath) == "try-error")
  # An ERROR log should have generated:
  testthat::expect_true(base::grepl("ERROR",badFilePathEval$output))
  
  
  # ---------- Test an incorrect location directory
  badLocEval <- testthat::evaluate_promise(try(def.file.comb.tsdl.splt(file = filePathBad,
                                          nameVarTime = nameVarTime, 
                                          mrgeCols = c("startDateTime", "endDateTime"),
                                          locDir = "location1",
                                          statDir = "stats",
                                          qmDir = "quality_metrics",
                                          corrColNams = TRUE,
                                          log = NULL)) )
  
  badLoc <- badLocEval$result
  # The result should be a try-error:
  testthat::expect_true(class(badLoc) == "try-error")
  
  # An ERROR log should have generated:
  testthat::expect_true(base::grepl("ERROR",badLocEval$output))
  

  # ------------------ Test a single merge column
  # Merging R dataframes with non-unique column names can yield a ".x" and a ".y" in the filename
  rsltSnglColEval <- testthat::evaluate_promise(def.file.comb.tsdl.splt(file = filePath,
                                                                     nameVarTime = nameVarTime, 
                                                                     mrgeCols = c("endDateTime"),
                                                                     locDir = "location",
                                                                     statDir = "stats",
                                                                     qmDir = "quality_metrics",
                                                                     corrColNams = TRUE,
                                                                     log = NULL))
  rsltSnglCol <- rsltGoodEval$result
  
  # Ensure no duplicate column names generated
  chckMrgeName <- lapply(1:length(rsltSnglCol), function(i)  lapply(1:length(rsltSnglCol[[i]]), function(j) grep("\\.x", names(rsltSnglCol[[i]][[j]])) )   )
  testthat::expect_equal(0, length(unlist(chckMrgeName) ) )
  
  
  
  # ------------------------------------------
  
  
  
  # ===============================================================================================================================================
  # Test scenario 1::
  # if Comb = FALSE, default when Comb is not passed in to wrap.loc.repo.strc,
  # then pfs/prt/2019/01/01/3119 copied to pfs/out/2019/01/01/CFGLOC100241/3119/location/
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir)
  testOutputDirnamedLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/", sourceId, "/location")
  expect_true (dir.exists(testOutputDirnamedLoc))
  
  # clean out the test output dirs and file recursively
  if (dir.exists(testOutputDir)) {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 2::
  # if Comb = TRUE then 3119 is replaced with the NAME, CFGLOC100241.
  # pfs/prt/2019/01/01/3119/location/ copied to pfs/out/2019/01/01/CFGLOC100241/location/
  #                   =====                                       ==============
  
  wrap.loc.repo.strc(DirIn = testInputDir,DirOutBase = testOutputDir,Comb = TRUE)
  
  testOutputDirSourceIdLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/location")
  expect_true (dir.exists(testOutputDirSourceIdLoc))
  # clean out the test output dirs and file recursively
  #
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
  
  # Test scenario 3::
  # If there is no location file, skip
  #  testInputDir = "C:/projects/NEON-IS-data-processing/flow/flow.loc.repo.strc.comb/flow.loc.repo.strc/tests/testthat/pfs/prt_noFiles/2019/01/01/3119"
  
  testInputDir <- base::paste0(wk_dir, '/', 'pfs/prt_noFiles/2019/01/01/3119')
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  
  expect_true (!dir.exists(testOutputDir))
  
  # Test scenario 4::
  # If there is more than one location file, use the first
  testInputDir <-
    base::paste0(wk_dir, '/', 'pfs/prt_moreThanOneFile/2019/01/01/3119')
  dirInLoc <- base::paste0(testInputDir, '/location')
  fileLoc <- base::dir(dirInLoc)
  # numFileLoc <- base::length(fileLoc)
  
  # Load in the location json and get the location name to verify the test
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc[1]))
  nameLoc <- loc$name
  sourceId <- loc$source_id
  
  testOutputDir = "pfs/out"
  testOutputDirPath <- base::paste0(testOutputDir, "/", installdate, collapse = '/')
  
  wrap.loc.repo.strc(DirIn = testInputDir, DirOutBase = testOutputDir, Comb = TRUE)
  testOutputDirSourceIdLoc <- base::paste0(testOutputDirPath, "/", nameLoc, "/location")
  expect_true (dir.exists(testOutputDirSourceIdLoc))
  
  if (dir.exists(testOutputDir))  {
    unlink(testOutputDir, recursive = TRUE)
  }
})




test_that("wrong file format",
          {
            file <- c('def.file.comb.ts/validFiles/testdata.txt', 'def.file.comb.ts/validFiles/testflagsdata.parquet')
            
            returnedData <- try(def.file.comb.ts(file = file, nameVarTime='readout_time'),
                                silent = TRUE)
            
            testthat::expect_true((class(returnedData)[1] == "try-error"))
            
          })

test_that("duplicate columns in the input files",
          {
            file <- c('def.file.comb.ts/invalidFiles/testdata.parquet', 'def.file.comb.ts/invalidFiles/testflagsdatadup.parquet')
            
            returnedData <- def.file.comb.ts(file = file, nameVarTime='readout_time')
            
            expect_true (length(returnedData) == 6)
            testthat::expect_true(is.list(returnedData))
            if (!(length(returnedData) == 0)) {
              testthat::expect_true (returnedData$source_id[2] == '16247')
              testthat::equals (returnedData$temp[5], 0.007209014)
              testthat::equals (returnedData$validCalQF[1], 0)
              #testthat::expect_null(returnedData$time[1])
            }
            
          })




# ########################################################################################################################
# ########################################################################################################################
# ########################################################################################################################
test_that("valid files to merge",
          {
            file <- c('def.file.comb.ts/validFiles/testdata.parquet', 'def.file.comb.ts/validFiles/testflagsdata.parquet')
           
            returnedData <- def.file.comb.ts(file = file, nameVarTime='readout_time')
            
            expect_true (length(returnedData) == 6)
            testthat::expect_true(is.list(returnedData))
            if (!(length(returnedData) == 0)) {
              testthat::expect_true (returnedData$source_id[2] == '16247')
              testthat::equals (returnedData$temp[5], 0.007209014)
              testthat::equals (returnedData$validCalQF[1], 0)
              #testthat::expect_null(returnedData$time[1])
            }
            
          })

test_that("wrong file format",
          {
            file <- c('def.file.comb.ts/validFiles/testdata.txt', 'def.file.comb.ts/validFiles/testflagsdata.parquet')
            
            returnedData <- try(def.file.comb.ts(file = file, nameVarTime='readout_time'),
                                silent = TRUE)
            
            testthat::expect_true((class(returnedData)[1] == "try-error"))
            
          })

test_that("duplicate columns in the input files",
          {
            file <- c('def.file.comb.ts/invalidFiles/testdata.parquet', 'def.file.comb.ts/invalidFiles/testflagsdatadup.parquet')
            
            returnedData <- def.file.comb.ts(file = file, nameVarTime='readout_time')
            
            expect_true (length(returnedData) == 6)
            testthat::expect_true(is.list(returnedData))
            if (!(length(returnedData) == 0)) {
              testthat::expect_true (returnedData$source_id[2] == '16247')
              testthat::equals (returnedData$temp[5], 0.007209014)
              testthat::equals (returnedData$validCalQF[1], 0)
              #testthat::expect_null(returnedData$time[1])
            }
            
          })



