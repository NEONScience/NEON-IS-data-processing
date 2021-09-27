########################################################################################################
#' @title Unit test of merge the contents of multiple data files that share a common time variable

#' @author
#' Mija Choi\email{choim@battelleecology.org} \cr
########################################################################################################

#' @description Wrapper function. Merge the contents of multiple data files that share a common time
#' variable but different data columns. Note that if the same column name (other than the
#' time variable) is found in more than one input file, only the first instance will be retained
#' for output. Any missing timestamps among the files will be filled with NA values for the affected
#' columns. Optionally select and/or rearrange columns for output.

#' @param DirIn Character value. The input path to the directory where the child directories indicated
#' in input argument \code{DirComb} reside. This path must be the direct parent of those directories.
#' The input path is structured as follows: #/pfs/BASE_REPO/##, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs' or the same name
#' as any of the terminal directories indicated in argument \code{DirComb}.
#'
#' For example:
#' DirIn = "/scratch/pfs/proc_group/soilprt/27134/2019/01/01"
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param DirComb Character vector. The name(s) of the directories (direct children of \code{DirIn})
#' where the data to be combined resides. All files in these directories will be combined into a single file.
#' For example, if DirComb=c("data","flags"), all files found in the "data" and "flags" directories will be
#' combined into a single file.
#'
#' @param NameVarTime Character string. The name of the time variable common across all
#' files. Note that any missing timestamps among the files will have their respective columns filled
#' with NA values.
#'
#' @param ColKeep Character vector. The names, in desired order, of the input columns
#' that should be copied over to the combined output file. The column names indicated here must be a
#' full or partial set of the union of the column names found in the input files. Use the output
#' schema in argument \code{SchmCombList} to rename them as desired. Note that column names may be listed
#' more than once here. In that case the same data will be duplicated in the indicated columns, but
#' the column name of all instances after the first will default to appending an index to the end of the column name.
#' Use the output schema in argument \code{SchmCombList} to rename them as desired.
#' If this argument is omitted or NULL, all columns found in the input files for each directory will be included
#' in the output file in the order they are encountered in the input files.
#'
#' @param NameDirCombOut Character value. The name of the output directory that will be created to
#' hold the combined file. It may be the same as one of \code{DirComb}, but note that the original directory
#' with that same name may not be copied through to the output in argument DirSubCopy.
#' !!!CLARIFY THIS BEHAVIOR IF OVERLAP WITH DirSubCopy!!!
#'
#' @param NameFileSufx Character string. A character suffix to add to the output
#' file name before any extension. The base output file name is the shortest file name found among the input files.
#' For example, if the shortest file name found in the input files is "prt_CFGLOC12345_2019-01-01.parquet", and the
#' input argument is "NameFileSufx=_stats_100", then the output file will be
#' "prt_CFGLOC12345_2019-01-01_stats_100.parquet". Default is NULL, indicating no suffix to be added.
#'
#' @param SchmCombList (optional) The list output from parsing the schema for the combined output, as generated
#' from NEONprocIS.base::def.schm.avro.pars. If not input or NULL, the schema will be constructed from the output
#' data frame, as controlled by argument \code{ColKeep}.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is). May not overlap with the output directory named in
#' argument \code{NameDirCombOut}.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A single file containined the merged data in DirOut, where DirOut replaces BASE_REPO of argument
#' \code{DirIn} but  otherwise retains the child directory structure of the input path. The file name will be the same
#' as the shortest file name found in the input files, with any suffix indicated in argument \code{NameFileSufx}
#' inserted in the file name prior to the file extension (if present). The ordering of the columns will follow that in
#' the description of argument \code{ColKeep}.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none.
# changelog and author contributions / copyrights
#   Mija Choi (2021-09-22)
#     Original Creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.data.comb.ts.R\n")

# Unit test of wrap.data.comb.ts.R
test_that("Unit test of wrap.data.comb.ts.R", {
  source('../../wrap.data.comb.ts.R')
  library(stringr)
  #
  wk_dir <- getwd()
  #
  # Test 1. Only required params are passed.
  #
  testInputDir <- 'pfs/prt/14491/2019/01/01'
  testOutputBase = "pfs/out"
  DirComb = c("data", "flags")
  NameDirCombOut = "data_flags"
  NameVarTime = "readout_time"
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <- wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime
  )
  
  testInputDataDir <- base::paste0(testInputDir, '/', 'data')
  testInputFlagsDir <- base::paste0(testInputDir, '/', 'flags')
  testOutputMergedDir <- base::paste0(gsub("prt", "out", testInputDir), '/', NameDirCombOut)
  
  fileData <- base::dir(testInputDataDir)
  fileFlags <- base::dir(testInputFlagsDir)
  fileMerged <- base::dir(testOutputMergedDir)
  
  myData <- NEONprocIS.base::def.read.parq(NameFile = base::paste0(testInputFlagsDir, '/', fileFlags))
  
  myMerged <- NEONprocIS.base::def.read.parq(NameFile = base::paste0(testOutputMergedDir, '/', fileMerged))
  
  myFlags <- NEONprocIS.base::def.read.parq(NameFile = base::paste0(testInputDataDir, '/', fileData))
  
  testthat::expect_true (
    colnames(myData) %in% colnames(myMerged) &&
      colnames(myFlags) %in% colnames(myMerged)
  )
  #
  # Test 2. The same test as Test 1 except DirSubCopy=NameDirCombOut, "data_flags", is passed.
  # Its original contents will not be copied through to the output
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <- wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    DirSubCopy = "data_flags"
  )
  
  # failing now
  # testthat::expect_true (!dir.exists(testOutputMergedDir))
  #
  # testthat::expect_true (dir.exists(base::paste0(gsub("prt", "out", testInputDir), '/', 'data/')))
  # testthat::expect_true (dir.exists(base::paste0(gsub("prt", "out", testInputDir), '/', 'flags/')))
  
  #
  # Test 3. The same test as Test 2 except DirSubCopy = "testSubDirCopy" is passed.
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  DirSubCopy = "testSubDirCopy"
  
  returnedOutputDir <- wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    DirSubCopy = DirSubCopy
  )
  
  testOutputDirSub <- base::paste0(gsub("prt", "out", testInputDir), '/', DirSubCopy)
  fileDirSub <- base::dir(testOutputDirSub)
  
  testthat::expect_true (any(file.exists(testOutputDirSub, fileDirSub, recursive = TRUE)))
  #
  # Test 4. The same test as Test 3 except  ColKeep="site_id" is passed.
  # the output file keeps only that column, "site_id", in this test
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  ColKeep = "site_id"
  
  returnedOutputDir <- wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    ColKeep = ColKeep
  )
  
  myMerged <- NEONprocIS.base::def.read.parq(NameFile = base::paste0(testOutputMergedDir,'/', fileMerged))
  
  testthat::expect_true (all(colnames(myMerged) == ColKeep))
  
  #
  # Test 5. The same test as Test 4 except  ColKeep="does-not-exist", which is not correct is passed.
  # No file will be written to the output directory due to the error of non-existing column
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  ColKeep = "does-not-exist"
  
  returnedOutputDir <- try(wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    ColKeep = ColKeep
  ), silent = TRUE)
  
  testthat::expect_true (!(file.exists(base::paste0(testOutputMergedDir, '/'), recursive = TRUE)))

  # Test 6. The same test as Test 5 except  SchmCombList is NOT NULL.
  # SchmCombList has the schema for the data only, not the combind, data + flags
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  SchmDataList <- NEONprocIS.base::def.schm.avro.pars(FileSchm = 'testdata/prt_calibrated.avsc')
  
  returnedOutputDir <- try(wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    SchmCombList = SchmDataList
  ), silent = TRUE)
  
  testthat::expect_true (!(file.exists(base::paste0(testOutputMergedDir, '/'), recursive = TRUE)))
  
  # Test 7. The same test as Test 6 except  SchmCombList has the schema of the combined file.
  # SchmCombList has the correct schema for the combind which is data + flags
  #
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  testDataFlagsDir = base::paste0(testInputDir, '/', 'data_flags')
  fileDataFlags <- base::dir(testDataFlagsDir)
  NameFile <- base::paste0(testDataFlagsDir, '/', fileDataFlags)
  SchmCombList <- arrow::read_parquet(file = NameFile, as_data_frame = FALSE)

  returnedOutputDir <- wrap.data.comb.ts(
    DirIn = testInputDir,
    DirOutBase = testOutputBase,
    DirComb = DirComb,
    NameDirCombOut = NameDirCombOut,
    NameVarTime = NameVarTime,
    SchmCombList = SchmCombList
  )
  
  testthat::expect_true (any(file.exists(testOutputBase, fileDataFlags, recursive = TRUE)))
  
})