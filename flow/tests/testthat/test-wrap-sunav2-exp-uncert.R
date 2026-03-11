##############################################################################################
#' @title Unit test for Wrapper for SUNA Expanded Uncertainty Calculations
#'
#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org} \cr
#' 
#' @description Wrapper function unit test. Calculates expanded uncertainty for SUNA v2 data bursts.
#' 
#' @param DirIn Character value. The base file path to the averaged stats and uncertainty coefficients.
#' 
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @seealso Currently none
#'
# changelog and author contributions
#   ncatolico (2026-02-09)
#     Original Creation
##############################################################################################

context("\n                       Unit test of wrap.sunav2.exp.uncert.R\n")

test_that("Unit test of wrap.sunav2.exp.uncert.R", {
  # Source the target script
  source('../../flow.sunav2.exp.uncert/wrap.sunav2.exp.uncert.R')
  library(stringr)
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  workingDirPath <- getwd()
  testDirOut = file.path(workingDirPath, 'pfs/out_suna')
  
  # Construct dummy input and output directories (should point to existing test data for actual runs)
  DirIn <- file.path(workingDirPath, 'pfs/nitrate_null_gap_ucrt_group/2019/11/18/nitrate-surfacewater_POSE102100/sunav2/CFGLOC101686')
  DirOutBase <- file.path(workingDirPath, 'pfs/nitrate_out')
  
  # Avro schema (as in examples, adapt as needed)
  testAvroDir <- file.path(workingDirPath, 'pfs/sunav2_avro_schemas/nitrate/nitrate_ucrt.avsc')
  SchmStats <- base::paste0(base::readLines(testAvroDir), collapse = '')
  
  # Subfolders to copy (optional, use dummy/test folders if available)
  DirSubCopy <- c("quality metrics", "location")   # Example; adjust per test data
  
  # Determine output location
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  testDirRepo <- InfoDirIn$dirRepo
  testDirOutPath <- base::paste0(DirOutBase, testDirRepo, "/stats")
  
  # Clean output before test
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # Run wrapper
  wrap.sunav2.exp.uncert(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    SchmStats = SchmStats,
    DirSubCopy = DirSubCopy,
    log = log
  )
  
  # Check the stats file was written to the destination
  testthat::expect_true(length(list.files(testDirOutPath, pattern = "\\.parquet$")) > 0)
  
  # Additional: check that non-numeric or NaN means are set as NA in output file (read file and check if possible)
  statsFiles <- list.files(testDirOutPath, full.names = TRUE, pattern = "\\.parquet$")
  if (length(statsFiles) > 0) {
    statsData <- NEONprocIS.base::def.read.parq(NameFile = statsFiles[1])
    testthat::expect_true(all(is.na(statsData$surfWaterNitrateMean[is.nan(statsData$surfWaterNitrateMean)])))
  }
  
  #delete output directory
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
})
