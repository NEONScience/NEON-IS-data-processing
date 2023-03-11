##############################################################################################
#' @title Combine kafka output and strip unneeded data columns

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Combined multiple L0 data files retrieved from kafka for a single
#' data day, strip unnecessary columns (so the data matches the L0 schema from Engineering), and 
#' remove the kafka offsets from the file name.
#' 
#' Data retrieved from Kafka may result in multiple files for a single data day depending on when the
#' data streamed to NEON HQ from the site. The file names include the start/stop kafka offsets to 
#' make them unique. Files retrieved from Kafka also include data that is not specified in the L0 
#' schema and thus potentially breaks workflows dependent on the L0 schema.
#' This module combines all kafka-retrieved files of kafka-related information so that the file 
#' name and format matches that of data generated from other sources (i.e. Trino).

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path is the folder:
#'         /data
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_SOURCEID_YYYY-MM-DD_KAFKAOFFSETBEGIN_KAFKAOFFSETEND.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/li191r_data_source_kafka/li191r/2023/03/01/11346/data/ with nested file:
#'    li191r_11346_2023-03-05_13275082_13534222.parquet
#'    li191r_11346_2023-03-05_13534225_13534273.parquet
#'
#' @param FileSchmL0 String. Optional. Full or relative path to L0 schema file. One of FileSchmL0 or SchmL0 must be 
#' provided.
#' @param SchmL0 String. Optional. Json formatted string of the AVRO L0 file schema. One of FileSchmL0 or SchmL0 must 
#' be provided. If both SchmL0 and FileSchmL0 are provided, SchmL0 will be ignored.
#' 
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the merged and filtered Kafka output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn <- '/scratch/pfs/li191r_data_source_kafka/li191r/2023/03/02/27733'
#' DirOutBase <- '/scratch/pfs/out'
#' FileSchmL0 <- '~/R/avro_schemas/schemas/li191r/li191r.avsc' # L0 schema
#' wrap.kfka.comb(DirIn,DirOutBase,FileSchmL0)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-03-07)
#     Initial creation
##############################################################################################
# Define test context
context("\n                       Unit test of wrap.kfka.comb.R\n")

# Unit test of wrap.kfka.comb.R
test_that("Unit test of wrap.kfka.comb.R", {
  
  source('../../flow.kfka.comb/wrap.kfka.comb.R')
  library(stringr)
  #
  testOutputBase = "pfs/out"
  #
  # Test 1. Failure: No L0 schema passed in
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
    }
  
  testInputDir <- 'pfs/li191r_data_source_kafka/li191r/2023/03/03/10246'
  
  returnedOutputDir <-try(wrap.kfka.comb(DirIn = testInputDir, DirOutBase = testOutputBase),silent = FALSE)
  
  testthat::expect_true((class(returnedOutputDir)[1] == "try-error"))
  
  
  #
  # Test 2. Success: Set of kafka-generated files passed in and combined to the output.
  
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  FileSchmL0 <- 'pfs/li191r_data_source_kafka/li191r.avsc'
  testOutputDir <- gsub("li191r_data_source_kafka", "out", testInputDir)
  testOutputFile <- 'li191r_10246_2023-03-03.parquet'
  returnedOutputDir <- wrap.kfka.comb(DirIn = testInputDir, 
                                      DirOutBase = testOutputBase,
                                      FileSchmL0=FileSchmL0)
  dirOutTest <- dir(fs::path(testOutputDir))
  testthat::expect_true (length(dirOutTest) == 1 && dirOutTest=='data')
  fileOutTest <- fs::path(testOutputDir,'data',testOutputFile)
  testthat::expect_true (file.exists(fileOutTest))
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fileOutTest)
  testthat::expect_true(nrow(dataChk)==28)
  testthat::expect_true(ncol(dataChk)==4)
  testthat::expect_true(all(diff(dataChk$readout_time) > 0)) # Test for sorted time
  
  
  # Test 3. Success: Pass-through directory copied to output
  if (dir.exists(testOutputBase)) {
    unlink(testOutputBase, recursive = TRUE)
  }
  
  returnedOutputDir <- wrap.kfka.comb(DirIn = testInputDir, 
                                      DirOutBase = testOutputBase,
                                      FileSchmL0=FileSchmL0,
                                      DirSubCopy='copyDir')
  testCopyFile <- list.files(fs::path(testInputDir,'copyDir'))
  fileCopyTest <- list.files(fs::path(testOutputDir,'copyDir'))
  testthat::expect_true(all(testCopyFile %in% fileCopyTest) && all(fileCopyTest %in% testCopyFile))
  
  
  
})
