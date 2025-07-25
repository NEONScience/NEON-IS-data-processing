##############################################################################################
#' @title Unit test of def.schm.parq.from.schm.avro.R, Create Parquet schema from Avro schema

#' @description
#' Definition function. Create a arrow schema object from an avro schema file or json string.

#' @param FileSchm String. Optional. Full or relative path to schema file. One of FileSchm or Schm must be
#' provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. One of FileSchm or Schm must
#' be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created for use within this function.
#'
#' @return An Apache Arrow Schema object containing a parquet schema representation of the avro schema.
#
# changelog and author contributions / copyrights
#   Mija Choi (2022-05-01)
#     original creation
#
# Define test context
#context("\n       | Unit test of create Parquet schema from Avro schema\n")
test_that("",
          {
            #1. No params passed in
            rpt <- try(NEONprocIS.base::def.schm.parq.from.schm.avro(), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
            
            #2.
            workingDirPath <- getwd()
            fileSchm <- file.path(workingDirPath, "testdata/HART_data.avsc")
            rpt <- NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm = fileSchm)
            
            testthat::expect_true(class(rpt)[1] == "Schema")
            testthat::expect_true(class(rpt)[2] == "ArrowObject")
            testthat::expect_true(class(rpt)[3] == "R6")
            
            #3.
            workingDirPath <- getwd()
            schm <- file.path(workingDirPath, "testdata/avro-schema.avsc")
            rpt <- NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm = schm)
            
            #4.  dataType = double is only allowed in arrow V11,  not in V9.0.0.
            
            workingDirPath <- getwd()
            schm <- file.path(workingDirPath, "testdata/avro-schema-double.avsc")
            rpt <- try(NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm = schm),silent = TRUE)
  
          })
