##############################################################################################
#' @title Unit test of def.schm.avro.pars.R, parse AVRO schema to list

#' @description
#' Definition function. Turn the JSON formatted AVRO schema (either from file or a json string)
#' into a formatted list.

#' @param FileSchm String. Optional. Full or relative path to schema file. One of FileSchm or Schm must be
#' provided.
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. One of FileSchm or Schm must
#' be provided. If both Schm and FileSchm are provided, Schm will be ignored.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created for use within this function.

#' @return A list of:\cr
#' \code{schmJson}: the avro schema in json format\cr
#' \code{schmList}: a list of avro schema properties and fields
#' \code{var}: a data frame of variables/fields in the schema, their data type(s), and documentation string

#
# changelog and author contributions / copyrights
#     revised the test after original test was written
#   Mija Choi (2022-04-07)
#     Added the comments in the beginning and made minor changes in the original test
test_that("when fileSchm and schm are both null, throw an exceptionr",
          {
            rpt <- try(NEONprocIS.base::def.schm.avro.pars(FileSchm = NULL, Schm = NULL), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
            
          })

test_that("when avro schm is pass, return all the elements in the schema",
          {
            workingDirPath <- getwd()
            FileSchm <- file.path(workingDirPath, "testdata/HART_data.avsc")
            
            rpt <- NEONprocIS.base::def.schm.avro.pars(FileSchm = FileSchm)
            testthat::expect_true(is.list(rpt))
            testthat::expect_true(length(rpt$schmJson) == 1)
            testthat::expect_true(length(rpt$schmList) == 4)
            testthat::expect_true(length(rpt$var) == 3)
            testthat::expect_true(rpt$var[1]$name[1] == 'source_id')
            testthat::expect_true(rpt$var[2]$type[3] == "long|timestamp-millis")
            testthat::expect_true(typeof(rpt$var[3]$doc[3]) == "character")
          })

test_that("when a non-schema file is passed in",
          {
            workingDirPath <- getwd()
            FileSchm <- file.path(workingDirPath, "testdata/HART_data.csv")
            rpt <- try(suppressWarnings(NEONprocIS.base::def.schm.avro.pars(FileSchm = FileSchm)),
                  silent = TRUE)
            testthat::expect_equal(class(rpt),'try-error')
            
          })

test_that("when a name = NULL is passed in",
          {
            workingDirPath <- getwd()
            FileSchm <- file.path(workingDirPath, "testdata/HART_data_NULL.avsc")
            rpt <- NEONprocIS.base::def.schm.avro.pars(FileSchm = FileSchm)
            testthat::expect_equal(rpt$var$name[3],as.character(NA))
            
          })
