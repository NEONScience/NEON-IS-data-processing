#library(testthat)
#source("R/def.schm.avro.pars.R")

test_that("when fileSchm and schm are both null, throw an exceptionr",
          {
            rpt <- try(NEONprocIS.base::def.schm.avro.pars(FileSchm = NULL, Schm = NULL), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))

          })

test_that("when avro schm is pass, return all the elements in the schema",
          {
            FileSchm <- "def.schm.avro.pars/prt_calibrated.avsc"
            rpt <- NEONprocIS.base::def.schm.avro.pars(FileSchm = FileSchm)
#            testthat::expect_false((class(rpt)[1] == "try-error"))
            testthat::expect_true(is.list(rpt))
            expect_true (length(rpt$schmJson) == 1)
            expect_true (length(rpt$schmList) == 5)
            expect_true (length(rpt$var) == 3)
            expect_true (rpt$var[1]$name[1] == 'source_id')
            expect_true (rpt$var[2]$type[3] == "long|timestamp-millis")
            expect_true (typeof(rpt$var[3]$doc[3]) == "character")
          })

