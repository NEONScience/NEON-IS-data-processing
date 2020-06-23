library(testthat)
source("R/def.schm.avro.pars.R")

test_that("when length of vector is not multiple of 2, throw an error",
          {
            FileSchm <- "tests/testthat/def.wrte.parq/prt_calibrated.avsc"
            rpt <- try(NEONprocIS.base::def.schm.avro.pars(FileSchm = FileSchm), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))

          })

