library(testthat)
source("R/def.read.avro.deve.R")

test_that("when more than one input is sent as an input, consider just the first one",
           {
             NameFile <- c("tests/testthat/def.read.avro.deve/prt_test.avro", "tests/testthat/def.read.avro.deve/prt_calibrated_2.avsc")
             rpt <- try(NEONprocIS.base::def.read.avro.deve(NameFile=NameFile, NameLib = "ravro.so"), silent = TRUE)
             testthat::expect_false((class(rpt)[1] == "try-error"))
             testthat::equals(length(rpt), 4 )

           })

 test_that("check data types of the reutrn list",
           {
             NameFile <- c("tests/testthat/def.read.avro.deve/prt_test.avro")
             rpt <- try(NEONprocIS.base::def.read.avro.deve(NameFile=NameFile, NameLib = "ravro.so"), silent = TRUE)
             testthat::expect_false((class(rpt)[1] == "try-error"))
             testthat::equals(length(rpt), 4 )
             #testthat::equals(class(rpt$source_id), "character" )
             testthat::equals(class(rpt$source_id), "string" )
             testthat::equals(class(rpt$readout_time), "POSIXct" )
             testthat::equals(class(rpt$resistance), "numeric" )

     })
