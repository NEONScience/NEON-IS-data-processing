#library(testthat)
#source("R/def.read.evnt.json.R")

# test_that("when json file is not sent as an input, throw an error",
#            {
#              NameFile <- c("tests/testthat/def.read.avro.deve/prt_test.avro")
#              rpt <- try(NEONprocIS.base::def.read.evnt.json(NameFile=NameFile), silent = TRUE)
#              testthat::expect_true((class(rpt)[1] == "try-error"))
# 
#            })

 # test_that("check valid data returned in a dataframe",
 #           {
 #             NameFile <- c("tests/testthat/def.read.evnt.json/ucrt-coef-fdas-input.json")
 #             rpt <- try(NEONprocIS.base::def.read.evnt.json(NameFile=NameFile), silent = TRUE)
 #             testthat::expect_false((class(rpt)[1] == "try-error"))
 #             testthat::equals(length(rpt), 4 )
 #             #testthat::equals(class(rpt$source_id), "character" )
 #             testthat::equals(class(rpt$source_id), "string" )
 #             testthat::equals(class(rpt$readout_time), "POSIXct" )
 #             testthat::equals(class(rpt$resistance), "numeric" )
 # 
 #     })
