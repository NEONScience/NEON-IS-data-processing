#library(testthat)
#source("R/def.read.avro.deve.R")

# test_that("when more than one input is sent as an input, consider just the first one",
#            {
#              workingDirPath <- getwd()
#              nameFile <- file.path(workingDirPath,"def.read.avro.deve/prt_test.avro")
#              nameFile2 <- file.path(workingDirPath,"def.read.avro.deve/prt_test2.avro")
#              nameLib <- file.path(workingDirPath, "ravro.so")
#              print(nameLib)
#              rpt <- try(def.read.avro.deve(NameFile = c(nameFile, nameFile2), NameLib = nameLib), silent = FALSE)
#              testthat::expect_true((class(rpt)[1] == "try-error"))
# 
#            })
# 
#  test_that("check data types of the reutrn list",
#            {
#              workingDirPath <- getwd()
#              nameFile <- file.path(workingDirPath,"def.read.avro.deve/prt_test.avro")
#              nameLib <- file.path(workingDirPath, "ravro.so")
#              rpt <- try(def.read.avro.deve(NameFile = nameFile, NameLib = nameLib), silent = FALSE)
#              testthat::expect_true((class(rpt)[1] == "try-error"))
#              testthat::equals(length(rpt), 4 )
#              testthat::equals(class(rpt$source_id), "character" )
#              testthat::equals(class(rpt$site_id), "character" )
#              testthat::equals(class(rpt$readout_time), "POSIXct" )
#              testthat::equals(class(rpt$resistance), "numeric" )
# 
#      })
