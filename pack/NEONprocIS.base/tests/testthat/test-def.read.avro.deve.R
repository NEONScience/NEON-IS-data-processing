library(testthat)
source("R/def.read.avro.deve.R")

test_that("when more than one input is sent as an input, consider just the first one",
           {
             NameFile <- c("/home/NEON/vchundru/gitRepo/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat/def.schm.avro.pars/prt_calibrated.avsc", "~/gitRepo/NEON-IS-data-processing/pack/NEONprocIS.base/tests/testthat/def.read.avro.deve/prt_calibrated_2.avsc")
             rpt <- try(NEONprocIS.base::def.read.avro.deve(NameFile=NameFile, NameLib = "~/gitRepo/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so"), silent = TRUE)
             testthat::expect_false((class(rpt)[1] == "try-error"))

           })

# test_that("when listVect length is less than 2, throw an error",
#           {#             listVect <- list(c('key1'))
#             NameList <- c('MyKeys','MyValues')
#             Type <- c('character','numeric')
#             rpt <- try(NEONprocIS.base::def.vect.pars.one.many(listVect=listVect,NameList=NameList,Type=Type), silent = TRUE)
#             testthat::expect_true((class(rpt)[1] == "try-error"))
# 
#           })
# 
# test_that("when type is not 2, throw an error",
#           {
#             listVect <- c('key1','1.5')
#             NameList <- c('MyKeys','MyValues')
#             Type <- c('character')
#             rpt <- try(NEONprocIS.base::def.vect.pars.one.many(listVect=listVect,NameList=NameList,Type=Type), silent = TRUE)
#             testthat::expect_true((class(rpt)[1] == "try-error"))
# 
#           })
# 
# 
# test_that("when NameList length is not 2, log an error but proceeds",
#            {
#               listVect <- list(c('key1', 'key2'))
#               NameList <- c('MyKeys','MyValues', 'wrongvalue')
#               Type <- c('character','numeric')
#               rpt <- try(NEONprocIS.base::def.vect.pars.one.many(listVect=listVect,NameList=NameList,Type=Type), silent = TRUE)
#               testthat::expect_true((class(rpt)[1] == "try-error"))
# 
#            })
# 
# test_that("when all the values are passed, returns values",
#           {
#               listVect <- list(c('key1','1.5','3','4','5'),c('key2','3'))
#               NameList <- c('MyKeys','MyValues')
#               Type <- c('character','numeric')
#               rpt <- NEONprocIS.base::def.vect.pars.one.many(listVect=listVect,NameList=NameList,Type=Type)
#               testthat::expect_equal(2, length(rpt))
#               testthat::expect_equal(rpt[[1]]$MyKeys, 'key1')
#               testthat::expect_equal(rpt[[1]]$MyValues[1], 1.5)
#               testthat::expect_equal(rpt[[1]]$MyValues[4], 5.0)
#               testthat::expect_equal(rpt[[2]]$MyKeys, 'key2')
#               testthat::expect_equal(rpt[[2]]$MyValues[1], 3.0)
# 
#           })