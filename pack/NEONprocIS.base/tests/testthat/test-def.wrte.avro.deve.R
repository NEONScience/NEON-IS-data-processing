#library(testthat)
#source("R/def.wrte.avro.deve.R")

test_that("When only required parameters are sent to the def.wrte.avro.deve function ",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            workingDirPath <- getwd()
            NameFile <- file.path(workingDirPath,"out.avro")
            schm <- file.path(workingDirPath,"def.wrte.parq/prt_calibrated.avsc")
            outputData <- NEONprocIS.base::def.wrte.avro.deve(data=data, NameFile = NameFile, NameLib='ravro.so')
            expect_true (file.exists(NameFile))
            if (file.exists(NameFile)) { file.remove(NameFile)}

          })

# test_that("When schema param is passed along with required parameters to the def.wrte.avro.deve function ",
#           {
#             data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#             workingDirPath <- getwd()
#             NameFile <- file.path(workingDirPath,"out.avro")
#             Schm <- file.path(workingDirPath,"def.schm.avro.pars/prt_calibrated.avsc")
#             outputData <- NEONprocIS.base::def.wrte.avro.deve(data=base::data.frame(), NameFile = NameFile, Schm = Schm, NameLib='ravro.so')
#             expect_true (file.exists(NameFile))
#             if (file.exists(NameFile)) { file.remove(NameFile)}
# 
#           })