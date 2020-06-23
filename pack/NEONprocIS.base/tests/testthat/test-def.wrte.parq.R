library(testthat)
source("R/def.wrte.parq.R")

#data,
#NameFile,
#Schm=NULL,
#NameFileSchm=NULL,
#CompType='gzip',
#CompLvl=5,
#Dict=NULL,
#log=NULL
#)
# test_that("write parquet file with basic parameter",
#      {
#        data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#        NameFile <- 'out.parquet'
#        NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile)
#        expect_true (file.exists(NameFile))
#        if (file.exists(NameFile)) { file.remove(NameFile)}
# 
#      })

# test_that("write parquet file with dict length of one is sent as a parameter",
#           {
#             data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#             NameFile <- 'out.parquet'
#             Dict <- c(TRUE)
#             NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict)
#             expect_true (file.exists(NameFile))
#             if (file.exists(NameFile)) { file.remove(NameFile)}
#           })

# test_that("when dict exists and length is not 1 and not equal to number of columns in data, throw an exception",
#           {
#             data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#             NameFile <- 'out.parquet'
#             Dict <- c(TRUE, FALSE, TRUE)
#             returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict), silent = TRUE)
#             testthat::expect_true((class(returnClass)[1] == "try-error"))
#           })

# test_that("when schme exists, write the file",
#           {
#             data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#             NameFile <- 'out.parquet'
#             Dict <- c(TRUE)
#             Schm <- "tests/testthat/def.wrte.parq/prt_calibrated.avsc"
#             returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict, Schm = Schm), silent = TRUE)
#             testthat::expect_true((class(returnClass)[1] == "try-error"))
#             if (file.exists(NameFile)) { file.remove(NameFile)}
#             
#           })