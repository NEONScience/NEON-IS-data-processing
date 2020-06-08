#library(testthat)
#source("R/def.arg.pars.R")


test_that("When one of the arguments is not passed as a character, processing stops",
          {

            inputargs <- c('/scratch/test','DIR_OUT', 10)
            names(inputargs) <- c("DirIn", "DirOut", "Freq")

            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs),
                             silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))


          })

test_that("When the Optional value Parameter name is not in the NameParaOptn, thow an exception ",
          {
            inputargs <- c("DirIn=/scratch/test","DirOut=DIR_OUT","Freq=10|20")
            NameParaOptn <- "Freq"
            ValuParaOptn <-  c(Freqs = 30)
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                      silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })

test_that("When one of the Parameter doesn't have a value, throw an exception",
          {
            inputargs <- c("DirIn=","DirOut=DIR_OUT","Freq=10|20")
            NameParaOptn <- "Freq"
            ValuParaOptn <-  c(Freq = 20)
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                               silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })

test_that("When one of the Parameter value is from system environment variables which doesn't exist, throw an exception",
          {
            inputargs <- c("DirIn=/scratch/test","DirOut=$DIR_OUT","Freq=10|20")
            NameParaOptn <- "Freq"
            ValuParaOptn <-  c(Freq = 20)
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                               silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })


test_that("When one of the Required Parameters value is missing, throw an exception",
          {
            inputargs <- c("DirIn=/scratch/test","DirOut=DIR_OUT","Freq=10|20")
            NameParaReqd <- "MissingReqTest"
            NameParaOptn <- "Freq"
            ValuParaOptn <-  c(Freq = 20)
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaReqd = NameParaReqd, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                               silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })

test_that("When one of the Required Parameters value is missing, throw an exception",
          {
            inputargs <- c("DirIn=/scratch/test","DirOut=DIR_OUT","Freq=10|20")
            NameParaReqd <- "DirIn"
            NameParaOptn <- "Freq"
            ValuParaOptn <-  c(Freq = 20)
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaReqd = NameParaReqd, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                               silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })

