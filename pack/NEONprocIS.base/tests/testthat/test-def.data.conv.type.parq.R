#library(testthat)
#source("R/def.data.conv.type.parq.R")

test_that("when type is sent as a character",
          {
            data <- data.frame(x=c(1,2,3),y=c('one','two','three'),stringsAsFactors=FALSE)
            type <- data.frame(name=c('x'),type=c('string|utf8'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.character(dataOut$x[1]))
            testthat::expect_true(is.character(dataOut$x[2]))
            testthat::expect_true(is.character(dataOut$x[3]))
            is.

          })

#show it to Cove , it should stop processing
# test_that("when name of the data to be converted is not in the data",
#           {
#             data <- data.frame(x=c(1,2,3),y=c('one','two','three'),stringsAsFactors=FALSE)
#             type <- data.frame(name=c('z'),type=c('string|utf8'),stringsAsFactors=FALSE)
#             dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
#             testthat::expect_true(is.character(typeof(dataOut$x[1])))
#             testthat::expect_true(is.character(typeof(dataOut$x[2])))
#             testthat::expect_true(is.character(typeof(dataOut$x[3])))
# 
#           })
#
#show it to Cove
# test_that("when data needs to be converted to timestampe",
#           {
#             data <- data.frame(x=c(1,2,3),y=c(base::as.POSIXlt('2020-01-01'),base::as.POSIXlt('2020-01-02'), base::as.POSIXlt('2020-01-03')),stringsAsFactors=FALSE)
#             type <- data.frame(name=c('y'),type=c('timestamp-millis'),stringsAsFactors=FALSE)
#             dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
#             testthat::expect_true(is. (typeof(data$y[1])))
#             testthat::expect_true(is.character(typeof(data$y[2])))
#             testthat::expect_true(is.character(typeof(data$y[3])))
# 
#           })

test_that("when data needs to be converted to int",
          {
            data <- data.frame(x=c(1,2,3),y=c("4", "5", "6"),stringsAsFactors=FALSE)
            type <- data.frame(name=c('y'),type=c('int'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.integer(dataOut$y[1]))
            testthat::expect_true(is.integer(dataOut$y[2]))
            testthat::expect_true(is.integer(dataOut$y[3]))

          })

test_that("when data needs to be converted to float",
          {
            data <- data.frame(x=c(1,2,3),y=c("4", "5", "6"),stringsAsFactors=FALSE)
            type <- data.frame(name=c('y'),type=c('float'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.numeric(dataOut$y[1]))
            testthat::expect_true(is.numeric(dataOut$y[2]))
            testthat::expect_true(is.numeric(dataOut$y[3]))

          })

test_that("when data needs to be converted to double",
          {
            data <- data.frame(x=c(1,2,3),y=c("4", "5", "6"),stringsAsFactors=FALSE)
            type <- data.frame(name=c('x'),type=c('double'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.double(dataOut$x[1]))
            testthat::expect_true(is.double(dataOut$x[2]))
            testthat::expect_true(is.double(dataOut$x[3]))

          })

test_that("when data needs to be converted to boolean",
          {
            data <- data.frame(x=c(1,2,3),y=c("4", "5", "6"),stringsAsFactors=FALSE)
            type <- data.frame(name=c('x'),type=c('boolean'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.logical(dataOut$x[1]))
            testthat::expect_true(is.logical(dataOut$x[2]))
            testthat::expect_true(is.logical(dataOut$x[3]))

          })

test_that("when data needs to be converted to list, return the same datatype as the original parameters",
          {
            data <- data.frame(x=c("1","2","3"),y=c("4", "5", "6"),stringsAsFactors=FALSE)
            type <- data.frame(name=c('x'),type=c('list'),stringsAsFactors=FALSE)
            dataOut <- NEONprocIS.base::def.data.conv.type.parq(data=data,type=type)
            testthat::expect_true(is.character(dataOut$x[1]))
            testthat::expect_true(is.character(dataOut$x[2]))
            testthat::expect_true(is.character(dataOut$x[3]))
            
          })