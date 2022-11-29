#library(testthat)
#source("R/def.data.mapp.schm.parq.R")

test_that("make sure missing data column are added",
          {
            data <- data.frame("source_id"=c(1,2,3), "site_id"=c('one','two','three'), stringsAsFactors=FALSE)
            file <- "def.rcd.fix.miss.na/testdata.parquet"
            parqdata <- NEONprocIS.base::def.read.parq(NameFile= file)
            schema <- attr(parqdata,'schema')
            returnData <- NEONprocIS.base::def.data.mapp.schm.parq(data = data, schm = schema)
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 2)
            testthat::expect_equal(colnames(returnData), c("readout_time","suspectCalQF"))

          }
)

test_that("Data type is converted when ConvType is sent as true",
          {

            timestamp1 = base::as.POSIXct('2019-01-01',tz='GMT')
            timestamp2 = base::as.POSIXct('2019-01-02',tz='GMT')
            timestamp3 = base::as.POSIXct('2019-01-03',tz='GMT')
            data <- data.frame("suspectCalQF"=c(1,2,3), "site_id"=c('one','two','three'), "temp" = c("10", "20", "30"),
                               "readout_time" = c(timestamp1, timestamp2, timestamp3), stringsAsFactors=FALSE)
            file <- "def.rcd.fix.miss.na/testdata.parquet"
            parqdata <- NEONprocIS.base::def.read.parq(NameFile= file)
            schema <- attr(parqdata,'schema')
            returnData <- NEONprocIS.base::def.data.mapp.schm.parq(data = data, schm = schema, ConvType=TRUE)
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 2)
            testthat::expect_equal(colnames(returnData), c("readout_time","suspectCalQF"))
            testthat::expect_equal(class(returnData$suspectCalQF), "character")
            testthat::expect_true("POSIXct" %in% class(returnData$readout_time))

           }
)

test_that("make sure data is not converted when ConvType is not passed as a parameter",
          {
            timestamp1 = base::as.POSIXct('2019-01-01',tz='GMT')
            timestamp2 = base::as.POSIXct('2019-01-02',tz='GMT')
            timestamp3 = base::as.POSIXct('2019-01-03',tz='GMT')
            data <- data.frame("suspectCalQF"=c(1,2,3), "site_id"=c('one','two','three'), "temp" = c("10", "20", "30"),
                                           "readout_time" = c(timestamp1, timestamp2, timestamp3), stringsAsFactors=FALSE)
            file <- "def.rcd.fix.miss.na/testdata.parquet"
            parqdata <- NEONprocIS.base::def.read.parq(NameFile= file)
            schema <- attr(parqdata,'schema')
            returnData <- NEONprocIS.base::def.data.mapp.schm.parq(data = data, schm = schema, ConvType=TRUE)
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 2)
            testthat::expect_equal(colnames(returnData), c("readout_time","suspectCalQF"))
            testthat::expect_equal(class(returnData$suspectCalQF), "character")
            
          }

)

