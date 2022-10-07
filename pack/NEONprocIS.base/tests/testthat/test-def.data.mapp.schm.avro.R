#library(testthat)
#source("R/def.data.mapp.schm.avro.R")

test_that("make sure missing data column are added",
          {
            data <- data.frame("source_id"=c(1,2,3), "site_id"=c('one','two','three'), stringsAsFactors=FALSE)
            FileSchm <- "def.schm.avro.pars/prt_calibrated.avsc"
            schm <- base::paste0(base::readLines(FileSchm),collapse='')

            returnData <- NEONprocIS.base::def.data.mapp.schm.avro(data = data, schm = paste(unlist(schm), collapse=''))
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 4)
            testthat::expect_equal(colnames(returnData), c("source_id","site_id", "readout_time", "temp"))

          }
)

test_that("Data type is converted when ConvType is sent as true",
          {

            timestamp1 = base::as.POSIXct('2019-01-01',tz='GMT')
            timestamp2 = base::as.POSIXct('2019-01-02',tz='GMT')
            timestamp3 = base::as.POSIXct('2019-01-03',tz='GMT')
            data <- data.frame("source_id"=c(1,2,3), "site_id"=c('one','two','three'), "temp" = c("10", "20", "30"),
                               "readout_time" = c(timestamp1, timestamp2, timestamp3), stringsAsFactors=FALSE)
            FileSchm <- "def.schm.avro.pars/prt_calibrated.avsc"
            schm <- base::paste0(base::readLines(FileSchm),collapse='')

            returnData <- NEONprocIS.base::def.data.mapp.schm.avro(data = data, schm = schm, ConvType=TRUE)
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 4)
            testthat::expect_equal(colnames(returnData), c("source_id","site_id", "readout_time", "temp"))
            testthat::expect_equal(typeof(returnData$source_id), "character")
            testthat::expect_equal(class(returnData$temp), "numeric")
            testthat::expect_equal(class(returnData$readout_time), "integer")

           }
)

test_that("make sure data is not converted when ConvType is not passed as a parameter",
          {
            data <- data.frame("source_id"=c(1,2,3), "site_id"=c('one','two','three'), "temp" = c("10", "20", "30"), stringsAsFactors=FALSE)
            FileSchm <- "def.schm.avro.pars/prt_calibrated.avsc"
            schm <- base::paste0(base::readLines(FileSchm),collapse='')
            
            returnData <- NEONprocIS.base::def.data.mapp.schm.avro(data = data, schm = paste(unlist(schm), collapse=''))
            testthat::expect_true(is.list(returnData))
            testthat::expect_true(length(returnData) == 4)
            testthat::expect_equal(colnames(returnData), c("source_id","site_id", "readout_time", "temp"))
            testthat::expect_equal(class(returnData$source_id), "numeric")
            testthat::expect_equal(class(returnData$temp), "character")
          }

)

