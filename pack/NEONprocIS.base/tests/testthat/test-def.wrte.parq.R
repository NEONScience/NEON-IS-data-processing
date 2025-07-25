#library(testthat)
#source("R/def.wrte.parq.R")


test_that("write parquet file with basic parameter",
     {
       data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
       NameFile <- 'out.parquet'
       NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile)
       expect_true (file.exists(NameFile))
       if (file.exists(NameFile)) { file.remove(NameFile)}

     })

test_that("write parquet file with dict length of one is sent as a parameter",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict)
            expect_true (file.exists(NameFile))
            if (file.exists(NameFile)) { file.remove(NameFile)}
          })

test_that("when dict exists and length is not 1 and not equal to number of columns in data, throw an exception",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE, FALSE, TRUE)
            returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict), silent = TRUE)
            testthat::expect_true((class(returnClass)[1] == "try-error"))
          })

test_that("when schme exists, write the file",
          {
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            Schm <- "def.wrte.parq/prt_calibrated.avsc"
            returnClass <- try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict, Schm = Schm), silent = TRUE)
            testthat::expect_true((class(returnClass)[1] == "try-error"))
            if (file.exists(NameFile)) { file.remove(NameFile)}

          })

test_that("when NameFileSchm exists, write the file",
          {
            time1 <- base::as.POSIXct('2019-01-01',tz='GMT')
            time2 <- base::as.POSIXct('2019-01-02',tz='GMT')
            time3 <- base::as.POSIXct('2019-01-03',tz='GMT')
            data <- data.frame(z=c('test1','test2','test3'), l=c(4345, 5342, 6345), x=c(time1, time2, time3), y=c(7.0, 8.0, 9.0), stringsAsFactors=FALSE)
            NameFile <- 'out.parquet'
            Dict <- c(TRUE)
            NameFileSchm <- "def.wrte.parq/prt_calibrated.avsc"
            rpt <- NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile, Dict =  Dict, NameFileSchm = NameFileSchm)
            testthat::expect_true((length(rpt) == 4))

            if (file.exists(NameFile)) { file.remove(NameFile)}
            
          })

test_that("use arrow schema",
          {
                  # Successful case - pass in arrow schema 
                  data <- NEONprocIS.base::def.read.parq(NameFile='pfs/proc_group/prt/2019/01/01/27134/data/prt_14491_2019-01-01.parquet')
                  NameFile <- 'out.parquet'
                  Sys.setenv(LOG_LEVEL='debug')
                  # Create a parquet schema that changes a field name
                  SchmNew <- NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm='prt_calibrated_changeField.avsc')
                  # Write it out
                  rpt <- NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile,Schm=SchmNew) 
                  # Read it back in
                  dataNew <- NEONprocIS.base::def.read.parq(NameFile)
                  
                  testthat::expect_true(names(dataNew)[4] == 'newFieldName')
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
                  # Successful case - arrow schema attached as attribute to the data frame when none is input
                  # Attach parquet schema that changes a field name
                  attr(data,'schema') <- SchmNew
                  # Write it out
                  rpt <- NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile) 
                  # Read it back in
                  dataNew <- NEONprocIS.base::def.read.parq(NameFile)
                  
                  testthat::expect_true(names(dataNew)[4] == 'newFieldName')
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
                  # Failure case: number of vars in the schema don't match that of data frame
                  data <- NEONprocIS.base::def.read.parq(NameFile='pfs/proc_group/prt/2019/01/01/27134/flags/prt_14491_2019-01-01_flagsCal.parquet')
                  # Write it out
                  rpt <- base::try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile,Schm=SchmNew),silent=FALSE)
                  testthat::expect_true('try-error' %in% base::class(rpt))
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
                  # Failure case: base schema attached as an attribute of data frame
                  attr(data,'schema') <- SchmNew # Attach calibrated data schema to flags data
                  # Write it out
                  rpt <- base::try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile),silent=FALSE)
                  testthat::expect_true('try-error' %in% base::class(rpt))
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
                  # Successful case - arrow schema attached as attribute to the data frame when none is input
                  # Arrow Class: dictionary<values=string, indices=int32>
                  data <- NEONprocIS.base::def.read.parq(NameFile='def.wrte.parq/L0_data_resistance.parquet')
                  # Write it out
                  rpt <- NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile) 
                  # Read it back in
                  dataNew <- NEONprocIS.base::def.read.parq(NameFile)
                  
                  testthat::expect_true(class(dataNew[[3]]) == 'factor')
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
                  
                  
          })

test_that("use schema input as arrow schema object",
          {
                  # Successful case
                  data <- NEONprocIS.base::def.read.parq(NameFile='pfs/proc_group/prt/2019/01/01/27134/data/prt_14491_2019-01-01.parquet')
                  NameFile <- 'out.parquet'
                  Sys.setenv(LOG_LEVEL='debug')
                  # Create and attach parquet schema that changes a field name
                  SchmNew <- NEONprocIS.base::def.schm.parq.from.schm.avro(FileSchm='prt_calibrated_changeField.avsc')
                  attr(data,'schema') <- SchmNew
                  # Write it out
                  rpt <- NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile) 
                  # Read it back in
                  dataNew <- NEONprocIS.base::def.read.parq(NameFile)
                  
                  testthat::expect_true(names(dataNew)[4] == 'newFieldName')
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  # Failure case: number of vars in the schema don't match that of data frame
                  data <- NEONprocIS.base::def.read.parq(NameFile='pfs/proc_group/prt/2019/01/01/27134/flags/prt_14491_2019-01-01_flagsCal.parquet')
                  attr(data,'schema') <- SchmNew # Attach calibrated data schema to flags data
                  # Write it out
                  rpt <- base::try(NEONprocIS.base::def.wrte.parq(data = data, NameFile = NameFile),silent=FALSE)
                  testthat::expect_true('try-error' %in% base::class(rpt))
                  
                  if (file.exists(NameFile)) { file.remove(NameFile)}
                  
                  
          })
          
