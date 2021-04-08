##############################################################################################
#' @title Unit test of def.wrte.avro.deve.R, definition function. Write AVRO file (development library)
#' Write AVRO file from data frame. Uses a super developmental version of the library. The 
#' requisite dependent libraries must be installed on the host system.

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' This test runs on Windows only due to the ravro file type, ".so" which is currently available
# .so for Linux-based systems and the Android OS.
# .dll for Windows

#' @param data Data frame. Data to write to file.
#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param NameSchm String. Optional. Schema name. 
#' @param NameSpceSchm String. Optional. Schema namepace. 
#' @param Schm String. Optional. Json formatted string of the AVRO file schema. Example:\cr
#' "{\"type\" : \"record\",\"name\" : \"ST\",\"namespace\" : \"org.neonscience.schema.device\",\"fields\" : [ {\"name\" :\"readout_time\",\"type\" : {\"type\" : \"long\",\"logicalType\" : \"timestamp-millis\"},\"doc\" : \"Timestamp of readout expressed in milliseconds since epoch\"}, {\"name\" : \"soilPRTResistance\",\"type\" : [ \"null\", \"float\" ],\"doc\" : \"DPID: DP0.00041.001 TermID: 1728 Units: ohm Description: Soil Temperature, Level 0\",\"default\" : null} ],\"__fastavro_parsed\" : true}"\cr
#' Defaults to NULL, at which point the schema will be constructed using a different input or the data frame
#' @param NameFileSchm String. Optional. A filename (include relative or aboslute path) of an avro 
#' schema file (.avsc format). Defaults to NULL, at which point the schema will be constructed using a different 
#' input or from the data frame
#' @param NameLib String. Name (including relative or absolute path) of AVRO library. Defaults to ./ravro.so.

#' @return Numeric. 0 = successful write.
#' 
#' @examples
#  data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#  NameFile <- file.path(workingDirPath,"out.avro")
#  Schm <- file.path(workingDirPath,"tests/testthat/def.schm.avro.pars/prt_19963_2019-01-01.avro")
#  writeSuccess <- NEONprocIS.base::def.wrte.avro.deve(data=base::data.frame(), NameFile = NameFile, NameSchm = Schm, NameLib='ravro.so')

# changelog and author contributions / copyrights
#     revised the test
#   Mija Choi (2020-04-08)
#     add the OS info due to ravro filetype constraint and execute this test only if the OS is Linux based
##############################################################################################


test_that("When only required parameters are sent to the def.wrte.avro.deve function ",
          { 
            writeSuccess = -1
            data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
            workingDirPath <- getwd()
            outFile <- file.path(workingDirPath,"testdata/out.avro")
            schm <- file.path(workingDirPath,"tests/testthat/def.wrte.parq/prt_calibrated.avsc")
            writeSuccess <- NEONprocIS.base::def.wrte.avro.deve(data=data, NameFile = outFile, NameLib='ravro.so')
            expect_true (file.exists(outFile))
            if (file.exists(outFile)) { file.remove(outFile)}

          })

# test_that("When schema param is passed along with required parameters to the def.wrte.avro.deve function ",
#           {
#             data <- data.frame(x=c(1,2,3), y=c('one','two','three'), stringsAsFactors=FALSE)
#             workingDirPath <- getwd()
#             outFile <- file.path(workingDirPath,"testdata/out.avro")
#             Schm <- file.path(workingDirPath,"tests/testthat/def.schm.avro.pars/prt_19963_2019-01-01.avro")
#             writeSuccess <- NEONprocIS.base::def.wrte.avro.deve(data=base::data.frame(), NameFile = outFile, NameSchm = Schm, NameLib='ravro.so')
#             expect_true (file.exists(NameFile))
#             if (file.exists(NameFile)) { file.remove(NameFile)}
# 
#           })