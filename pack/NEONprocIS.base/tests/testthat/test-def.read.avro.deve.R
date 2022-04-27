##############################################################################################
#' @title Unit test of def.read.avro.deve.R, definition function. Read in AVRO file. Uses a super developmental version of the library. The
#' requisite dependent libraries must be installed on the host system.

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' This test runs on Windows only due to the ravro file type, ".so" which is currently available
# .so for Linux-based systems and the Android OS.
# .dll for Windows

#' @param NameFile String. Name (including relative or absolute path) of AVRO file.
#' @param NameLib String. Name (including relative or absolute path) of AVRO library.
#' The file type NameLib is .so at the time when the test is written
# .so for Linux-based systems and the Android OS.

#' @return A data frame of the data contained in the AVRO file. The schema is included in attribute 'schema'

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' myData <- NEONprocIS.base::def.read.avro.deve(NameFile='/scratch/test/myFile.avro',NameLib='/ravro.so')
#' attr(myData,'schema') # Returns the schema

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#     revised the test
#   Mija Choi (2020-03-10)
#     add the OS info due to ravro filetype constraint and execute this test only if the OS is Linux based
##############################################################################################

test_that("   Testing def.read.avro.deve.R, definition function. Read AVRO file",
          {
            os_type = Sys.info()["sysname"]
            # Execute the test when the OS is Linux-based, skip otherwise
            if (os_type == "Linux")
            {
              workingDirPath <- getwd()
              
              nameLib <- file.path(workingDirPath, "ravro.so")
              col_List = c('source_id', 'site_id', 'readout_time', 'resistance')
              
              nameFile <- file.path(workingDirPath, "testdata/HART_data.avro")
              #
              # Happy path 1: When one input data is passed in,
              #
              cat("\n |=================================================================================|\n")
              cat("\n   Test 1: When one input data is passed in.")
              cat("\n |=================================================================================|\n")
              
              rpt <- def.read.avro.deve(NameFile = nameFile, NameLib = nameLib)
              
              expect_true ((is.data.frame(rpt)) && !(is.null(rpt)))
              expect_true (all (names(rpt) == col_List) && rpt$site_id == "HARV")
              
              nameFile <- file.path(workingDirPath, "def.read.avro.deve/prt_test.avro")
              nameFile2 <- file.path(workingDirPath, "def.read.avro.deve/prt_test2.avro")
              #
              # Happy path 2: When more than one input are passed in, the first one will be selected
              #
              cat("\n |=================================================================================|")
              cat("\n   Test 2: When more than one input are passed in, the first one will be taken.")
              cat("\n |=================================================================================|\n")
              
              rpt <- def.read.avro.deve(NameFile = c(nameFile2, nameFile), NameLib = nameLib)
              
              expect_true ((is.data.frame(rpt)) && !(is.null(rpt)))
              expect_true (all (names(rpt) == col_List) && rpt$site_id == "not-HARV")
              
              # Sad path 1: avro file has one column, resistance, missing
              cat("\n |=================================================================================|")
              cat("\n  Test 3 - negative: avro file has one column, resistance, missing.")
              cat("\n |=================================================================================|\n")
              
              nameFile <- file.path(workingDirPath, "def.read.avro.deve/prt_test_noResistance.avro")
              col_List_noResistance = c('source_id', 'site_id', 'readout_time')
              rpt <- def.read.avro.deve(NameFile = nameFile, NameLib = nameLib)
              
              expect_true ((is.data.frame(rpt)) && !(is.null(rpt)))
              expect_true (all (names(rpt) == col_List_noResistance))
              
              # Sad path 2: 
              cat("\n |=================================================================================|")
              cat("\n  Test 4 - negative: ")
              cat("\n |=================================================================================|\n")

              nameFile <- file.path(workingDirPath, "testdata/weather_type_null.avro")
              rpt <- try(def.read.avro.deve(NameFile = nameFile, NameLib = nameLib), silent = TRUE)
  #            expect_true ((is.data.frame(rpt)) && !(is.null(rpt)))

            }
          })
