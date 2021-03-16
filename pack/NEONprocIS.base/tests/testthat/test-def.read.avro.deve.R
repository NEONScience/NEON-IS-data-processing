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

test_that("   Testing Filter named location information by date-time range", {
  os_type = Sys.info()["sysname"]
  # Execute the test when the OS is Linux-based, skip otherwise
  if (os_type == "Linux")
  {
    cat("\n When more than one input is sent as an input, consider just the first one\n")
    
    workingDirPath <- getwd()
    
    nameFile <- file.path(workingDirPath, "def.read.avro.deve/prt_test.avro")
    nameFile2 <- file.path(workingDirPath, "def.read.avro.deve/prt_test2.avro")
    nameLib <- file.path(workingDirPath, "ravro.so")
    print(nameLib)
    rpt <- def.read.avro.deve(NameFile = c(nameFile, nameFile2),NameLib = nameLib)
    
    col_List = c('source_id','site_id','readout_time','resistance')   
    expect_true ((is.data.frame(rpt)) && !(is.null(rpt)))
    expect_true (all (names(rpt) == col_List ))
    
    
    cat("\n Check data types of the reutrn list\n")
    
    workingDirPath <- getwd()
    nameFile <- file.path(workingDirPath, "def.read.avro.deve/prt_test.avro")
    nameLib <-  file.path(workingDirPath, "ravro.so")
    rpt <- try(def.read.avro.deve(NameFile = nameFile, NameLib = nameLib),silent = FALSE)
    
    testthat::equals(length(rpt), 4)
    testthat::equals(class(rpt$source_id), "character")
    testthat::equals(class(rpt$site_id), "character")
    testthat::equals(class(rpt$readout_time), "POSIXct")
    testthat::equals(class(rpt$resistance), "numeric")
    
    
  }
})
