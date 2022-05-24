##############################################################################################
#' @title Unit test of def.read.evnt.json.R, read event data in json format

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Definition function. Read event data in json format into data frame. Converts timestamps to POSIX.
#' NOTE: The json records must have uniform format.

#' @param NameFile String. Name (including relative or absolute path) of event json file.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which no logger other than
#' standard R error messaging will be used.

#' @return A data frame of the data contained in the json file.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' myEventData <- NEONprocIS.base::def.read.evnt.json(NameFile='/scratch/test/myFile.json')

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2022-04-12)
#     Test revision after the original
##############################################################################################
test_that("   Testing def.read.evnt.json.R, definition function. Read AVRO file",
          {
            workingDirPath <- getwd()
            
# Test 1, A valid event json is passed in            
            NameFile <- file.path(workingDirPath, "testdataJson/event.json")
            rpt <- NEONprocIS.base::def.read.evnt.json(NameFile = NameFile)
            testthat::equals(length(rpt), 4)
            testthat::equals(class(rpt$source_id), "character")
            testthat::equals(class(rpt$source_id), "string")
            testthat::equals(class(rpt$readout_time), "POSIXct")
            testthat::equals(class(rpt$resistance), "numeric")
            
# Test 2, the input json does not have timestamp            
            NameFile <- file.path(workingDirPath, "testdataJson/event_noTimestamp.json")
            rpt <- try(NEONprocIS.base::def.read.evnt.json(NameFile = NameFile), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
            
# Test 3, Not an event json is passed in
            NameFile <- file.path(workingDirPath, "def.read.avro.deve/prt_test.avro")
            rpt <- try(NEONprocIS.base::def.read.evnt.json(NameFile = NameFile), silent = TRUE)
            testthat::expect_true((class(rpt)[1] == "try-error"))
            
})
