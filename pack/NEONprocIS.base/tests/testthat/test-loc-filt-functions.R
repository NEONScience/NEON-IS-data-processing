##############################################################################################
#' @title Unit test of NEON Filter named location

#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Run unit tests for Filter named location, which reads named location information (including geolocation history) from JSON file
#' for NEON instrumented systems sensor and filter it for a date-time range

#' @param NameFileIn Filename (including relative or absolute path). Must be json format.
#' @param NameFileOut Filename (including relative or absolute path). Must be json format. Defaults to
#' NULL, in which case only the filtered json will be returned in list format
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' location information will be filtered for the exact time of TimeBgn

#' @return TRUE when a test passes. Log errors when fails and moves on to the next test. \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.base")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.base")
#'
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT)
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT)
#' NameFileIn <- 'locations.json'
#' NameFileOut <- 'filtered_locations.json'
#' loc <- def.loc.filt(NameFileIn,NameFileOut,TimeBgn,TimeEnd)

#' @seealso \link[NEONprocIS.base]
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-01-07)
#     original creation
##############################################################################################
# Define test context
context("\n       | Filter named location information by date-time range\n")

# Test Filter named location information by date-time range for NEON instrumented systems sensors
test_that("   Testing Filter named location information by date-time range", {
  ##########
  ##########  Happy path:::: Named location(s) filtered by date-time range
  ##########
  #        "name": "CFGLOC108440"
  #        "site": "HARV",
  #        "install_date": "2017-02-07T00:17:20Z",
  #        "remove_date": "2017-02-07T00:18:28Z"
  
  
  NameFileIn = 'locations.json'
  
  NameFileOut = 'locations-out.json'
  
  # Happy path test 1:  No features and locations in the time range by sending today's date as the Begin and NULL as End date
  
  cat(
    "\n       |===================================================================================|\n"
  )
  cat("\n       |------ Positive test 1:: timeBgn is now and timeEnd is NULL                        |\n")
  cat("\n       |------ No features returned in this time range                                     |\n")
  cat(
    "\n       |===================================================================================|\n"
  )
  
  TimeBgn <- Sys.Date()
  TimeEnd <- NULL
  
  locReturned <-
    NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) == 0)
  
  # Happy path test 2: will have features in the time range
  
  cat(
    "\n       |------ Positive test 2:: between '2017-02-06T00:10:20Z' and '2017-02-07T00:18:28Z' |\n"
  )
  cat("\n       |------ Will have features returned in the time range                               |\n")
  cat(
    "\n       |===================================================================================|\n"
  )
  
  TimeBgn <- base::as.POSIXct('2017-02-06T00:10:20Z')
  TimeEnd <- base::as.POSIXct('2017-02-07T00:18:28Z')
  
  locReturned <-
    NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) > 0)
  
  #
  # Sad path test 1:  An empty json is passed on to def.loc.filt
  
  NameFileIn = 'locations-empty.json'
  
  TimeBgn <- base::as.POSIXct('2017-02-06T00:10:20Z')
  TimeEnd <- base::as.POSIXct('2017-02-07T00:18:28Z')
  
  cat("\n       |------ Negatgive test 1::An empty json is passed on to def.loc.filt               |\n")
  cat("\n       |------ Log the error and exit                                                     |\n")
  cat(
    "\n       |==================================================================================|\n\n"
  )
  
  locReturned <-
    NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  # expect_true (length(locReturned$features) == 0)
  
  #
  # Sad path test 2:  An invalid json is passed on to def.loc.filt
  
  cat("\n       |------ Negatgive test 2::An invalid json is passed on                             |\n")
  cat("\n       |------ Log the error and exit                                                     |\n")
  cat(
    "\n       |==================================================================================|\n\n"
  )
  
  NameFileIn = 'locations-invalid.json'
  
  locReturned <-
    NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  # expect_true (length(locReturned$features) == 0)
  
  # Or check to see if the output file is generated and then remove it after testing
  if (file.exists("locations-out.json")) {
    file.remove("locations-out.json")
  }
})
