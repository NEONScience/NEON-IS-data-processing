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
#   Mija Choi (2020-01-30)
#     Added json and json schema validations
#   Mija Choi (2020-05-6)
#     Added more tests to get the test coverage over 95%
##############################################################################################
# Define test context
#context("\n       | Filter named location information by date-time range\n")

# Test Filter named location information by date-time range for NEON instrumented systems sensors
test_that("   Testing Filter named location information by date-time range", {
  ##########
  ##########  Tests to have Named location(s) filtered by date-time range
  ##########
  #        "name": "CFGLOC108440"
  #        "site": "HARV",
  #        "install_date": "2017-02-07T00:17:20Z",
  #        "remove_date": "2017-02-07T00:18:28Z"
  
  NameFileIn = 'locations.json'
  
  NameFileOut = 'locations-out.json'
  
  # Happy path #1:  No features and locations in the time range by sending today's date as the Begin and NULL as End date
  
  cat("\n       |=====================================   Test Summary   ====================================|\n")
  
  TimeBgn <- Sys.Date()
  TimeEnd <- NULL
  cat("\n       |------ Positive test 1:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   timeBgn is now and timeEnd is NULL                                |\n\n")
 
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) == 0)
  
  cat("\n       |------                   No features returned in this time range                           |\n")
  cat("\n       |===========================================================================================|\n")
  
  # Happy path #2: will have features in the time range
  
  cat("\n       |------ Positive test 2:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   between '2017-02-06T00:10:20Z' and '2017-02-07T00:18:28Z'         |\n\n")
  
  TimeBgn <- base::as.POSIXct('2017-02-06T00:10:20Z')
  TimeEnd <- base::as.POSIXct('2017-02-07T00:18:28Z')
  
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) > 0)
  
  cat("\n       |------                   Will have features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
 
 # Happy path #3-a: locations json has 3 features with 1 level ref locations, will have features returned in the time range
  
  cat("\n       |------ Positive test 3-a:: Input JSON is valid and conforms to the schema                  |\n")
  cat("\n       |------                   locations json has 3 features with 1 level ref locations          |\n\n")
  cat("\n       |------                   will return 3 features                                            |\n")
  cat("\n       |------                   between '2017-02-06T00:10:20Z' and '2017-02-07T00:18:28Z'         |\n\n")
  
  NameFileIn = 'locations-3-features.json'
 
  TimeBgn <- base::as.POSIXct('2017-05-01 00:10:20', tz = 'GMT')
  TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz = 'GMT')
 
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) == 3)
  
  cat("\n       |------                   Will have 2 features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
  # Happy path #3-b: locations json has 3 features with 1 level ref locations, will have features returned in the time range
  
  cat("\n       |------ Positive test 3-b:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   locations json has 3 features with 1 level ref locations            |\n\n")
  cat("\n       |------                   will return 2 features (note that timeBgn to filter 1 feture out    |\n")
  cat("\n       |------                   between '2018-05-01 00:10:20Z' and '2020-03-09 00:18:28Z'           |\n\n")
  
  NameFileIn = 'locations-3-features.json'
  
  TimeBgn <- base::as.POSIXct('2018-05-01 00:10:20', tz = 'GMT')
  TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz = 'GMT')
  
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) == 2)
  
  cat("\n       |------                   Will have 3 features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
  
  #
  # Happy path #4: locations json has 2 lvl nested reference_locations and will have features returned in the time range
  
  cat("\n       |------ Positive test 4:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   locations json has 2 level nested reference_locations             |\n\n")
  cat("\n       |------                   between '2019-05-01 00:10:20Z' and '2020-03-09 00:18:28Z'         |\n\n")
  
  NameFileIn = 'locations-2lvl-ref-locs.json'
 
  TimeBgn <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
  TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz = 'GMT')
  
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) > 0)
  
  cat("\n       |------                   Will have features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
  #
  # Happy path #5: will have features in the time range
  
  cat("\n       |------ Positive test 5:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   locations json has 3 lvl nested reference_locations                    |\n\n")
  cat("\n       |------                   between '2017-02-06T00:10:20Z' and '2017-02-07T00:18:28Z'         |\n\n")
  
  NameFileIn = 'locations-3lvl-ref-locs.json'
  
  NameFileOut = 'locations-out.json'
  
  TimeBgn <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
  TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz = 'GMT')
  
  locReturned <-
    NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) > 0)
  
  cat("\n       |------                   Will have features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
  #  
  # Happy path test 6: will have features in the time range
  
  cat("\n       |------ Positive test 6:: Input JSON is valid and conforms to the schema                    |\n")
  cat("\n       |------                   locations json has 0 nested reference_locations                   |\n\n")
  cat("\n       |------                   between '2019-05-01 00:10:20Z' and '2020-03-09 00:18:28Z'         |\n\n")
  
  NameFileIn = 'locations-0lvl-ref-locs.json'
  
  TimeBgn <- base::as.POSIXct('2019-05-01 00:10:20', tz = 'GMT')
  TimeEnd <- base::as.POSIXct('2020-03-09 00:18:28', tz = 'GMT')
  
  locReturned <-NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  expect_true (length(locReturned$features) > 0)
  
  cat("\n       |------                   Will have features returned in the time range                     |\n")
  cat("\n       |===========================================================================================|\n")
  #
  # Sad path test 1:  A blank json is passed on to def.loc.filt
  
  NameFileIn = 'locations-empty.json'
  
  TimeBgn <- base::as.POSIXct('2017-02-06T00:10:20Z')
  TimeEnd <- base::as.POSIXct('2017-02-07T00:18:28Z')
  
  cat("\n       |------ Negative test 1::A blank json is passed on to def.loc.filt                          |\n")
  cat("\n       |------                  A blank Json is not strictly valid.                                |\n\n")
  locReturned <- NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd)
  
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat("\n       |===========================================================================================|\n")
  
  expect_true (class(locReturned) == 'try-error')
  #
  # Sad path test 2:  A json with syntax error is passed on to def.loc.filt
  #
  cat("\n       |------ Negative test 2::A json with syntax error(s) is passed on                           |\n")
  cat("\n       |------                  such as mismatching double quotes, missing values, etc...          |\n\n")
  NameFileIn = 'locations-invalid.json'
  
  locReturned <-
    try (NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd))
  
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat("\n       |===========================================================================================|\n")
  
  expect_true (class(locReturned) == 'try-error')
  
  # A json has no syntax errors, but does not conform to the schema
  # Sad path test 3:  An empty contents json, {}, is passed on to def.loc.filt
  
  cat("\n       |------ Negative test 3::An empty contents json, {}, is passed on                           |\n")
  cat("\n       |------                 That is strictly valid, but does not conforms to the schema         |\n\n")
  
  NameFileIn = 'locations-emptyContents.json'
  
  locReturned <- try(NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd))
  
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat("\n       |===========================================================================================|\n")
 
  expect_true (class(locReturned) == 'try-error')
  #
  # A json has no syntax errors, but does not conform to the schema
  # Sad path test 4:  A json has missing fields, is passed on to def.loc.filt
  
  cat("\n       |------ Negative test 4::A json with missing fields, is passed on                           |\n")
  cat("\n       |------                  That is strictly valid, but does not conforms to the schema        |\n\n")
  
  NameFileIn = 'locations-invalidSchema.json'
  
  locReturned <-
    try(NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd))
  
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat(
    "\n       |===========================================================================================|\n"
  )
  expect_true (class(locReturned) == 'try-error')
  
  # Or check to see if the output file is generated and then remove it after testing
  if (file.exists("locations-out.json")) {
    file.remove("locations-out.json")
  }
  #
  # A json does not exist
  # Sad path test 5:  A non-existing json is passed on to def.loc.filt
  #
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat("\n       |------                 This will err out due to input json missing                         |\n\n")
  
  NameFileIn = 'not-existing.json'
  
  locReturned <-
    try(NEONprocIS.base::def.loc.filt (NameFileIn, NameFileOut, TimeBgn, TimeEnd))
  
  cat("\n       |----------- Return error, log the message and exit                                         |\n")
  cat("\n       |===========================================================================================|\n")
  
  expect_true (class(locReturned) == 'try-error')
  # Or check to see if the output file is generated and then remove it after testing
  if (file.exists("locations-out.json")) {
    file.remove("locations-out.json")
  }
}
)
