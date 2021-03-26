##############################################################################################
#' @title Unit test of def.loc.geo.hist.R, read sensor locations json file and return the geolocation history of
#' all the configured locations within the file
#' 
#' @author
#' Mija Choi \email{choim@batelleEcology.org}

#' @description
#' Run unit tests for def.loc.geo.hist.R, Read sensor locations json file and return the geolocation history of
#' all the configured locations within the file
#' 
#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' A list of configured locations found in the file. Nested within the list element for each 
#' configured location is a list of geolocation history, one list element per location change. 
#' Further nested within the list element for each geolocation is a variable list of properties 
#' of that geolocation. Each geolocation property may also be a list of elements.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' To run with testthat:
#' devtools::test(pkg="<path>/NEON-IS-data-processing/pack/NEONprocIS.base")
#' an example, devtools::test(pkg="C:/projects/NEON-IS-data-processing/pack/NEONprocIS.base")
#' 
#' To run the test
#' NameFile <- '~/pfs/aquatroll200_23614_locations.json'
#' locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFile)

#' @seealso \link[NEONprocIS.base]
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2021-03-15)
#     original creation
##############################################################################################
library(rapportools)

# Define test context
#context("\n       | Read sensor locations json file and return the geolocation history of all the configured locations within the file \n")

# Read sensor locations json file and return the geolocation history of all the configured locations within the file
test_that("   Read sensor locations json file and return the geolocation history of all the configured locations within the file ", {
  ##########

  testDir = "testdataJson/"

  # Happy path #1:a valid location json with location_properties 

  cat("\n       |=====================================   Test Summary   ====================================|\n")
  cat("\n       |== a location json with location_properties        ==|\n")
  
  testFile = 'prt_24688_locations.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  testthat::expect_true (is.list(locGeoHist))
  testthat::expect_match(names(locGeoHist), locMeta$name) 

 # Happy path #2: a location json with StartDate and with EndDate
 
  cat("\n       |== a location json with StartDate and with EndDate ==|\n")
  testFile = 'locations-wStartDate-wEndDate.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  testthat::expect_true (is.list(locGeoHist))
  testthat::expect_match(names(locGeoHist), locMeta$name) 
  
  # Happy path #3: a location json with null StartDate
  cat("\n       |== a location json with null StartDate             ==|\n")
  testFile = 'locations-nullStartDate.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  testthat::expect_true (is.list(locGeoHist))
  testthat::expect_match(names(locGeoHist), locMeta$name) 
  
  # Happy path #4: a location json with #level= 2
  cat("\n       |== a location json with #level= 2                  ==|\n")
  testFile = 'locations-2lvl-ref-locs.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  
  # Happy path #4: a location json with #level= 3
  cat("\n       |== a location json with #level= 3                  ==|\n")
  testFile = 'locations-3lvl-ref-locs.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  testthat::expect_true (is.list(locGeoHist))
  testthat::expect_match(names(locGeoHist), locMeta$name) 
  
  # Happy path #5: a location json with #level= 0
  cat("\n       |== a location json with #level= 0                  ==|\n")
  testFile = 'locations-0lvl-ref-locs.json'
  NameFileIn <- paste0(testDir, testFile)
  locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFileIn)
  locGeoHist <- NEONprocIS.base::def.loc.geo.hist(NameFile=NameFileIn)
  testthat::expect_true (is.list(locGeoHist))
  testthat::expect_match(names(locGeoHist), locMeta$name) 
  
}
)
