##############################################################################################
#' @title Get metadata/properties from sensor locations json file 

#' @description
#' Definition function. Read sensor locations json file and return a data frame of metadata/properties 
#' filtered for a selected named location and/or install time range of interest. This function also
#' reads location-based location files (no sensor install information)

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param NameLoc Character value of the named location to restrict output to. Defaults to NULL, 
#' in which case no filtering is done for named location
#' @param TimeBgn POSIXct timestamp of the start time of interest (inclusive). Defaults to NULL, 
#' in which case no filtering is done for installed time range. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param TimeEnd POSIXct timestamp of the end time of interest (non-inclusive). Defaults to NULL, in 
#' which case the location information will be filtered for the exact time of TimeBgn. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples 
#' # Not run
#' # NameFile <- '/scratch/pfs/prt_calibrated_location_group/prt/2019/01/01/767/location/prt_767_locations.json'
#' # NameLoc <- 'CFGLOC100140'
#' # TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT)
#' # TimeEnd <- base::as.POSIXct('2019-01-02',tz='GMT)
#' # locMeta <- NEONprocIS.base::def.loc.meta(NameFile=NameFile,NameLoc=NameLoc,TimeBgn=TimeBgn,TimeEnd=TimeEnd)

#' @seealso \link[NEONprocIS.base]{def.read.avro.deve}
#'
#' @export

#   Mija Choi (2021-07-30)
#     Add tests after adding parsing of active periods
##############################################################################################
context("Location metadata tests")

test_that("When no restriction, get all locations",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile)
            expect_true (length(locationMetaData$site) > 0)
            
          })


test_that("When restricted to location, return only that location",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            nameLoc <- "CFGLOC101663"
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, NameLoc = nameLoc)
            expect_true (length(locationMetaData$site) == 1)
           # expect_true (locationMetaData[1], equals("POSE"))

          })

test_that("location that are install before or equal to time Begin ",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)

          })

test_that("location that have removal date before time Begin",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-05-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 0)

          })

test_that("location that have removal date before time Begin",
          {
            nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
            timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)

          })


test_that("location that have install, removal, transaction dates null",
          {
            nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations_alldates_null.json"
            timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
            expect_true (length(locationMetaData$site) == 1)
            testthat::equals(locationMetaData$install_date, "NA")
            testthat::equals(locationMetaData$remove_date, "NA")
            testthat::equals(locationMetaData$transaction_date, "NA")
            expect_true (length(locationMetaData$active_periods) == 1)

          })

test_that("location that have install, removal, and remove data after timeBgn",
          {
            nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations.json"
            timeBgn <- base::as.POSIXct('2018-09-05',tz='GMT')
            timeEnd <- base::as.POSIXct('2019-09-05',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn, TimeEnd=timeEnd)
            expect_true (length(locationMetaData$site) == 1)
            expect_true (length(locationMetaData$active_periods) == 1)
            
          })

test_that("test the active dates",
          {
            nameFile <- "def.loc.meta/test_input/pfs/2020/12/31/CFGLOC113261/location/CFGLOC113261.json"
            timeBgn <- base::as.POSIXct('2020-12-30',tz='GMT')
            timeEnd <- base::as.POSIXct('2021-01-31',tz='GMT')
            locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn, TimeEnd=timeEnd)
            expect_true (length(locationMetaData$site) == 1)
            expect_true (length(locationMetaData$active_periods) == 1)
            
          })
