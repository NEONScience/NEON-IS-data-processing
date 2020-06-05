library(testthat)
source("R/def.loc.meta.R")


# test_that("When one of the arguments is not passed as a character, processing stops",
#           {
#             
#             inputargs <- c('/scratch/test','DIR_OUT', 10)
#             names(inputargs) <- c("DirIn", "DirOut", "Freq")
# 
#             returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs),
#                              silent = TRUE)
#             testthat::expect_true((class(returnedPara)[1] == "try-error"))
#            
# 
#           })


test_that("When restricted to location, then return only that locaiton",
          {
            inputargs <- c("DirIn=/scratch/test","DirOut=DIR_OUT","Freq=10|20")
            NameParaOptn <- c("OptnPara1", "OptnPara2")
            #ValuParaOptn <-  c("OptnPara1 = test1", "OptnPara2 = test2")
            ValuParaOptn <-  c("OptnPara1 = test1")
            returnedPara <-try(NEONprocIS.base::def.arg.pars(arg = inputargs, NameParaOptn = NameParaOptn, ValuParaOptn = ValuParaOptn),
                      silent = TRUE)
            testthat::expect_true((class(returnedPara)[1] == "try-error"))
          })
# 
# test_that("location that are install before or equal to time Begin ",
#           {
#             nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
#             timeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
#             locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
#             expect_true (length(locationMetaData$site) == 1)
# 
# 
#           })
# 
# test_that("location that have removal date before time Begin",
#           {
#             nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
#             timeBgn <- base::as.POSIXct('2019-05-05',tz='GMT')
#             locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
#             expect_true (length(locationMetaData$site) == 0)
# 
# 
#           })
# 
# test_that("location that have removal date before time Begin",
#           {
#             nameFile <- "def.loc.meta/test_input/pfs/prt_calibrated_location_group/prt/2019/01/01/16247/prt_16247_location.json"
#             timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
#             locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
#             expect_true (length(locationMetaData$site) == 1)
# 
# 
#           })
# 
# 
# test_that("location that have install, removal, transaction dates null",
#           {
#             nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations_alldates_null.json"
#             timeBgn <- base::as.POSIXct('2019-09-05',tz='GMT')
#             locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn)
#             expect_true (length(locationMetaData$site) == 1)
#             testthat::equals(locationMetaData$install_date, "NA")
#             testthat::equals(locationMetaData$remove_date, "NA")
#             testthat::equals(locationMetaData$transaction_date, "NA")
#             expect_true (length(locationMetaData$active_periods) == 1)
# 
#           })
# 
# test_that("location that have install, removal, and remove data after timeBgn",
#           {
#             nameFile <- "def.loc.meta/test_input/pfs/2019/01/02/CFGLOC101580/location/prt_20208_locations.json"
#             timeBgn <- base::as.POSIXct('2018-09-05',tz='GMT')
#             timeEnd <- base::as.POSIXct('2019-09-05',tz='GMT')
#             locationMetaData <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, TimeBgn = timeBgn, TimeEnd=timeEnd)
#             expect_true (length(locationMetaData$site) == 1)
#             expect_true (length(locationMetaData$active_periods) == 1)
#             
#           })





