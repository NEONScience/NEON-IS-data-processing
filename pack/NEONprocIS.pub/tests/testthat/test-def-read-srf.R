##############################################################################################
#' @title Unit test of def.read.srf.R, 
#' Read Science Review Flags for NEON instrumented systems data products from JSON file to data frame

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description 
#' Definition function. Read Science Review Flags information from json file and convert to data frame.

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param strJson character string of data in JSON format (as produced by rjson::toJSON()). Note that
#' only one of NameFile or strJson may be entered. If more than one are supplied, the first
#' valid input will be used.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A data frame with all science review flags contained in the json file. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2023-03-07)
#      Original Creation
##############################################################################################
test_that("   Testing def.read.srf.R, definition function. Read science review file",{

  wk_dir <- getwd()
  
  #1. a correct test json
  testInputFile <- 'pfs/surfacewaterPhysical_testSRF/surfacewater-physical_ARIK130100/surfacewater-physical_PRLA130100_science_review_flags.json'
  tt = NEONprocIS.pub::def.read.srf(NameFile=testInputFile)
  testthat::expect_true(is.data.frame(tt) == TRUE)
  testthat::expect_true(tt$group_name[1] == "surfacewater-physical_PRLA130100")
  testthat::expect_true(tt$id[1] == 15381)
  testthat::expect_true(tt$start_date[1] == as.POSIXct("2019-10-01 10:00:00", "GMT"))
  testthat::expect_true(tt$end_date[1] == as.POSIXct("2020-01-03", "GMT"))
  testthat::expect_true(tt$measurement_stream_name[1] == "NEON.D09.PRLA.DP1.20016.001.04114.130.100.030")
  testthat::expect_true(tt$srf_term_name[1] == "sWatElevFinalQFSciRvw")
  testthat::expect_true(tt$srf[1] == 1)
  testthat::expect_true(tt$user_comment[1] == "Pressure transducer vent clogged periodically, causing barometric pressure fluctuations to be reflected in data.")
  testthat::expect_true(tt$create_date[1] == as.POSIXct("2020-11-25 18:45:15", "GMT"))
  testthat::expect_true(tt$last_update_date[1] == as.POSIXct("2020-11-25 18:45:15","GMT"))
  
  #2. strJson only provided 

  srfItems <- list(
        "group_name" =  "surfacewater-physical_PRLA130100",
        "id"= 15381,
        "start_date" = "2019-10-01T10:00:00Z",
        "end_date" = "2020-01-03T00:00:00Z",
        "measurement_stream_name" = "NEON.D09.PRLA.DP1.20016.001.04114.130.100.030",
        "srf_term_name" = "sWatElevFinalQFSciRvw",
        "srf" = 1,
        "user_comment" = "Pressure transducer vent clogged periodically, causing barometric pressure fluctuations to be reflected in data.",
        "create_date" = "2020-11-25T18:45:15Z",
        "last_update_date" = "2020-11-25T18:45:15Z"
        )

  srfList = array(srfItems)
  srfList <- list("science_review_flags" =  srfList)
  srfJson = rjson::toJSON(srfList)

  tt = try(NEONprocIS.pub::def.read.srf(strJson = srfJson), silent = TRUE)

  #3. no params provided 
  
  tt = try(NEONprocIS.pub::def.read.srf(), silent = TRUE)

})
