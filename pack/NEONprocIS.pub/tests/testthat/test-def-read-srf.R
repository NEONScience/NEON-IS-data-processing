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
#  testOutputDir = "pfs/out"
  testInputFile <- 'pfs/surfacewaterPhysical_testSRF/surfacewater-physical_PRLA130100/surfacewater-physical_PRLA130100_science_review_flags.json'
  tt = NEONprocIS.pub::def.read.srf(NameFile=testInputFile)
  print(tt)
  
})