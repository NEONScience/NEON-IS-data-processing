##############################################################################################
#' @title Unit test of def.srf.aply.R, 
#' Apply SRF records to data in a publication table

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description Apply the science review flag to the data in a publication table (i.e. L1+).
#' Application of the science review flag involves:
#' 1) Populating the SRF field/column in the publication table with the SRF value in each SRF record for the time
#'    interval over which it applies,
#' 2) Forcing the final quality flag to 1 if the value of the SRF is 1 or 2. Note that a SRF value of 0 indicates
#' that a previous manual flag was removed and the final quality flag is computed normally, 
#' 3) Setting terms/columns to be redacted to NA if the SRF value is 2. The terms to redact are indicated in the 
#' publication workbook - the redactionFlag column for these terms is populated with the measurement stream 
#' of the SRF. 

#' @param srf Data frame of science review flag records to apply, as produced by NEONprocIS.pub::def.read.srf
#' Note that the srf records should already be filtered for those corresponding to the location/product instance
#' of the data in dataTabl (i.e. (site, domain, HOR, VER).
#' Note: the qfFinl column is added to the srf data frame using the def.srf.term.qf.finl function:
#' srf$qfFinl <- NEONprocIS.pub::def.srf.term.qf.finl(termSrf=srf$srf_term_name,log=log)

#' 
#' @param dataTabl Data frame of data for a single publication workbook table. Note that the column names in the data frame
#' should match those in the pub workbook (need not be ordered the same).
#' 
#' @param pubWbTabl Data frame of the publication workbook filtered for the publication table corresponding to dataTabl.
#' 
#' @param NameVarTimeBgn Character string. The name of the time variable in dataTabl (and pubWbTabl) indicating 
#' the start time of the aggregation interval. Default is 'startDateTime'.
#' 
#' @param NameVarTimeEnd Character string. The name of the time variable in dataTabl (and pubWbTabl) indicating 
#' the end time of the aggregation interval. If the data are instantaneous output, set NameVarTimeEnd to the same variable as 
#' NameVarTimeBgn. If any part of the aggregation interval falls within the time range of the SRF, 
#' the SRF will be applied. Note that the aggregation end time and SRF end time are exclusive, meaning they are not considered 
#' part of the interval. Default is 'endDateTime'. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame with the same format and size of dataTabl, with applicable SRF actions applied.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.pub]{def.read.srf}
#' @seealso \link[NEONprocIS.pub]{def.read.pub.wb}

#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2023-03-21)
#      Original Creation
##############################################################################################
test_that("   Testing def.read.srf.R, definition function. Read science review file",{

  wk_dir <- getwd()
  
  #1. Check to see if the science review flag to the data in a publication table
  #testInputFile <- 'pfs/pubWb/surfacewater-physical_PRLA130100_science_review_flags.json'
  testInputFile <- 'pfs/pubWb/par-quantum-line_CPER001000_science_review_flags.json'
  srf = NEONprocIS.pub::def.read.srf(NameFile=testInputFile)
  
  # srf needs to have the qfFinl column
  srf$qfFinl <- NEONprocIS.pub::def.srf.term.qf.finl(termSrf=srf$srf_term_name,log=log)

  #FilePubWb <- 'pfs/pubWb//PublicationWorkbook_elevSurfacewater.txt'
  FilePubWb <- 'pfs/pubWb//PublicationWorkbook_parQuantumLine.txt'
  pubWb <- NEONprocIS.pub::def.read.pub.wb(NameFile=FilePubWb)
  TablPub <- 'PARQL_1min'
  FileData <- c('pfs/pubWb/par-quantum-line_CPER001000_2023-02-03_PARQL_1min_001.parquet') # Files must have same # of rows
  data <- base::lapply(FileData,arrow::open_dataset)
  dataTabl <- NEONprocIS.base::def.read.parq(NameFile = FileData)
  
  TimeBgn='startDTime'
  TimeEnd='endDateTime'

  returnedDataTabl = NEONprocIS.pub::def.srf.aply(srf=srf,dataTabl=dataTabl, pubWbTabl=pubWb, NameVarTimeBgn=TimeBgn,NameVarTimeEnd=TimeEnd)

  testthat::expect_true(is.data.frame(returnedDataTabl) == TRUE)
  # A data frame with the same format and size of dataTabl.
  testthat::expect_true((nrow(returnedDataTabl) == nrow(dataTabl)) == TRUE)
  testthat::expect_true((ncol(returnedDataTabl)>= ncol(dataTabl)) == TRUE)
  # A data frame with applicable SRF actions applied.
  testthat::expect_true(any(srf$qfFinl %in% colnames(returnedDataTabl)))
  # srf$srf[2] = 2
  srf$srf[3] = 2
  returnedDataTabl = NEONprocIS.pub::def.srf.aply(srf=srf,dataTabl=dataTabl, pubWbTabl=pubWb, NameVarTimeBgn=TimeBgn,NameVarTimeEnd=TimeEnd)
  
  
})
