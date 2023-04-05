##############################################################################################
#' @title Unit test of def.srf.filt.R, 
#' Filter Science Review Flag file for a particular time range

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Definition function. Filter a data frame of SRF flags for a particular time range and return information
#' relevant only to data processing. Any SRF records outside the input time range will be removed from 
#' the output, and values in user_comment, create_date, and update_date are replaced
#' with NA (null is written for these fields in the output json file, if an output file is indicated in 
#' the input parameters). Date-times in the columns start_date and end_date that are outside the time
#' range of interest will be truncated to the time range of interest. Any start_dates or end_dates that
#' fall within the time of interest are not modified.  
#' NOTE: This function does not include error checking of the data frame, since this function is often run 
#' in a large loop after the input json file has already been checked for conformance to the expected schema. 
#' If error checking of the srf contents is desired, use a function like NEONprocIS.pub::def.read.srf to 
#' read in the SRF data fed into this function.

#' @param srf Data frame of science review flags, as read from NEONprocIS.pub::def.read.srf
#' @param NameFileOut Optional. Filename (including relative or absolute path) to write the filtered output.
#' Defaults to NULL, in which case only the filtered data frame will be returned.
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' group information will be filtered for a the exact time of TimeBgn
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The filtered SRF data frame. If NameFileOut is specified, 
#' the truncated information will also be written to file in json format (the same json format as the function 
#' NEONprocIS.pub::def.read.srf expects. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # NOT RUN
#' srf <- NEONprocIS.qaqc('/path/to/input/srfs.json')
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT)
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT)
#' NameFileOut <- '/path/to/filtered/srfs.json'
#' srfFilt <- def.srf.filt(srf,NameFileOut,TimeBgn,TimeEnd)


#' @seealso \link[NEONprocIS.pub]{def.read.srf}

# changelog and author contributions / copyrights
#   Mija Choi (2023-03-29)
#      Original Creation
##############################################################################################
test_that("   Testing def.srf.filt.R, Filter Science Review Flag file ",{

  wk_dir <- getwd()
  DirOutBase = 'pfs/out'
  pathFileOut <- fs::path(wk_dir,DirOutBase)
  outFileName <-  fs::path(pathFileOut,'filteredSrf.json')
  
  #1. 2 rows in the output filtered srf dataframe when output file is not specified

  testInputFile <- 'pfs/surfacewaterPhysical_testSRF/sw-physical_ARIK130100/sw-physical_PRLA130100_science_review_flags.json'
  
  srf = NEONprocIS.pub::def.read.srf(NameFile=testInputFile)
  TimeBgn <- base::as.POSIXct('2019-10-01',tz='GMT')
  TimeEnd <- base::as.POSIXct('2020-01-30',tz='GMT')
  NameFileOut <- 'pfs/out/filtered/srfs.json'
  srfFilt <- NEONprocIS.pub::def.srf.filt(srf=srf,TimeBgn=TimeBgn,TimeEnd=TimeEnd)
  testthat::expect_true(is.data.frame(srfFilt) == TRUE)
  testthat::expect_true(nrow(srfFilt) == 2)
  
  #2. Only one row in the output srf file in the current directory

  TimeBgn <- base::as.POSIXct('2022-01-20',tz='GMT')
  TimeEnd <- base::as.POSIXct('2022-01-26',tz='GMT')
  NameFileOut <- 'filteredSrfs.json'
  NEONprocIS.pub::def.srf.filt(srf=srf,NameFileOut= NameFileOut,TimeBgn=TimeBgn,TimeEnd=TimeEnd)
  testthat::expect_true(file.exists(NameFileOut) == TRUE)
  
  #3. no row in the output srf
  
  if (file.exists(NameFileOut)) {
    file.remove(NameFileOut)
  }
  
  TimeBgn <- base::as.POSIXct('2023-03-20',tz='GMT')
  TimeEnd <- base::as.POSIXct('2023-03-26',tz='GMT')
  srfFilt <- NEONprocIS.pub::def.srf.filt(srf=srf,TimeBgn=TimeBgn,TimeEnd=TimeEnd)
  testthat::expect_true(is.data.frame(srfFilt) == TRUE)
  testthat::expect_true(nrow(srfFilt) == 0)
 })
