##############################################################################################
#' @title Regularization module for NEON IS data processing.

#' @author
#' Mija Choi \email{choim@batelleEcology.org}
#'
#' @description Wrapper function. Bin data to generate a regular time sequence of observations.
#' General code workflow:
#'      Error-check input parameters
#'      Read regularization frequency from location file if expected
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the regularized data
#'
#'
#' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /location (only required if any ParaRglr$FreqRglr are NA)
#' The data folder holds a any number of data files to be regularized.
#' The location folder holds json files with the location data corresponding to the data files in the data
#' directory. The regularization frequency will be gathered from the location files for rows in ParaRglr where
#' ParaRglr$FreqRglr is NA.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param ParaRglr Data frame with minimum variables:\cr
#' \code{DirRglr} Character. The directory that is a direct child of DirIn where the data to be
#' regularized resides. Note that all indicated directories in the data frame must be present in DirIn.\cr
#' \code{SchmRglr} A json-formatted character string containing the schema for the regularized output. May be NA,
#' in which case the schema will be created automatically from the output data frame with the same variable names
#' as the input data frame.
#' \code{FreqRglr} Numeric value of the regularization frequency in Hz. May be NA, in which case the location file
#' mentioned in the DirIn parameter will be used to find the regularization frequency ("Data Rate" property).
#' Note that a non-NA value for FreqRglr supercedes the data rate in the location file. \cr
#' \code{MethRglr} Character string indicating the regularization method (per the choices in
#' eddy4R.base::def.rglr for input parameter MethRglr)\cr
#' \code{WndwRglr} Character string indicating the windowing method (per the choices in
#' eddy4R.base::def.rglr for input parameter WndwRglr)\cr
#' \code{IdxWndw} Character string indicating the index allocation method (per the choices in
#' eddy4R.base::def.rglr for input parameter IdxWndw)\cr
#' \code{RptTimeWndw} Logical TRUE or FALSE (default) pertaining to the
#' choices in eddy4R.base::def.rglr for input parameter RptTimeWndw. TRUE will output
#' two additional columns at the end of the output data file for the start and end times of the time windows
#' used in the regularization. Note that the output variable readout_time will be included in the output
#' regardless of the choice made here, and will probably match the start time of the bin unless
#' MethRglr=CybiEcTimeMeas.\cr
#' \code{DropNotNumc} Logical TRUE (default) or FALSE pertaining to the
#' choices in eddy4R.base::def.rglr for input parameter DropNotNumc. TRUE will drop
#' all non-numeric columns prior to the regularization (except for readout_time). Dropped columns will
#' not be included in the output.\cr
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Regularized data output in Parquet format in DirOutBase, where DirOutBase directory
#' replaces BASE_REPO of DirIn but otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaRglr <- data.frame(DirRglr=c('data','flags'),
#'                        SchmRglr = c(NA,NA),
#'                        FreqRglr = c(0.5,0.5),
#'                        MethRglr = c('CybiEc','CybiEc'),
#'                        WndwRglr = c('Trlg','Trlg'),
#'                        IdxWndw = c('IdxWndwMin','IdxWndwMin'),
#'                        RptTimeWndw = c(FALSE,FALSE),
#'                        DropNotNumc = c(FALSE,FALSE),
#'                        stringsAsFactors=FALSE)
#' wrap.rglr(DirIn="~/pfs/relHumidity_calibrated_data/hmp155/2020/01/01/CFGLOC101252",
#'           DirOutBase="~/pfs/out",
#'           ParaRglr=ParaRglr
#' )

#' @seealso None currently
# changelog and author contributions / copyrights
#   Mija Choi (2021-10-20)
#     original creation
##############################################################################################
# Define test context
context("\n       | Unit test of Regularization module for NEON IS data processing \n")

test_that("Unit test of wrap.rglr.R", {
  source('../../flow.rglr/wrap.rglr.R')
  library(stringr)
  FreqTest <- 0.5 #Hz
  ParaRglr <- data.frame(
    DirRglr = c('data', 'flags'),
    SchmRglr = c(NA, NA),
    FreqRglr = c(FreqTest,FreqTest),
    MethRglr = c('CybiEc', 'CybiEc'),
    WndwRglr = c('Trlg', 'Trlg'),
    IdxWndw = c('IdxWndwMin', 'IdxWndwMin'),
    RptTimeWndw = c(FALSE, FALSE),
    DropNotNumc = c(FALSE, FALSE),
    stringsAsFactors = FALSE
  )
  DirIn = "pfs/regularization/ptb330a/2020/01/01/CFGLOC100247"
  DirOutBase = "pfs/out"
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  #1 Test 1 - no active periods in location file. Regularize to day in directory structure. 
  
  wrap.rglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirSubCopy = "location",
    ParaRglr = ParaRglr
  )
  
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  dirInLoc <- base::paste0(DirIn, '/location')
  dirInUcrt <- base::paste0(DirIn, '/uncertainty_coef')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  fileLoc <- base::dir(dirInLoc)
  fileUcrt <- base::dir(dirInUcrt)
  dirOutData <- gsub("regularization", "out", dirInData)
  dirOutFlags <- gsub("regularization", "out", dirInFlags)
  dirOutLoc <- gsub("regularization", "out", dirInLoc)
  dirOutUcrt <- gsub("regularization", "out", dirInUcrt)
  
  # Check for regularized output
  expect_true ((file.exists(fs::path(dirOutData,fileData), recursive = TRUE)) &&
                 (file.exists(fs::path(dirOutFlags, fileFlags), recursive = TRUE)))
  
  # Check for pass-through output
  expect_true ((file.exists(fs::path(dirOutLoc, fileLoc), recursive = TRUE)))

  # Check for no uncertainty_coef output (not passed-through)
  expect_false ((file.exists(fs::path(dirOutUcrt, fileUcrt), recursive = TRUE)))
  
  # Check for all NA for the whole day at the specified regularization frequency
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  InfoDir <- NEONprocIS.base::def.dir.splt.pach.time(dir=DirIn)
  expect_true(nrow(dataChk)==FreqTest*86400)
  expect_true(dataChk$readout_time[1] == InfoDir$time)
  expect_true(utils::tail(dataChk$readout_time,1) == InfoDir$time + as.difftime(1,units='days') - as.difftime(1/FreqTest,units='secs'))
  
  # Clear the output
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  #2 Test for more than 1 location file, grab active period and regularization freq from the one that has it

  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn = "pfs/regularization/ptb330a/2020/01/01/CFGLOC100959"
  ParaRglr_NA <- ParaRglr
  ParaRglr_NA$FreqRglr = c(NA, NA)
  wrap.rglr(DirIn = DirIn,
            DirOutBase = DirOutBase,
            ParaRglr = ParaRglr_NA)
  
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  dirInLoc <- base::paste0(DirIn, '/location')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  fileLoc <- base::dir(dirInLoc)
  dirOutData <- gsub("regularization", "out", dirInData)
  dirOutFlags <- gsub("regularization", "out", dirInFlags)

  # Check for regularized output
  expect_true ((file.exists(fs::path(dirOutData,fileData), recursive = TRUE)) &&
                 (file.exists(fs::path(dirOutFlags, fileFlags), recursive = TRUE)))
  
  # Check for all NA for over the active at the regularization frequency in the location file
  locMeta <- NEONprocIS.base::def.loc.meta(fs::path(dirInLoc,'CFGLOC100959.json'))
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  timeActv <- locMeta$active_periods[[1]]
  FreqTest <- as.numeric(locMeta$dataRate)
  lenTimeActv <- difftime(timeActv$end_date,timeActv$start_date,units='days')
  expect_true(nrow(dataChk)==as.numeric(lenTimeActv)*FreqTest*86400)
  expect_true(dataChk$readout_time[1] == timeActv$start_date)
  expect_true(utils::tail(dataChk$readout_time,1) == timeActv$end_date - as.difftime(1/FreqTest,units='secs'))
  
  #3 Test for multiple active periods from location file - regularizing over each interval
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn = "pfs/regularization/ptb330a/2020/01/01/CFGLOC101256"
  wrap.rglr(DirIn = DirIn,
            DirOutBase = DirOutBase,
            ParaRglr = ParaRglr_NA)
  dirInData <- base::paste0(DirIn, '/data')
  dirInLoc <- base::paste0(DirIn, '/location')
  fileData <- base::dir(dirInData)
  fileLoc <- base::dir(dirInLoc)
  dirOutData <- gsub("regularization", "out", dirInData)

  locMeta <- NEONprocIS.base::def.loc.meta(fs::path(dirInLoc,'CFGLOC101256.json'))
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  timeActv <- locMeta$active_periods[[1]]
  FreqTest <- as.numeric(locMeta$dataRate)
  lenTimeActv <- as.difftime(0,units='days')
  for(idxTimeActv in seq_len(nrow(timeActv))){
    lenTimeActv <- lenTimeActv + difftime(timeActv$end_date[idxTimeActv],timeActv$start_date[idxTimeActv],units='days')
  }
  expect_true(nrow(dataChk)==as.numeric(lenTimeActv)*FreqTest*86400)
  
  #4 Test for multiple data values in a bin. Selecting the correct one. Also write bin start and end times
  # Part A - first value in bin (indicated from existing parameters)
  dataIn <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirInData,fileData))
  expect_true(dataChk$temperature[1]==dataIn$temperature[1])
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # Part B - last value in bin, and write out bin start/end
  ParaRglr_WndwMax <- ParaRglr_NA
  ParaRglr_WndwMax$IdxWndw = c('IdxWndwMax', 'IdxWndwMax')
  ParaRglr_WndwMax$RptTimeWndw = c(TRUE,TRUE)
  wrap.rglr(DirIn = DirIn,
            DirOutBase = DirOutBase,
            ParaRglr = ParaRglr_WndwMax)
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  expect_true(dataChk$temperature[1]==dataIn$temperature[3])
  expect_true(all(c('timeWndwBgn','timeWndwEnd') %in% names(dataChk)))
  
  #5 send in a bad output schema
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  ParaRglr_badSchm <- ParaRglr_WndwMax
  ParaRglr_badSchm$SchmRglr <- c('NA','NA')
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr_badSchm),
                        silent=TRUE)
  
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  #6 Test for no location files
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn = "pfs/proc_group/prt_noFiles/2019/01/01/3119"
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  
  dirOutData <- gsub("proc_group", "out", dirInData)
  dirOutFlags <- gsub("proc_group", "out", dirInFlags)
  
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr_NA),
                        silent=TRUE)
  
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  #
  # Test 7, readout_time is missing
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  DirIn = "pfs/proc_group/prt_14491_noreadoutTime/2019/01/01/14491"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr),
                        silent=TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))

  #
  # Test 8, wrong data, fail to read parquet file
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn = "pfs/proc_group/prt_14491_wrong_data/2019/01/01/14491"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr),
                        silent=TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  # Test 9, location file of "Data Rate":"NA"
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }

    DirIn = "pfs/proc_group/prt/2019/01/01/3119"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr_NA),
                        silent=TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
})

