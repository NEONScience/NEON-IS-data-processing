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
  ParaRglr <- data.frame(
    DirRglr = c('data', 'flags'),
    SchmRglr = c(NA, NA),
    FreqRglr = c(0.5, 0.5),
    MethRglr = c('CybiEc', 'CybiEc'),
    WndwRglr = c('Trlg', 'Trlg'),
    IdxWndw = c('IdxWndwMin', 'IdxWndwMin'),
    RptTimeWndw = c(FALSE, FALSE),
    DropNotNumc = c(FALSE, FALSE),
    stringsAsFactors = FALSE
  )
  DirIn = "pfs/proc_group/prt/2019/01/01/CFGLOC101670"
  DirOutBase = "pfs/out"
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  #1 Test 1
  
  wrap.rglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirSubCopy = "data_flags",
    ParaRglr = ParaRglr
  )
  
  # remove the test output symbolic link
  DirSrc = 'CFGLOC101670'
  cmdLs <- base::paste0('ls ', base::paste0(DirSrc))
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  dirOutData <- gsub("proc_group", "out", dirInData)
  dirOutFlags <- gsub("proc_group", "out", dirInFlags)
  
  expect_true ((file.exists(dirOutData, fileData, recursive = TRUE)) &&
                 (file.exists(dirOutFlags, fileFlags, recursive = TRUE)))
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  #2 Test for no location files
  
  ParaRglr_NA <- ParaRglr
  ParaRglr_NA$FreqRglr = c(NA, NA)
  DirIn = "pfs/proc_group/prt_noFiles/2019/01/01/3119"
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  
  dirOutData <- gsub("proc_group", "out", dirInData)
  dirOutFlags <- gsub("proc_group", "out", dirInFlags)
  
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr_NA),silent = TRUE)
  
  #  testthat::expect_true((class(returnedOutput)[1] == "try-error"))
  
  #3 Test for more than 1 location file
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  DirIn = "pfs/proc_group/prt_moreThanOneFile/2019/01/01/16247"
  returnedOutput <- wrap.rglr(DirIn = DirIn,
                              DirOutBase = DirOutBase,
                              ParaRglr = ParaRglr)
  
  # Test 4, need location files
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  DirIn = "pfs/proc_group/prt/2019/01/01/CFGLOC101670"
  returnedOutput <- wrap.rglr(DirIn = DirIn,
                              DirOutBase = DirOutBase,
                              ParaRglr = ParaRglr_NA)
  
  # Test 5, location file of "Data Rate":"NA"
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  DirIn = "pfs/proc_group/prt/2019/01/01/3119"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr_NA),silent = TRUE)
  #
  # Test 6, readout_time is missing
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  DirIn = "pfs/proc_group/prt_14491_noreadoutTime/2019/01/01/14491"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr),silent = TRUE)
  #
  # Test 7, wrong data, fail to read parquet file

  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn = "pfs/proc_group/prt_14491_wrong_data/2019/01/01/14491"
  returnedOutput <- try(wrap.rglr(DirIn = DirIn,
                                  DirOutBase = DirOutBase,
                                  ParaRglr = ParaRglr),silent = TRUE)

})
