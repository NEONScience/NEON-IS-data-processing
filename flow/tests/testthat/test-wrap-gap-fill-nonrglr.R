##############################################################################################
#' @title Gap filling module for NEON IS data processing.

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' #' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirFill List of the terminal directories where the data to be
#' gap filled resides. This will be one or more child levels away from "DirIn". All files in the
#' terminal directory will be gap filled. The value may also be a vector of terminal directories,
#' separated by pipes (|). All terminal directories must be present and at the same directory level.
#' For example, "DirFill=data|flags" indicates to gap fill the data files within each the data
#' and flags directories.
#' 
#' @param FileSchm Character value (optional), where value is the full path to schema for data output by
#' this workflow. The value may be NA, in which case the output schema will be the same as the input
#' data. The value may be a single file, in which case it will apply to all output, or
#' multiple values in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirFill and value is the path to the schema file
#' for the output of that directory. Multiple dir:value pairs are separated by pipes (|).
#' For example, "FileSchm=data:/path/to/schemaData.avsc|flags:NA" indicates that the
#' output from the data directory will be written with the schema /path/to/schemaData.avsc and the
#' output from the flags directory will be the same as the input files found in that
#' directory.
#' 
#' @param WndwFill Character value. The window in minutes in which data are expected. It is formatted as a 3 character sequence,
#'  representing the number of minutes over which any number of measurements are expected. 
#' For example, "WndwFill=015" refers to a 15-minute interval, while "WndwAgr=030" refers to a 
#' 30-minute  interval. 
#'  
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#'
#' @description Unit tests for wrap.gap.fill.nonrglr.
#'
# changelog and author contributions / copyrights
#   Nora Catolico (2026-02-09)
#     initial creation
##############################################################################################

context("\n       | Unit test of Gap Filling Non-Regularized module for NEON IS data processing \n")

test_that("Unit test of wrap.gap.fill.nonrglr", {
  source('../../flow.gap.fill.nonrglr/wrap.gap.fill.nonrglr.R')
  library(stringr)
  
  log <- NEONprocIS.base::def.log.init(Lvl = "debug")
  
  # Define input parameters (adjust paths to your test files as needed)
  DirIn <- "pfs/sunav2_location_group_and_restructure/sunav2/2025/06/23/CFGLOC110819"
  DirOutBase <- "pfs/nitrate_out"
  DirFill <- c("data", "flags")
  WndwFill <- 15 # 15 minutes window
  
  #Schemas
  schmData<-'pfs/sunav2_avro_schemas/sunav2_logfilled.avsc'
  schmCalFlag<-'pfs/sunav2_avro_schemas/sunav2_calibration_flags.avsc'
  schmLogFlag <-  'pfs/sunav2_avro_schemas/sunav2_log_flags.avsc'
  NameCol = c('DirFill', 'FileSchmFill')
  SchmDirs <- c("data","flags","flags")
  Schmas = c(schmData,schmCalFlag,schmLogFlag)
  SchmFill <- data.frame(SchmDirs, Schmas)
  names(SchmFill) <- NameCol
  # Read in the schema(s)
  SchmFill$SchmFill <- NA
  for (idxSchmFill in 1:base::length(SchmFill$FileSchmFill)) {
    if (SchmFill$FileSchmFill[idxSchmFill] != 'NA') {
      SchmFill$SchmFill[idxSchmFill] <-
        base::paste0(base::readLines(SchmFill$FileSchmFill[idxSchmFill]),
                     collapse = '')
    }
  }
  
  # Clean up before run
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # 1. Base test: Fill gaps in input data and check output files exist
  wrap.gap.fill.nonrglr(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    DirFill = DirFill,
    WndwFill = WndwFill,
    SchmFill = SchmFill,
    DirSubCopy = NULL
  )
  
  dirInData <- base::paste0(DirIn, '/data')
  dirInFlags <- base::paste0(DirIn, '/flags')
  dirInLoc <- base::paste0(DirIn, '/location')
  fileData <- base::dir(dirInData)
  fileFlags <- base::dir(dirInFlags)
  fileLoc <- base::dir(dirInLoc)
  dirOutData <- gsub("gap_fill_nonrglr", "out", dirInData)
  dirOutFlags <- gsub("gap_fill_nonrglr", "out", dirInFlags)
  dirOutLoc <- gsub("gap_fill_nonrglr", "out", dirInLoc)
  
  # Check for filled output presence
  testthat::expect_true ((file.exists(fs::path(dirOutData,fileData), recursive = TRUE)) &&
                 (file.exists(fs::path(dirOutFlags, fileFlags)[1], recursive = TRUE)))
  
  # Check for pass-through output of 'location' subdir
  testthat::expect_true ((file.exists(fs::path(dirOutLoc, fileLoc)[1], recursive = TRUE)))
  
  # 2. Check that gap-filled output includes min expected points
  dataChk <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  InfoDir <- NEONprocIS.base::def.dir.splt.pach.time(dir=DirIn)
  timeBgn <- InfoDir$time
  timeEnd <- InfoDir$time + as.difftime(1, units = 'days')
  # Sequence of expected window starts
  all_starts <- seq(timeBgn, timeEnd - WndwFill*60, by = WndwFill*60)
  num_starts <- length(all_starts)
  testthat::expect_true(length(dataChk$readout_time)>=num_starts)
  
  # Remove output
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # 3. No files in input directory: should error
  DirIn_nofiles <- "pfs/sunav2_location_group_and_restructure/sunav2/2025/06/24/CFGLOCEMPTY"
  returnedOutput <- try(wrap.gap.fill.nonrglr(
    DirIn = DirIn_nofiles,
    DirOutBase = DirOutBase,
    DirFill = DirFill,
    WndwFill = WndwFill,
    SchmFill = SchmFill
  ), silent = TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  # Remove output
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # 4. File missing readout_time column: should error
  DirIn_noreadout <- NEONprocIS.base::def.read.parq(NameFile=fs::path(dirOutData,fileData))
  DirIn_noreadout$readout_time<-NULL
  returnedOutput <- try(wrap.gap.fill.nonrglr(
    DirIn = DirIn_noreadout,
    DirOutBase = DirOutBase,
    DirFill = DirFill,
    WndwFill = WndwFill,
    SchmFill = SchmFill
  ), silent = TRUE)
  testthat::expect_true("try-error" %in% class(returnedOutput))
  
  # Remove output
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
})
