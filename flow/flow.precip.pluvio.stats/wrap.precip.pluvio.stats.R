##############################################################################################
#' @title  Process data from Pluvio 200L weighing gauge precipitation sensors to 1 minute and 30 minute 
#' aggregations

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow.read in data, cal flags, plausibility and uncertainty coef, merge to one DF,
#' calculate bulk precipitaiton, uncertainty and aggregate flags appropriately for 30 minute data
#' 
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags
#'         /uncertainty_coeff

#' The flags folder holds two files containing calibration quality flags and plausibility flags 
#' These files should respectively be named in the convention:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsCal.parquet
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsPlausibility.parquet
#' All other files in this directory will be ignored.
#'
#'The data folder holds the precipitation data as accu_nrt from the pluvio sensor
#'
#'Uncertainty_coeff folder holds the uncertaint_coefficients.json file with uncertainty data provided by CVAL
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param SchmData (Optional). A json-formatted character string containing the schema for the aggregated data, standard calibration and
#' custom plausibility QFs as well as the heaterQMs

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data/flags/threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path (i.e. carried through as-is). Note that the 'stats' directory
#' is automatically populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the precipitation and uncertainty estimates along with 
#' quality flags (incl. the final quality flag) produced by the custom pluvio algorithm, where DirOutBase 
#' replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory structure of the 
#' input path. The terminal directories for each datum include, at a minimum, 'stats' and 'data'. The stats folder
#' contains one minute and 30-minute output for the relevant day of data. It contains the precipitation sum, uncertainty estimates,
#' quality flags and final quality flag. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn='/scratch/pfs/precipWeighing_qm_stats_test_data/
#' DirOutBase='/scratch/pfs/out_tb'
#' 
#' wrap.precip.pluvio.stats(DirIn,DirOutBase,DirSubCopy)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-07-21)
#     Initial creation
##############################################################################################
wrap.precip.pluvio.stats <- function(DirIn,
                               DirOutBase,
                               SchmData = NULL, #new schema with all data
                               DirSubCopy = NULL,
                               log = NULL) {
  
  # Load required libraries at start
  library(dplyr)
  library(lubridate)
  library(data.table)
  
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Gather info about the input directory and create the output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn, log = log)

  dir_paths <- list(
    data = fs::path(DirIn, 'data'),
    flags = fs::path(DirIn, 'flags'),
    uncertainty_coef = fs::path(DirIn, 'uncertainty_coef')
  )
  
  dirOut <- fs::path(DirOutBase, InfoDirIn$dirRepo)
  dirOutStat <- fs::path(dirOut, 'stats')
  
  NEONprocIS.base::def.dir.crea(
    DirBgn = dirOut,
    DirSub = c('stats'),
    log = log
  )
  
  # Copy directories more efficiently
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy, c('stats')))
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(
      DirSrc = fs::path(DirIn, DirSubCopy),
      DirDest = dirOut,
      LnkSubObj = FALSE,
      log = log
    )
  }
  
  # Get file lists more efficiently
  file_patterns <- list(
    data = '.parquet',
    flagsCal = 'flagsCal.parquet',
    flagsPlau = 'flagsPlausibility.parquet'
    
  )
  
  # Create mapping from file pattern names to directory names
  dir_mapping <- c(data = "data", flagsCal = "flags", flagsPlau = "flags")
  
  files <- lapply(names(file_patterns), function(type) {
    dir_key <- dir_mapping[[type]]
    base::list.files(dir_paths[[dir_key]], pattern = file_patterns[[type]], 
                     full.names = FALSE, recursive = TRUE)
  })
  names(files) <- names(file_patterns)
  
  # Read and merge datasets more efficiently
  data <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dir_paths$data, files$data),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  qfCal <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dir_paths$flags, files$flagsCal),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  qfPlau <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dir_paths$flags, files$flagsPlau),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  
  # Use data.table for faster join
  setDT(data)
  setDT(qfCal)
  setDT(qfPlau)
  data <- merge(data, qfCal, by = 'readout_time', all = TRUE)
  data <- merge(data, qfPlau, by = 'readout_time', all = TRUE)

  # Read uncertainty coefficients
  fileUcrt <- base::dir(dir_paths$uncertainty_coef)
  
  # Handle multiple UCRT files
  if (base::length(fileUcrt) == 0) {
    log$warn("No uncertainty coefficient files found")
    ucrtCoef <- base::list()
  } else {
    log$info(base::paste0("Found ", length(fileUcrt), " uncertainty coefficient file(s)"))
    ucrtCoef <- base::list()
    
    # Read all UCRT files
    for (i in seq_along(fileUcrt)) {
      nameFileUcrt <- fs::path(dir_paths$uncertainty_coef, fileUcrt[i])
      ucrtCoef_temp <- base::try(rjson::fromJSON(file = nameFileUcrt, simplify = TRUE), silent = FALSE)
      
      if (base::class(ucrtCoef_temp) == 'try-error') {
        log$error(base::paste0('Cannot read uncertainty file: ', nameFileUcrt))
        stop()
      } else {
        ucrtCoef <- c(ucrtCoef, ucrtCoef_temp)
      }
    }
  }
  
  # Process U_CVALA1 uncertainty coefficients
  if (length(ucrtCoef) > 0) {
    ucrtCoef_df <- dplyr::bind_rows(ucrtCoef)
    uCvalA1_rows <- ucrtCoef_df$Name == "U_CVALA1"
    uCvalA1 <- as.numeric(ucrtCoef_df$Value[uCvalA1_rows])
    
    # Handle multiple calibrations more efficiently
    if (length(uCvalA1) > 1) {
      log$info('Applying multiple U_CVALA1 calibration coefficients')
      cals <- ucrtCoef_df[uCvalA1_rows, ]
      cals[, c('start_date', 'end_date')] <- lapply(cals[, c('start_date', 'end_date')], 
                                                    function(x) as.POSIXct(x, format = "%Y-%m-%dT%H:%M:%OS", tz = 'GMT'))
      
      # Vectorized assignment using data.table
      data[, uCvalA1 := NA_real_]
      setorder(cals, start_date)
      
      for (i in seq_len(nrow(cals))) {
        data[readout_time >= cals$start_date[i] & readout_time <= cals$end_date[i], 
             uCvalA1 := as.numeric(cals$Value[i])]
      }
      
      # Log how many rows were assigned values
      assigned_rows <- sum(!is.na(data$uCvalA1))
      log$info(paste0('Applied U_CVALA1 coefficients to ', assigned_rows, ' rows'))
      
    } else if (length(uCvalA1) == 1) {
      data[, uCvalA1 := uCvalA1]
      log$info(paste0('Applied single U_CVALA1 coefficient: ', uCvalA1))
    } else {
      data[, uCvalA1 := 0]
      log$info('No U_CVALA1 coefficients found, setting to 0')
    }
  } else {
    data[, uCvalA1 := 0]
    log$info('No UCRT data available, setting uCvalA1 to 0')
  }
  
  # Optional: Check for any unassigned values and handle them
  if (exists("data") && "uCvalA1" %in% names(data)) {
    na_count <- sum(is.na(data$uCvalA1))
    if (na_count > 0) {
      log$warn(paste0(na_count, " rows have NA values for uCvalA1"))
    }
  }
  
  #apply UCRT to combined
  data[, combinedUcrt := uCvalA1 * accu_nrt]
    
  #if ucrt less than 0.1mm, change to 0.1mm (manufacturer accuracy spec)
  data[, combinedUcrt := fifelse(combinedUcrt < 0.1, 0.1, combinedUcrt)]
    
  # More efficient aggregation using data.table
  data[, `:=`(
    startDateTime_1min = floor_date(readout_time, '1 minute'),
    endDateTime_1min = ceiling_date(readout_time, '1 minute', change_on_boundary = TRUE)
  )]
  
  # First, aggregate to 1-minute intervals
  stats_01min <- data[, .(
    startDateTime = first(startDateTime_1min),
    endDateTime = first(endDateTime_1min),
    precipBulk = sum(accu_nrt, na.rm = TRUE),  # Sum precipitation within 1-minute interval
    precipBulkExpUncert = sqrt(sum(combinedUcrt^2, na.rm = TRUE)), # Quadrature sum for 1-minute
    precipNumPts = sum(!is.na(accu_nrt)), # Count non-NA values WITHIN each 1-minute group
    nullQF = ifelse(mean(nullQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(nullQF, na.rm = TRUE))),
    extremePrecipQF = ifelse(mean(rangeQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(rangeQF, na.rm = TRUE))),
    gapQF = ifelse(mean(gapQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(gapQF, na.rm = TRUE))),
    sensorErrorQF = ifelse(mean(sensorErrorQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(sensorErrorQF, na.rm = TRUE))),
    heaterErrorQF = ifelse(mean(heaterErrorQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(heaterErrorQF, na.rm = TRUE))),
    validCalQF = ifelse(mean(validCalQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(validCalQF, na.rm = TRUE))),
    suspectCalQF = ifelse(mean(suspectCalQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(suspectCalQF, na.rm = TRUE)))
  ), by = .(startDateTime_1min)]
  
  # Calculate finalQF for 1-minute data
  stats_01min[, finalQF := pmax(nullQF, extremePrecipQF, gapQF, sensorErrorQF, heaterErrorQF, na.rm = TRUE)]
  
  # Create 30-minute time groups
  stats_01min[, time_group := floor_date(startDateTime, "30 mins")]
  
  # Now aggregate the 1-minute data to 30-minute intervals
  stats_30min <- stats_01min[, .(
    startDateTime = min(startDateTime),
    endDateTime = max(endDateTime),
    precipBulk = sum(precipBulk, na.rm = TRUE),
    precipBulkExpUncert = sqrt(sum(precipBulkExpUncert^2, na.rm = TRUE)) * 2, # Quadrature sum with 2x multiplier
    precipNumPts = sum(precipNumPts, na.rm = TRUE), # Sum the counts from 1-minute intervals
    nullQF = ifelse(mean(nullQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(nullQF, na.rm = TRUE))),
    extremePrecipQF = ifelse(mean(extremePrecipQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(extremePrecipQF, na.rm = TRUE))),
    gapQF = ifelse(mean(gapQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(gapQF, na.rm = TRUE))),
    sensorErrorQF = ifelse(mean(sensorErrorQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(sensorErrorQF, na.rm = TRUE))),
    validCalQF = ifelse(mean(validCalQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(validCalQF, na.rm = TRUE))),
    suspectCalQF = ifelse(mean(suspectCalQF == 1, na.rm = TRUE) >= 0.1, 1L, as.integer(min(suspectCalQF, na.rm = TRUE))),
    # Special heater error QF at 50% 
    heaterErrorQF = ifelse(mean(heaterErrorQF == 1, na.rm = TRUE) >= 0.5, 1L, as.integer(min(heaterErrorQF, na.rm = TRUE)))
  ), by = time_group]
  
  # Update finalQF based on the aggregated flags
  stats_30min[, finalQF := pmax(nullQF, extremePrecipQF, gapQF, sensorErrorQF, heaterErrorQF, validCalQF, suspectCalQF, na.rm = TRUE)]
  
  # Clean up
  stats_01min[, time_group := NULL]
  stats_30min[, time_group := NULL]
  # Clean up
  stats_01min[, startDateTime_1min := NULL]
  stats_30min[, startDateTime_1min := NULL]
  
  # Clean up
  stats_01min[, endDateTime_1min := NULL]
  stats_30min[, endDateTime_1min := NULL]
  
 # Reorder columns to match schema requirements
  col_order <- c('startDateTime', 'endDateTime', 'precipBulk', 'precipBulkExpUncert', 'precipNumPts',
                 'nullQF', 'gapQF', 'extremePrecipQF', 'heaterErrorQF',
                 'sensorErrorQF', 'validCalQF', 'suspectCalQF', 'finalQF')
  
  stats_aggr01 <- stats_01min[, ..col_order]
  stats_aggr30 <- stats_30min[, ..col_order]
  
  # Convert back to data.frame if needed for writing
  setDF(stats_aggr01)
  setDF(stats_aggr30)
  
  # Write output files
  write_output_files(stats_aggr01, stats_aggr30, files$data, dirOutStat, SchmData, log)
  
  return()
}

# Helper function for file writing
write_output_files <- function(stats_01, stats_30, fileData, dirOutStat, SchmData, log) {
  if (is.na(fileData)) return()
  
  # Create output filenames
  nameFileIdxSplt <- strsplit(fileData, '.', fixed = TRUE)[[1]]
  base_name <- paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt) - 1)], collapse = '.')
  extension <- utils::tail(nameFileIdxSplt, 1)
  
  file_names <- c(
    paste0(base_name, '_stats_001.', extension),
    paste0(base_name, '_stats_030.', extension)
  )
  
  file_paths <- fs::path(dirOutStat, file_names)
  datasets <- list(stats_01, stats_30)
  
  # Write files
  for (i in seq_along(file_paths)) {
    rptWrte <- base::try(
      NEONprocIS.base::def.wrte.parq(
        data = datasets[[i]],
        NameFile = file_paths[i],
        NameFileSchm = NULL,
        Schm = SchmData,
        log = log
      ),
      silent = TRUE
    )
    
    if ('try-error' %in% base::class(rptWrte)) {
      log$error(base::paste0('Cannot write output to ', file_paths[i]))
      stop()
    } else {
      interval <- ifelse(i == 1, '1 min', '30 min')
      log$info(base::paste0('Wrote ', interval, ' precipitation to file ', file_paths[i]))
    }
  }
}
