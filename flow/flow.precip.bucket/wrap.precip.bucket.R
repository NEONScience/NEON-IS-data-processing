##############################################################################################
#' @title  Process data from metone370380 tipping bucket sensors to 1 minute and 30 minute 
#' aggragations

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Add 0s to periods where no precipitation recorded, process Throughfall area conversion
#' and uncertainty (as applicable, location context based), process heater QMs, aggregate to 1 minute and apply 
#' extremePrecipFlag and finalQF, aggregate to 30 minutes, including summed precipitation, extreme precip flags and 
#' uncertainty calculations 
#' 
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags
#'         /location
#'         /threshold
#'         /uncertainty_coeff


#' The flags folder holds one file containing calibration quality flags for 
#' the central processing day only. These files should respectively be named in the convention:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsCal.parquet
#' All other files in this directory will be ignored.
#'
#' The threshold folder contains a single file named thresholds.json that holds threshold parameters
#' applicable to the tipping buckets. 
#' 
#' The location folder holds location specific information so that throughfall specific area conversions can be applied
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
#' quality flags (incl. the final quality flag) produced by the custom tipping bucket algorithm, where DirOutBase 
#' replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory structure of the 
#' input path. The terminal directories for each datum include, at a minimum, 'stats' and 'location'. The stats folder
#' contains one minute and 30-minute output for the relevant day of data. It contains the precipitation sum, uncertainty estimates, quality flags specific
#'to the bucket algorithm, and final quality flag. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn='/scratch/pfs/precipBucket_thresh_select'
#' DirOutBase='/scratch/pfs/out_tb'


#' wrap.precip.bucket(DirIn,DirOutBase,DirSubCopy)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-06-10)
#     Initial creation
#   Teresa Burlingame  (2025-07-10)
#     Optimizations with AI assist and update ucrt for sec precip. 
##############################################################################################
wrap.precip.bucket <- function(DirIn,
                               DirOutBase,
                               SchmData = NULL,
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
  
  # Use list for path construction to enable $ operator
  dir_paths <- list(
    data = fs::path(DirIn, 'data'),
    flags = fs::path(DirIn, 'flags'), 
    threshold = fs::path(DirIn, 'threshold'),
    location = fs::path(DirIn, 'location'),
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
    flags = 'flagsCal.parquet',
    threshold = 'threshold'
  )
  
  files <- lapply(names(file_patterns), function(type) {
    base::list.files(dir_paths[[type]], pattern = file_patterns[[type]], full.names = FALSE)
  })
  names(files) <- names(file_patterns)
  
  # Read thresholds (optimized error handling)
  if (base::length(files$threshold) > 1) {
    log$debug(base::paste0('threshold files are ', paste(files$threshold, collapse = ', ')))
    files$threshold <- files$threshold[1]
    log$info(base::paste0('Using first threshold file: ', files$threshold))
  }
  
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(
    NameFile = fs::path(dir_paths$threshold, files$threshold)
  )
  
  # Verify terms exist (vectorized)
  termTest <- "precipBulk"
  if (!termTest %in% thsh$term_name) {
    log$error(base::paste0('Missing threshold term: ', termTest))
    stop()
  }
  
  # Get threshold values more efficiently using named indexing
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 1"] <- "InactiveHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 2"] <- "BaseHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 1"] <- "ExtremePrecipMax"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 2"] <- "FunnelHeater"
  
  # Extract thresholds using vectorized lookup
  thsh_subset <- thsh[thsh$term_name == termTest, ]
  threshold_lookup <- setNames(thsh_subset$number_value, thsh_subset$threshold_name)
  
  # Pre-allocate threshold variables
  thresholds <- list(
    inactiveHeater = threshold_lookup["InactiveHeater"],
    baseHeater = threshold_lookup["BaseHeater"],
    extremePrecipQF = threshold_lookup["ExtremePrecipMax"],
    funnelHeater = threshold_lookup["FunnelHeater"]
  )
  
  # Validate all thresholds exist
  if (any(is.na(thresholds))) {
    log$error("Missing threshold values")
    stop()
  }
  
  # Read location data
  fileLoc <- base::dir(dir_paths$location)
  if (length(fileLoc) == 0) {
    log$error(base::paste0('No location data in ', dir_paths$location))
    stop()
  }
  
  if (length(fileLoc) > 1) {
    log$warn(base::paste0('Multiple location files, using first: ', fileLoc[1]))
    fileLoc <- fileLoc[1]
  }
  
  loc <- NEONprocIS.base::def.loc.meta(NameFile = fs::path(dir_paths$location, fileLoc))
  
  # Read and merge datasets more efficiently
  data <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dir_paths$data, files$data),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  qfCal <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dir_paths$flags, files$flags),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  # Use data.table for faster join
  setDT(data)
  setDT(qfCal)
  data <- merge(data, qfCal, by = 'readout_time', all = TRUE)
  
  # Read uncertainty coefficients
  fileUcrt <- base::dir(dir_paths$uncertainty_coef)
  
  if (base::length(fileUcrt) != 1) {
    log$warn("No single uncertainty coefficient file found")
    ucrtCoef <- base::list()
  } else {
    nameFileUcrt <- fs::path(dir_paths$uncertainty_coef, fileUcrt)
    ucrtCoef <- base::try(rjson::fromJSON(file = nameFileUcrt, simplify = TRUE), silent = FALSE)
    if (base::class(ucrtCoef) == 'try-error') {
      log$error(base::paste0('Cannot read uncertainty file: ', nameFileUcrt))
      stop()
    }
  }
  
  # Process uncertainty coefficients more efficiently
  if (length(ucrtCoef) > 0) {
    ucrtCoef_df <- dplyr::bind_rows(ucrtCoef)
    uCvalA1_rows <- ucrtCoef_df$Name == "U_CVALA1"
    uCvalA1 <- as.numeric(ucrtCoef_df$Value[uCvalA1_rows])
    
    # Handle multiple calibrations more efficiently
    if (length(uCvalA1) > 1) {
      log$info('Applying multiple calibration coefficients')
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
    } else if (length(uCvalA1) == 1) {
      data[, uCvalA1 := uCvalA1]
    } else {
      data[, uCvalA1 := 0]
    }
  } else {
    data[, uCvalA1 := 0]
  }
  
  # Vectorized data cleaning
  data[is.na(precipitation), precipitation := 0]
  data[is.na(validCalQF), validCalQF := -1]
  data[is.na(suspectCalQF), suspectCalQF := -1]
  
  # Process throughfall conversion and uncertainty calculation
  is_throughfall <- grepl("throughfall", loc$context)
  if (is_throughfall) {
    A_b <- 32429  # surface area of tipping bucket
    A_t <- 251400 # surface area of throughfall collector
    tf_conv <- A_b / A_t # Multiplier for precip conversion
    
    data[, `:=`(
      precipitation = precipitation * tf_conv,
      uThPTi = uCvalA1 * precipitation * tf_conv,
      uAtPTi = 0.01 * precipitation * tf_conv,
      combinedUcrt = sqrt((uCvalA1 * precipitation * tf_conv)^2 + (0.01 * precipitation * tf_conv)^2)
    )]
    
    log$debug(base::paste0('Throughfall sensor detected at ', loc$name, ' applying area conversion'))
  } else {
    data[, combinedUcrt := uCvalA1 * precipitation]
  }
  
  # Vectorized heater QF assignment
  data[, precipHeaterQF := fcase(
    heater_current < thresholds$inactiveHeater, 0,
    heater_current < thresholds$baseHeater, 1,
    heater_current <= thresholds$funnelHeater, 2,
    heater_current > thresholds$funnelHeater, 3,
    default = NA_real_
  )]
  
  # More efficient aggregation using data.table
  data[, `:=`(
    startDateTime_1min = floor_date(readout_time, '1 minute'),
    endDateTime_1min = ceiling_date(readout_time, '1 minute', change_on_boundary = TRUE)
  )]
  
  # 1-minute aggregation with quadrature uncertainty calculation
  stats_aggr01 <- data[, .(
    precipBulk = sum(precipitation, na.rm = TRUE),
    precipBulkExpUncert = sqrt(sum(combinedUcrt^2, na.rm = TRUE)) * 2,  # Always use quadrature
    precipHeater0QM = round(sum(precipHeaterQF == 0, na.rm = TRUE) / .N * 100, 0),
    precipHeater1QM = round(sum(precipHeaterQF == 1, na.rm = TRUE) / .N * 100, 0),
    precipHeater2QM = round(sum(precipHeaterQF == 2, na.rm = TRUE) / .N * 100, 0),
    precipHeater3QM = round(sum(precipHeaterQF == 3, na.rm = TRUE) / .N * 100, 0),
    validCalQF = max(validCalQF, na.rm = TRUE),
    suspectCalQF = max(suspectCalQF, na.rm = TRUE)
  ), by = .(startDateTime = startDateTime_1min, endDateTime = endDateTime_1min)]
  
  # Add derived columns
  stats_aggr01[, `:=`(
    extremePrecipQF = fifelse(precipBulk >= thresholds$extremePrecipQF, 1L, 0L),
    finalQF = fifelse(precipBulk >= thresholds$extremePrecipQF, 1L, 0L)
  )]
  
  # 30-minute aggregation
  stats_aggr01[, `:=`(
    startDateTime_30min = floor_date(startDateTime, '30 minute'),
    endDateTime_30min = ceiling_date(endDateTime, '30 minute')
  )]
  
  # 30-minute aggregation with quadrature uncertainty calculation
  stats_aggr30 <- stats_aggr01[, .(
    precipBulk = sum(precipBulk, na.rm = TRUE),
    precipBulkExpUncert = sqrt(sum((precipBulkExpUncert / 2)^2, na.rm = TRUE)) * 2,  # Always use quadrature
    precipHeater0QM = round(mean(precipHeater0QM, na.rm = TRUE), 0),
    precipHeater1QM = round(mean(precipHeater1QM, na.rm = TRUE), 0),
    precipHeater2QM = round(mean(precipHeater2QM, na.rm = TRUE), 0),
    precipHeater3QM = round(mean(precipHeater3QM, na.rm = TRUE), 0),
    validCalQF = max(validCalQF, na.rm = TRUE),
    suspectCalQF = max(suspectCalQF, na.rm = TRUE),
    extremePrecipQF = max(extremePrecipQF, na.rm = TRUE),
    finalQF = as.integer(max(finalQF, na.rm = TRUE))
  ), by = .(startDateTime = startDateTime_30min, endDateTime = endDateTime_30min)]
  
  # Clean up temporary columns and reorder
  stats_aggr01[, c('startDateTime_30min', 'endDateTime_30min') := NULL]
  
  # Reorder columns to match schema requirements
  col_order <- c('startDateTime', 'endDateTime', 'precipBulk', 'precipBulkExpUncert',
                 'precipHeater0QM', 'precipHeater1QM', 'precipHeater2QM', 'precipHeater3QM',
                 'validCalQF', 'suspectCalQF', 'extremePrecipQF', 'finalQF')
  
  stats_aggr01 <- stats_aggr01[, ..col_order]
  stats_aggr30 <- stats_aggr30[, ..col_order]
  
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
