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
##############################################################################################
wrap.precip.bucket <- function(DirIn,
                               DirOutBase,
                               SchmData = NULL,
                               DirSubCopy = NULL,
                               log = NULL
) {
  
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Gather info about the input directory and create the output directory.
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn, log = log)
  dirInData <- fs::path(DirIn, 'data')
  dirInQf <- fs::path(DirIn, 'flags')
  dirInThsh <-  fs::path(DirIn, 'threshold')
  dirInLoc <- fs::path(DirIn, 'location')
  dirUcrtCoef <- fs::path(DirIn, 'uncertainty_coef')
  
  dirOut <- fs::path(DirOutBase, InfoDirIn$dirRepo)
  dirOutStat <- fs::path(dirOut, 'stats')
  
  
  NEONprocIS.base::def.dir.crea(
    DirBgn = dirOut,
    DirSub = c('stats'),
    log = log
  )
  
  
  # Copy with a symbolic link the desired subfolders
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy, c('stats')))
  if (base::length(DirSubCopy) > 0) {
    
    NEONprocIS.base::def.dir.copy.symb(
      DirSrc = fs::path(DirIn, DirSubCopy),
      DirDest = dirOut,
      LnkSubObj = FALSE,
      log = log
    )
  }
  
  
  #grab files for cal, data, thresholds
  fileData <- base::list.files(dirInData, pattern = '.parquet', full.names = FALSE)
  fileQfCal <- base::list.files(dirInQf, pattern = 'flagsCal.parquet', full.names = FALSE)
  fileThsh <- base::list.files(dirInThsh, pattern = 'threshold', full.names = FALSE)
  
  # Read in the thresholds file (read first file only, there should only be 1)
  if (base::length(fileThsh) > 1) {
    log$debug(base::paste0('threshold files are ', fileThsh))
    fileThsh <- fileThsh[1]
    log$info(base::paste0('There is more than one threshold file in ', dirInThsh, '. Using ', fileThsh))
  }
  log$info(base::paste0(dirInThsh, '/', fileThsh))
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = base::paste0(dirInThsh, '/', fileThsh))
  
  # Verify that the term(s) needed in the input parameters are included in the threshold files
  termTest <- "precipBulk"
  exstThsh <- termTest %in% base::unique(thsh$term_name) # Do the terms exist in the thresholds
  if (base::sum(exstThsh) != base::length(termTest)) {
    log$error(base::paste0('Thresholds for term(s): ', base::paste(termTest[!exstThsh], collapse = ','), ' do not exist in the thresholds file. Cannot proceed.'))
    stop()
  }
  # Assign thresholds
  
  ###########TODO temporary sub in of names remove after testing is complete
  
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 1"] <- "inactiveHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 2"] <- "baseHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 1"] <- "extremePrecipQF"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 2"] <- "funnelHeater"
  
  
  ######################
  
  # thresholds to variables
  thshIdxTerm <- thsh[thsh$term_name == termTest, ]
  inactiveHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "inactiveHeater"]
  baseHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "baseHeater"]
  extremePrecipQF <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "extremePrecipQF"]
  funnelHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "funnelHeater"]
  
  #verify that the thresholds are all there
  ThshList <- c(inactiveHeater, baseHeater, extremePrecipQF, funnelHeater)
  
  if (length(ThshList) < nrow(thsh)) {
    log$error(base::paste0('Not all Thresholds specified for term(s): ', base::paste(termTest[!exstThsh], collapse = ','), ' do not exist in the thresholds file. Cannot proceed.'))
    stop()
  }
  
  #read in location information
  fileLoc <- base::dir(dirInLoc)
  
  # If there is no location file, skip
  numFileLoc <- base::length(fileLoc)
  if (numFileLoc == 0) {
    log$error(base::paste0('No location data in ', dirInLoc, '. Skipping...'))
    stop()
  }
  
  # If there is more than one location file, use the first
  if (numFileLoc > 1) {
    log$warn(base::paste0('There is more than one location file in ', dirInLoc, '. Using the first... (', fileLoc[1], ')'))
    fileLoc <- fileLoc[1]
  }
  
  # Load in the location json
  loc <- NEONprocIS.base::def.loc.meta(NameFile = base::paste0(dirInLoc, '/', fileLoc))
  
  # Read the datasets
  data <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dirInData, fileData),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  qfCal <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dirInQf, fileQfCal),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  #combine into one file
  data <- full_join(data, qfCal, by = 'readout_time')
  
  
  # pull in UCRT file
  fileUcrt <- base::dir(dirUcrtCoef)
  
  if (base::length(fileUcrt) != 1) {
    log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ", dirUcrtCoef, "... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
    ucrtCoef <- base::list()
  } else {
    nameFileUcrt <- base::paste0(dirUcrtCoef, '/', fileUcrt) # Full path to file
  }
  # Open the uncertainty file
  ucrtCoef <- base::try(rjson::fromJSON(file = nameFileUcrt, simplify = TRUE), silent = FALSE)
  if (base::class(ucrtCoef) == 'try-error') {
    # Generate error and stop execution
    log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.'))
    stop()
  }
  
  ucrtCoef_df <- dplyr::bind_rows(ucrtCoef)
  uCvalA1 <- as.numeric(ucrtCoef_df$Value[ucrtCoef_df$Name == "U_CVALA1"])
  
  ####testing multiple calibration coefficients.
  # TODO delete after talk with cove
  # ucrtCoef_df <- dplyr::bind_rows(ucrtCoef, ucrtCoef)
  # ucrtCoef_df$calibration_id[1:6] <- 123456
  # ucrtCoef_df$end_date[1:6] <- '2025-05-31T12:00:00.000Z'
  # ucrtCoef_df$start_date[7:12] <- '2025-05-31T12:00:00.000Z'
  # ucrtCoef_df$Value[6] <- 1
  # uCvalA1 <- as.numeric(ucrtCoef_df$Value[ucrtCoef_df$Name == "U_CVALA1"])
  
  #if more than one relevant calibration, grab correct uncertainty coefficients from CVAL based on time. 
  if (length(uCvalA1) < 1 & all(is.na(data$precipitation))) {
    log$info('No precipitation recorded, uncertainty data will not be calculated.')
    data$uCvalA1 <- 0
  } else if (length(uCvalA1) < 1 ) {
    # Generate error and stop execution
    log$error('Uncertainty value uCvalA1 necessary and not available')
    stop()
  } else if (length(uCvalA1) > 1) {
    # Adjust to finding more than one valid cal in a day.
    log$info(base::paste0('More than one calibration for date ', InfoDirIn$time, ' applying multiple coefficients.'))
    cals <- ucrtCoef_df[ucrtCoef_df$Name == "U_CVALA1",]
    cals$start_date <-  as.POSIXct(cals$start_date, format = "%Y-%m-%dT%H:%M:%OS", tz = 'GMT')
    cals$end_date <- as.POSIXct(cals$end_date, format = "%Y-%m-%dT%H:%M:%OS", tz = 'GMT')
    
    # Initialize uCvalA1 in data with NA
    data <- data %>%
      mutate(uCvalA1 = NA_real_)
    
    # Sort cals by start_date 
    cals <- cals[order(cals$start_date), ]
    
    # Iterate through each calibration entry and apply the coefficient
    for (i in 1:nrow(cals)){
      log$info(paste0('Grabbing calibration coefficient for calibration id ', cals$calibration_id[i]))
      current_value <- as.numeric(cals$Value[i])
      current_start_date <- cals$start_date[i]
      current_end_date <- cals$end_date[i]
      
      data <- data %>%
        mutate(uCvalA1 = dplyr::if_else(
          readout_time >= current_start_date & readout_time <= current_end_date,
          current_value,
          uCvalA1 # Keep existing value if not within the current range
        ))
    }
  } else {
    data$uCvalA1 <- uCvalA1
  }

  #Convert all NA values to 0, no data = no rain.
  data$precipitation[is.na(data$precipitation)] <- 0
  
  ########## Data where no rain was recorded will have a -1 var for valid Cal and suspect Cals
  data$validCalQF[is.na(data$validCalQF)] <- -1
  data$suspectCalQF[is.na(data$suspectCalQF)] <- -1
  
  #if throughfall sensor, downscale precip and UCRT based on area
  
  if (grepl(loc$context, pattern = "throughfall")) {
    # convert throughfall values (based on context)
    A_b <- 32429 # surface area of tipping bucket 
    A_t <- 251400 #surface area of throughfall collector
    tf_conv <- A_b / A_t #Multiplier for precip conversion.
    data$precipitation <- data$precipitation * tf_conv
    #use tip and area conversions to calculate uncertainties
    data$uThPTi <- data$uCvalA1 * data$precipitation
    data$uAtPTi <- 0.01 * data$precipitation #In ATBD and java code
    data$combinedUcrt <- sqrt(data$uThPTi^2 + data$uAtPTi^2)
    log$debug(base::paste0('Throughfall sensor detected at ', loc$name, ' applying area conversion'))
  } else {
    data$combinedUcrt <- data$uCvalA1 * data$precipitation
  }
  
  # Apply heater QMs If throughfall or non heatead should be NA
  
  data <- data %>%
    dplyr::mutate(
      precipHeaterQF = dplyr::case_when(
        heater_current < inactiveHeater ~ 0,
        heater_current >= inactiveHeater & heater_current < baseHeater ~ 1,
        heater_current >= baseHeater & heater_current <= funnelHeater ~ 2,
        heater_current > funnelHeater ~ 3,
        TRUE ~ NA #should handle a missing stream in TFs
      )
    )
  
  # Aggregate to 1 minute and apply extremePrecip and heaterQF Flags
  stats_aggr01 <- data %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(readout_time, '1 minute')) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(readout_time, '1 minute', change_on_boundary = T)) %>%
    dplyr::group_by(startDateTime, endDateTime) %>%
    dplyr::summarise(
      precipBulk = base::sum(precipitation),
      precipBulkExpUncert = base::sqrt(base::sum(combinedUcrt^2)) * 2, #keep NAs so that it will NA with lack of Coeff 
      precipHeater0QM = base::round(base::length(base::which(precipHeaterQF == 0)) / dplyr::n() * 100, 0),
      precipHeater1QM = base::round(base::length(base::which(precipHeaterQF == 1)) / dplyr::n() * 100, 0),
      precipHeater2QM = base::round(base::length(base::which(precipHeaterQF == 2)) / dplyr::n() * 100, 0),
      precipHeater3QM = base::round(base::length(base::which(precipHeaterQF == 3)) / dplyr::n() * 100, 0),
      validCalQF = base::max(validCalQF, na.rm = T),
      suspectCalQF = base::max(validCalQF, na.rm = T)
    ) %>%
    dplyr::mutate(extremePrecipQF = dplyr::case_when(
      precipBulk >= extremePrecipQF ~ 1,
      precipBulk < extremePrecipQF ~ 0,
      TRUE ~ -1
    )) %>%
    #flag data with finalQF if extreme precip observed.
    dplyr::mutate(finalQF = dplyr::case_when(
      extremePrecipQF == 1 ~ 1,
      TRUE ~ 0
    )) %>%
    dplyr::mutate(finalQF = as.integer(finalQF)) #make integer
  
  
  #aggregate one minute data further to 30 minute, taking the max calue for flags, the mean for QMs
  stats_aggr30 <- stats_aggr01 %>%
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, '30 minute')) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(endDateTime, '30 minute')) %>%
    dplyr::group_by(startDateTime, endDateTime) %>%
    dplyr::summarise(
      precipBulk = base::sum(precipBulk, na.rm = T),
      precipBulkExpUncert = base::sqrt(base::sum((precipBulkExpUncert / 2)^2)) * 2,
      precipHeater0QM = base::round(base::mean(precipHeater0QM, na.rm = T), 0),
      precipHeater1QM = base::round(base::mean(precipHeater1QM, na.rm = T), 0),
      precipHeater2QM = base::round(base::mean(precipHeater2QM, na.rm = T), 0),
      precipHeater3QM = base::round(base::mean(precipHeater3QM, na.rm = T), 0),
      validCalQF = base::max(validCalQF, na.rm = T),
      suspectCalQF = base::max(validCalQF, na.rm = T),
      extremePrecipQF = base::max(extremePrecipQF, na.rm = T),
      finalQF = as.integer(max(finalQF, na.rm = T))
    )
  
  ### reorder data. for schemas
  stats_aggr01 <- stats_aggr01 %>% dplyr::select(
    startDateTime, endDateTime, precipBulk, precipBulkExpUncert,
    precipHeater0QM, precipHeater1QM, precipHeater2QM, precipHeater3QM,
    validCalQF, suspectCalQF, extremePrecipQF, finalQF
  )
  
  stats_aggr30 <- stats_aggr30 %>% dplyr::select(
    startDateTime, endDateTime, precipBulk, precipBulkExpUncert,
    precipHeater0QM, precipHeater1QM, precipHeater2QM, precipHeater3QM,
    validCalQF, suspectCalQF, extremePrecipQF, finalQF
  )
  
  # Write out the file for this aggregation interval.
  nameFileIdx <- fileData
  if (!is.na(nameFileIdx)) {
    
    # Append the center date to the end of the file name to know where it came from
    nameFileIdxSplt <- base::strsplit(nameFileIdx, '.', fixed = TRUE)[[1]]
    nameFileStatIdxSplt001 <- c(
      paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt) - 1)],
             '_stats_001'),
      utils::tail(nameFileIdxSplt, 1)
    )
    nameFileStatIdxSplt030 <- c(
      paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt) - 1)],
             '_stats_030'),
      utils::tail(nameFileIdxSplt, 1)
    )
    nameFileStatOut001Idx <- base::paste0(nameFileStatIdxSplt001, collapse = '.')
    nameFileStatOut030Idx <- base::paste0(nameFileStatIdxSplt030, collapse = '.')
    
    # Write out the data to file
    fileStatOut001Idx <- fs::path(dirOutStat, nameFileStatOut001Idx)
    fileStatOut030Idx <- fs::path(dirOutStat, nameFileStatOut030Idx)
    
    rptWrte <-
      base::try(NEONprocIS.base::def.wrte.parq(
        data = stats_aggr01,
        NameFile = fileStatOut001Idx,
        NameFileSchm = NULL,
        Schm = SchmData,
        log = log
      ),
      silent = TRUE
      )
    if ('try-error' %in% base::class(rptWrte)) {
      log$error(base::paste0(
        'Cannot write output to ',
        fileStatOut001Idx,
        '. ',
        attr(rptWrte, "condition")
      ))
      stop()
    } else {
      log$info(base::paste0(
        'Wrote 1 min precipitation to file ',
        fileStatOut001Idx
      ))
    }
    
    rptWrte <-
      base::try(NEONprocIS.base::def.wrte.parq(
        data = stats_aggr30,
        NameFile = fileStatOut030Idx,
        NameFileSchm = NULL,
        Schm = SchmData,
        log = log
      ),
      silent = TRUE
      )
    if ('try-error' %in% base::class(rptWrte)) {
      log$error(base::paste0(
        'Cannot write output to ',
        fileStatOut030Idx,
        '. ',
        attr(rptWrte, "condition")
      ))
      stop()
    } else {
      log$info(base::paste0(
        'Wrote 30 min precipitation to file ',
        fileStatOut030Idx
      ))
    }
  }
  return()
}


