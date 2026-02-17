##############################################################################################
#' @title Apply temperature flags to high-frequency data

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Join minute-interval temperature flags to high-frequency (e.g., 10-second) soil moisture data
#' using time-based overlap matching.

#' @param dataSm Data frame containing soil moisture data with readout_time column
#' @param tempData Data frame with temperature flags (startDateTime, endDateTime, temp_flag)
#' @param qfColName Character. Name of the QF column to update in dataSm
#' @param log A logger object. Defaults to NULL.

#' @return Updated dataSm data frame with qfColName column populated with temperature flags

#' @references
#' License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-17)
#'     original creation
##############################################################################################
def.apply.temp.flags <- function(dataSm,
                                 tempData,
                                 qfColName,
                                 log = NULL) {
  
  # Initialize log if not provided
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Convert to data.table for efficient joins
  dtSm <- data.table::as.data.table(dataSm)
  dtTemp <- data.table::as.data.table(tempData)[, .(startDateTime, endDateTime, temp_flag)]
  
  # Ensure proper datetime format
  dtSm[, `:=`(
    readout_start = base::as.POSIXct(readout_time, tz = "UTC"),
    readout_end = base::as.POSIXct(readout_time, tz = "UTC")
  )]
  
  dtTemp[, `:=`(
    startDateTime = base::as.POSIXct(startDateTime, tz = "UTC"),
    endDateTime = base::as.POSIXct(endDateTime, tz = "UTC"),
    temp_flag = base::as.integer(temp_flag)
  )]
  
  # Set key for foverlaps (required on the interval table)
  data.table::setkey(dtTemp, startDateTime, endDateTime)
  
  # Perform overlap join - find which minute interval each point falls within
  joined <- data.table::foverlaps(
    x = dtSm[, .(readout_start, readout_end, .rows = .I)],
    y = dtTemp[, .(startDateTime, endDateTime, temp_flag)],
    by.x = c("readout_start", "readout_end"),
    by.y = c("startDateTime", "endDateTime"),
    type = "within",  # point must fall within interval
    nomatch = NA_integer_
  )
  
  # Get current QF values
  newQf <- dataSm[[qfColName]]
  
  # Update QF values where we found a matching interval
  hasMatch <- !base::is.na(joined$temp_flag)
  newQf[joined$.rows[hasMatch]] <- joined$temp_flag[hasMatch]
  
  # Assign back to original data frame
  dataSm[[qfColName]] <- newQf
  
  numMatched <- base::sum(hasMatch)
  numTotal <- base::nrow(dataSm)
  log$debug(base::paste0('Applied temperature flags: ', numMatched, ' / ', numTotal, 
                         ' rows matched (', base::round(100 * numMatched / numTotal, 1), '%)'))
  
  return(dataSm)
}
