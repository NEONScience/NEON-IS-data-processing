##############################################################################################
#' @title Calculate temperature test flags for soil moisture data

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Calculate temperature-based quality flags by comparing soil temperature to uncertainty.
#' If primary sensor is flagged, uses average of neighboring sensors.

#' @param sensorInfo List containing 'closest' sensor and 'neighbors' list (from def.find.temp.sensor)
#' @param targetDepth Numeric. The target depth (in meters) for distance validation
#' @param distThreshold Numeric. Maximum allowed distance (in meters) between sensor and target depth
#' @param log A logger object. Defaults to NULL.

#' @return Data frame with columns:
#' \describe{
#'   \item{startDateTime}{Start time of measurement interval}
#'   \item{endDateTime}{End time of measurement interval}
#'   \item{temp_flag}{Flag value: 0=pass, 1=fail, -1=test not run}
#' }

#' @references
#' License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-17)
#'     original creation
##############################################################################################
def.calc.temp.flags <- function(sensorInfo,
                                targetDepth = NULL,
                                distThreshold = NULL,
                                log = NULL) {
  
  # Initialize log if not provided
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Constants
  QF_PASS <- 0L
  QF_FAIL <- 1L
  QF_NA <- -1L
  TEMP_DIFF_THRESHOLD <- 1  # degrees
  
  # Read primary sensor data
  closestSensor <- sensorInfo$closest
  tempDataClose <- NEONprocIS.base::def.read.parq(closestSensor$data_path)
  
  # Initialize output data frame
  tempData <- tempDataClose[, c('startDateTime', 'endDateTime')]
  tempData$temp_flag <- NA_integer_
  
  # Check if primary sensor has good data (finalQF == 0)
  idxGood <- !base::is.na(tempDataClose$finalQF) & tempDataClose$finalQF == QF_PASS
  
  if (base::all(idxGood)) {
    # All primary sensor data is good - use it directly
    log$debug(base::paste0('Using primary sensor ', closestSensor$sensor_id, ' (all data good)'))
    tempData$temp_flag <- base::as.integer(
      tempDataClose$soilTempMean < tempDataClose$soilTempExpUncert
    )
    
  } else {
    # Some primary data is flagged - need to use neighbors for those intervals
    log$debug(base::paste0('Primary sensor has flagged data, attempting neighbor average'))
    
    # Use primary sensor where data is good
    tempData$temp_flag[idxGood] <- base::as.integer(
      tempDataClose$soilTempMean[idxGood] < tempDataClose$soilTempExpUncert[idxGood]
    )
    
    # Try to use neighbor average for flagged intervals
    nextHigher <- sensorInfo$neighbors$higher
    nextLower <- sensorInfo$neighbors$lower
    
    # Check if neighbors exist and are within acceptable distance
    useNeighbors <- FALSE
    if (!base::is.null(nextHigher) && !base::is.null(nextLower) && !base::is.null(targetDepth) && !base::is.null(distThreshold)) {
      distHigher <- base::abs(targetDepth - nextHigher$depth_m)
      distLower <- base::abs(targetDepth - nextLower$depth_m)
      
      if (distHigher > distThreshold || distLower > distThreshold) {
        log$warn(base::paste0('Neighbor sensors exceed ', distThreshold, 'm distance threshold. ',
                              'Higher: ', base::round(distHigher, 3), 'm, ',
                              'Lower: ', base::round(distLower, 3), 'm. ',
                              'Skipping neighbor averaging.'))
      } else {
        useNeighbors <- TRUE
      }
    } else if (!base::is.null(nextHigher) && !base::is.null(nextLower)) {
      # targetDepth or distThreshold not provided, proceed without distance check (legacy behavior)
      useNeighbors <- TRUE
    }
    
    if (useNeighbors) {
      # Read neighbor data
      tempDataHigher <- NEONprocIS.base::def.read.parq(nextHigher$data_path)
      tempDataLower <- NEONprocIS.base::def.read.parq(nextLower$data_path)
      
      # Filter for good data only (finalQF < 1)
      tempDataHigher <- tempDataHigher[tempDataHigher$finalQF < 1, ]
      tempDataLower <- tempDataLower[tempDataLower$finalQF < 1, ]
      
      # Calculate test statistic for each neighbor
      tempDataLower$zeroCheckLow <- tempDataLower$soilTempMean - tempDataLower$soilTempExpUncert
      tempDataHigher$zeroCheckHigh <- tempDataHigher$soilTempMean - tempDataHigher$soilTempExpUncert
      
      # Join neighbor data
      tempDataJoin <- base::merge(
        tempDataLower[, c("startDateTime", "endDateTime", "zeroCheckLow")],
        tempDataHigher[, c("startDateTime", "endDateTime", "zeroCheckHigh")],
        by = c("startDateTime", "endDateTime"),
        all = TRUE
      )
      
      # Calculate average of neighbor checks
      tempDataJoin$avgZeroCheck <- base::rowMeans(
        base::cbind(tempDataJoin$zeroCheckLow, tempDataJoin$zeroCheckHigh),
        na.rm = TRUE
      )
      tempDataJoin$avgZeroCheck[base::is.nan(tempDataJoin$avgZeroCheck)] <- NA_real_
      
      # Test if average is less than threshold
      tempDataJoin$zeroCheck <- base::ifelse(
        base::is.na(tempDataJoin$avgZeroCheck),
        NA_integer_,
        base::as.integer(tempDataJoin$avgZeroCheck < TEMP_DIFF_THRESHOLD)
      )
      
      # Merge with primary data and fill in gaps
      tempData <- base::merge(
        tempData,
        tempDataJoin[, c("startDateTime", "endDateTime", "zeroCheck")],
        by = c("startDateTime", "endDateTime"),
        all.x = TRUE
      )
      
      # Use neighbor check where primary flag is NA
      idxNeedNeighbor <- base::is.na(tempData$temp_flag)
      tempData$temp_flag[idxNeedNeighbor] <- tempData$zeroCheck[idxNeedNeighbor]
      tempData$zeroCheck <- NULL
      
      log$debug(base::paste0('Filled ', base::sum(idxNeedNeighbor & !base::is.na(tempData$temp_flag)), 
                             ' intervals using neighbor average'))
    } else {
      log$warn('Insufficient neighbor sensors to calculate backup flags')
    }
  }
  
  # Set remaining NA values to -1 (test could not be run)
  tempData$temp_flag[base::is.na(tempData$temp_flag)] <- QF_NA
  
  return(tempData)
}
