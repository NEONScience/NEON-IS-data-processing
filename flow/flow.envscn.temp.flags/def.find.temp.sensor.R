##############################################################################################
#' @title Find closest temperature sensor to target depth

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Identify the temperature sensor with depth closest to the target depth.
#' In case of tie, prefers shallower (less negative/more positive) depth.

#' @param targetDepth Numeric value. Target depth in meters (negative = below surface)
#' @param sensorDepthDf Data frame with columns sensor_id, depth_m, data_path, location_path
#' @param log A logger object. Defaults to NULL.

#' @return A list with two elements:
#' \describe{
#'   \item{closest}{Data frame row for the closest sensor}
#'   \item{neighbors}{List with elements 'higher' and 'lower' containing neighbor sensor info}
#' }

#' @references
#' License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-17)
#'     original creation
##############################################################################################
def.find.temp.sensor <- function(targetDepth,
                                 sensorDepthDf,
                                 log = NULL) {
  
  # Initialize log if not provided
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Filter out sensors with missing depths
  validSensors <- sensorDepthDf[!base::is.na(sensorDepthDf$depth_m), ]
  
  if (base::nrow(validSensors) == 0) {
    log$error('No valid temperature sensors with depth information')
    stop()
  }
  
  # Calculate absolute difference from target depth
  validSensors$abs_diff <- base::abs(validSensors$depth_m - targetDepth)
  
  # Sort by: 1) smallest difference, 2) shallowest depth, 3) sensor_id (for stability)
  validSensors <- validSensors[base::order(validSensors$abs_diff, 
                                           -validSensors$depth_m, 
                                           validSensors$sensor_id), ]
  
  # Select closest sensor
  closestSensor <- validSensors[1, c("sensor_id", "depth_m", "data_path", "location_path")]
  closestDepth <- closestSensor$depth_m
  
  # Find neighbor sensors (one shallower, one deeper)
  # Shallower = greater depth_m (less negative)
  higherSensors <- validSensors[validSensors$depth_m > closestDepth, ]
  if (base::nrow(higherSensors) > 0) {
    higherSensors <- higherSensors[base::order(higherSensors$depth_m, higherSensors$sensor_id), ]
    nextHigher <- higherSensors[1, c("sensor_id", "depth_m", "data_path", "location_path")]
  } else {
    nextHigher <- NULL
  }
  
  # Deeper = smaller depth_m (more negative)
  lowerSensors <- validSensors[validSensors$depth_m < closestDepth, ]
  if (base::nrow(lowerSensors) > 0) {
    lowerSensors <- lowerSensors[base::order(-lowerSensors$depth_m, lowerSensors$sensor_id), ]
    nextLower <- lowerSensors[1, c("sensor_id", "depth_m", "data_path", "location_path")]
  } else {
    nextLower <- NULL
  }
  
  log$debug(base::paste0('Closest sensor to depth ', targetDepth, 'm: ', 
                         closestSensor$sensor_id, ' (', closestDepth, 'm)'))
  
  return(base::list(
    closest = closestSensor,
    neighbors = base::list(
      higher = nextHigher,
      lower = nextLower
    )
  ))
}
