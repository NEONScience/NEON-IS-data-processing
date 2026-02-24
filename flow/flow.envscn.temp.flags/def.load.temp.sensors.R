##############################################################################################
#' @title Load temperature sensor metadata and data paths

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org}

#' @description
#' Extract temperature sensor IDs, depths, and file paths from a directory structure.
#' Parses location files to extract z_offset (depth) and matches with corresponding data files.

#' @param DirTemp Character value. Path to directory containing temperature sensor data with
#' subdirectories structured as temp-soil_SENSORID/data/ and temp-soil_SENSORID/location/

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL.

#' @return A data frame with columns:
#' \describe{
#'   \item{sensor_id}{Sensor identifier (e.g., temp-soil_GRSM005501)}
#'   \item{depth_m}{Sensor depth in meters (negative = below surface)}
#'   \item{location_path}{Path to location JSON file}
#'   \item{data_path}{Path to data parquet file}
#' }

#' @references
#' License: GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2025-02-17)
#     original creation
##############################################################################################
def.load.temp.sensors <- function(DirTemp,
                                  log = NULL) {
  
  # Initialize log if not provided
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Find all temperature sensor files
  filesTemp <- base::list.files(DirTemp, full.names = TRUE, recursive = TRUE)
  
  # Filter for 1-minute data files and location JSON files
  filesTempData <- filesTemp[base::grepl("/temp-soil_[^/]+/data/[^/]+001\\.parquet$", filesTemp)]
  filesTempLocation <- filesTemp[base::grepl("/temp-soil_[^/]+/location/[^/]*_locations\\.json$", filesTemp)]
  
  if (base::length(filesTempData) == 0) {
    log$warn(base::paste0('No temperature data files found in ', DirTemp))
    return(data.frame(sensor_id = character(), depth_m = numeric(), 
                      location_path = character(), data_path = character()))
  }
  
  if (base::length(filesTempLocation) == 0) {
    log$warn(base::paste0('No temperature location files found in ', DirTemp, 
                          '. Temperature test will not run.'))
    return(data.frame(sensor_id = character(), depth_m = numeric(), 
                      location_path = character(), data_path = character()))
  }
  
  # Helper function to extract sensor ID from path
  extract_sensor_id <- function(paths) {
    matches <- base::regmatches(paths, base::regexpr("temp-soil_[^/]+", paths))
    return(matches)
  }
  
  # Helper function to extract z_offset from location JSON
  extract_z_offset <- function(path) {
    base::tryCatch({
      locData <- jsonlite::fromJSON(path, simplifyVector = FALSE)
      zOffset <- locData$features[[1]]$properties$locations$features[[1]]$properties$z_offset
      # Check if zOffset is NULL or has length 0
      if (base::is.null(zOffset) || base::length(zOffset) == 0) {
        return(NA_real_)
      }
      return(base::as.numeric(zOffset))
    }, error = function(e) {
      return(NA_real_)
    })
  }
  
  
  # Build location metadata table
  sensorIds <- extract_sensor_id(filesTempLocation)
  depths <- base::vapply(filesTempLocation, extract_z_offset, numeric(1))
  
  dfLocations <- data.frame(
    sensor_id = sensorIds,
    depth_m = depths,
    location_path = filesTempLocation,
    stringsAsFactors = FALSE
  )
  
  # Check for duplicate sensor_ids and average depths if found
  dupCheck <- base::table(dfLocations$sensor_id)
  dupSensors <- base::names(dupCheck)[dupCheck > 1]
  
  if (base::length(dupSensors) > 0) {
    log$warn(base::paste0('Multiple location files found for sensor(s): ',
                          base::paste(dupSensors, collapse = ', '),
                          '. Averaging depths.'))
    
    # Average depths for duplicate sensors
    avgDepths <- stats::aggregate(
      depth_m ~ sensor_id, 
      data = dfLocations, 
      FUN = function(x) base::mean(x, na.rm = TRUE)
    )
    firstPaths <- dfLocations[!base::duplicated(dfLocations$sensor_id), c('sensor_id', 'location_path')]
    dfLocations <- base::merge(avgDepths, firstPaths, by = 'sensor_id')
  } else {
    dfLocations <- dfLocations[!base::duplicated(dfLocations$sensor_id), ]
  }
  
  # Build data file table
  dataIds <- extract_sensor_id(filesTempData)
  dfData <- data.frame(
    sensor_id = dataIds,
    data_path = filesTempData,
    stringsAsFactors = FALSE
  )
  dfData <- dfData[!base::duplicated(dfData$sensor_id), ]
  
  # Join location and data information
  sensorDepthDf <- base::merge(dfLocations, dfData, by = "sensor_id", all = FALSE)
  
  # Validate that all depths were successfully extracted
  if (base::any(base::is.na(sensorDepthDf$depth_m))) {
    failedSensors <- sensorDepthDf$sensor_id[base::is.na(sensorDepthDf$depth_m)]
    log$warn(base::paste0('Failed to extract depth from location files for: ',
                           base::paste(failedSensors, collapse = ', ')))
  }
  
  log$info(base::paste0('Loaded ', base::nrow(sensorDepthDf), ' temperature sensors'))
  log$info(base::paste0('Temp sensors found: ', sensorDepthDf$sensor_id))
  
  return(sensorDepthDf)
}
