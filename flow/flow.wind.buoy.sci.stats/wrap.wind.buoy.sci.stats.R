##############################################################################################
#' @title Wrapper for buoy wind SCI statistics

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply SCI statistics to buoy wind data.

#'
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility flags and quality flag thresholds.
#'  
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param WndwAgr Character value. The window aggregation period for the buoy wind data (e.g., "002" for 2-minute averages, "030" for 30-minute averages).
#' 
#' @param SchmDataOut (optional), A json-formatted character string containing the schema for the data file.
#' This should be the same for the input as the output.  Only the number of rows of measurements should change. 
#' 
#' @param SchmFlagsOut (optional), A json-formatted character string containing the schema for the output flags. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Buoy wind data file and combined flag file in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#'                                                                                                                                                                                          
#' @changelog
#' Nora Catolico (2026-07-13)
#' Initial creation
##############################################################################################
wrap.wind.buoy.sci.stats <- function(DirIn,
                                  DirOutBase,
                                  WndwAgr,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirIn_rmyoung <- paste0(DirIn,"/rmyoung")
  DirIn_hmr3300 <- paste0(DirIn,"/hmr3300")

  #list directories
  allDir_rmyoung <- base::list.dirs(path = DirIn_rmyoung, full.names = TRUE, 
        recursive = TRUE)
  allDir_hmr3300 <- base::list.dirs(path = DirIn_hmr3300, full.names = TRUE, 
        recursive = TRUE)

  # Extract CFGLOC IDs
  config <- base::unique(
    sub("^.*\\/(CFGLOC[^\\/]*)($|\\/.*)$", "\\1",
    grep("(^|/)CFGLOC[^/]*($|/)", allDir_rmyoung, value = TRUE))
  )  

  DirInData_rmyoung <- paste0(DirIn,"/rmyoung/",config[1],"/data")
  DirInData_hmr3300 <- paste0(DirIn,"/hmr3300/",config[1],"/data")
  DirInFlags_rmyoung <- paste0(DirIn,"/rmyoung/",config[1],"/flags")
  DirInThresholds_hmr3300 <- paste0(DirIn,"/hmr3300/",config[1],"/threshold")
  DirInLocations_rmyoung <- paste0(DirIn,"/rmyoung/",config[1],"/location")
  
  #create output directories
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData_rmyoung <- base::paste0(DirOut,"/rmyoung/",config[1],"/data")
  DirOutFlags_rmyoung <- base::paste0(DirOut,"/rmyoung/",config[1],"/flags") 
  base::dir.create(DirOutData_rmyoung,recursive=TRUE)
  base::dir.create(DirOutFlags_rmyoung,recursive=TRUE)
  
  #' Read in parquet file of buoy wind data.
  dataFileName_rmyoung<-base::list.files(DirInData_rmyoung,full.names=FALSE)
  if(length(dataFileName_rmyoung)==0){
    log$error(base::paste0('Data file not found in ', DirInData_rmyoung)) 
    stop()
  } else {
    data_rmyoung<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData_rmyoung, '/', dataFileName_rmyoung),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',dataFileName_rmyoung))
  }
  dataFileName_hmr3300<-base::list.files(DirInData_hmr3300,full.names=FALSE)
  if(length(dataFileName_hmr3300)==0){
    log$debug(base::paste0('HMR3300 file not found in ', DirInData_hmr3300))
  } else {
    data_hmr3300<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData_hmr3300, '/', dataFileName_hmr3300),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',dataFileName_hmr3300))
  }
  flagFileName_rmyoung<-base::list.files(DirInFlags_rmyoung,full.names=FALSE)
  if(length(flagFileName_rmyoung)==0){
    log$error(base::paste0('Flag file not found in ', DirInFlags_rmyoung)) 
    stop()
  } else {
    flags_rmyoung<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags_rmyoung, '/', flagFileName_rmyoung),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',flagFileName_rmyoung))
  }


  
  

  # Merge the buoy compass adjusted direction with the rmyoung data based on readout_time
  wind_data <- merge(data_rmyoung, data_hmr3300[, c("readout_time", "compass_direction_adjusted")], by="readout_time", all=TRUE)

  #The wind direction measurements corrected by buoy compass data is calculated by summing the uncorrected 
  #but calibrated wind direction measurements, the declination-adjusted compass measurements,
  #and the wind-monitor on-mast offset (azimuth) from the Named Location Database
  wind_data$direction_corrected <- (wind_data$direction_calibrated + wind_data$compass_direction_adjusted + wind_data$azimuth) %% 360

  ###############
  #5. set to 0 when no wind
  wind_data$direction_corrected[wind_data$speed_calibrated == 0] <- 0

  ###############
  #6. Unit-vector mean wind direction must be converted from degrees to radians,
  #according to the meteorological coordinate system 
  wind_data$direction_corrected_rad <- wind_data$direction_corrected * (pi / 180)

  ###############
  #7. Calculate the mean and variance with analytical two-pass method
  #In the first pass, the components of the average distance vector over an observation period with sample size n are calculated 
  # The first window begins at the nearest whole minute <= first timestamp in the series.
  anchor_time <- lubridate::floor_date(base::min(wind_data_avg$readout_time, na.rm = TRUE), unit = 'minute')

  safe_mean <- function(x) {
    if (base::all(base::is.na(x))) {
      return(NA_real_)
    }
    base::mean(x, na.rm = TRUE)
  }

  # Build anchored window starts.
  dt_secs <- as.numeric(base::difftime(wind_data_avg$readout_time, anchor_time, units = 'secs'))
  
  for(j in 1:length(WndwAgr)){
    Wndw <- as.numeric(WndwAgr[j])
    wind_data_avg<- wind_data
    wind_data_avg$windowStart <- anchor_time + base::floor(dt_secs / (Wndw*60)) * (Wndw*60)    

    # Compute first-pass vector components for each averaging period.
    wind_data_avg <- wind_data_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        yBar = safe_mean(base::sin(direction_corrected_rad)),
        xBar = safe_mean(base::cos(direction_corrected_rad)),
        n = base::sum(!base::is.na(direction_corrected_rad))
      ) %>%
      dplyr::ungroup()

    # Second pass — Eq (10): mean unit-vector wind direction (radians)
    wind_data_avg <- wind_data_avg %>%
      dplyr::mutate(
        direction_unit_vector_mean = dplyr::case_when(
          xBar > 0 & yBar == 0 ~ 2 * pi,
          TRUE ~ (2 * pi + base::atan2(yBar, xBar)) %% (2 * pi)
        )
      )

    # Eq (12): minimum angular distance between each observation and the window mean
    wind_data_avg <- wind_data_avg %>%
      dplyr::mutate(
        min_angular_distance = 2 * base::atan(base::tan(0.5 * (direction_corrected_rad - direction_unit_vector_mean)))
      )

    # Eq (13): mean of minimum angular distances over the window
    wind_data_avg <- wind_data_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        A_Tbar = safe_mean(min_angular_distance)
      ) %>%
      dplyr::ungroup()

    # Eq (14): arithmetic mean wind direction (radians)
    # theta_T = [2*pi + theta_bar + A_Tbar] %% (2*pi)
    wind_data_avg <- wind_data_avg %>%
      dplyr::mutate(
        windDirMean = (2 * pi + direction_unit_vector_mean + A_Tbar) %% (2 * pi)
      )

    # Eq (15): sample variance of wind direction
    # s^2 = n^{-1} * sum(A_i^2) - A_Tbar^2
    wind_data_avg <- wind_data_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        windDirVar = safe_mean(min_angular_distance^2) - A_Tbar^2
      ) %>%
      dplyr::ungroup()

    # Eq (16): convert mean wind direction from radians to degrees
    # theta_T_deg = theta_T_rad * (180 / pi)
    wind_data_avg <- wind_data_avg %>%
      dplyr::mutate(
        windDirMean = windDirMean * (180 / pi)
      )

    # Eq (17): convert sample variance from rad^2 to deg^2
    # s^2_deg = s^2_rad * (180/pi)^2
    wind_data_avg <- wind_data_avg %>%
      dplyr::mutate(
        windDirVar = windDirVar * (180 / pi)^2
      )

    #reduce file to one row per observation window
    wind_data_avg <- wind_data_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    #only keep the necessary columns for further analysis
    dataOut <- wind_data_avg[, c("readout_time", "source_id", "site_id", "windDirMean", "windDirVar", "n")]
    flagsOut <- wind_data_avg[, c("readout_time", "source_id", "site_id", "buoyWindDirDeadZone")]
    
    
    #' Write out data file
    dataFileName<-paste0(DirOutData_rmyoung,"/",tools::file_path_sans_ext(dataFileName_rmyoung), "_", WndwAgr[j], ".parquet")
    rptOutData <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                                      NameFile = dataFileName,
                                                      Schm = SchmDataOut),silent=TRUE)
    if(class(rptOutData)[1] == 'try-error'){
      log$error(base::paste0('Cannot write Data to ',dataFileName,'. ',attr(rptOutData, "condition")))
      stop()
    } else {
      log$info(base::paste0('Data written successfully in ', dataFileName))
    }
    
    #' Write out flags file
    flagFileName<-paste0(DirOutFlags_rmyoung,"/",tools::file_path_sans_ext(dataFileName_rmyoung), "_", WndwAgr[j], "_deadband.parquet")    
    rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,
                                                      NameFile = flagFileName,
                                                      Schm = SchmFlagsOut),silent=TRUE)
    if(class(rptOutFlags)[1] == 'try-error'){
      log$error(base::paste0('Cannot write Flags to ',flagFileName,'. ',attr(rptOutFlags, "condition")))
      stop()
    } else {
      log$info(base::paste0('Flags written successfully in ', flagFileName))
    }

  }  
}



