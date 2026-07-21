##############################################################################################
#' @title Wrapper for buoy wind-specific direction statistics and uncertainty calculations

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply wind direction statistics and uncertainty calculations to buoy wind data.
#'
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility flags and quality flag thresholds.
#'  
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param WndwAgr Character value. The window aggregation period for the buoy wind data (e.g., "002" for 2-minute averages, "030" for 30-minute averages).
#' 
#' @param SchmStatsOut (optional), A json-formatted character string containing the schema for the data file.
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
wrap.wind.buoy.direction.stats.ucrt <- function(DirIn,
                                  DirOutBase,
                                  WndwAgr,
                                  SchmStatsOut=NULL,
                                  SchmFlagsOut=NULL,
                                  log=NULL
){
  
  # Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)

  DirInData <- paste0(DirIn,"/data")
  DirInFlags<- paste0(DirIn,"/flags")
  DirInThresholds <- paste0(DirIn,"/threshold")
  DirInUcrt <- paste0(DirIn,"/uncertainty_data")
  DirInUcrtCoef <- base::paste0(DirIn,'/uncertainty_coef')
  
  # Create output directories
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutStats <- base::paste0(DirOut,"/stats")
  base::dir.create(DirOutStats,recursive=TRUE)
  
  # Read in parquet file of buoy wind data.
  dataFileName <- base::list.files(DirInData,full.names=FALSE) 
  dataFileName <- dataFileName[!base::grepl("zone.identifier", tolower(dataFileName))]
  if(length(dataFileName)==0){
    log$error(base::paste0('Data file not found in ', DirInData)) 
    stop()
  } else {
    data_wind <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData, '/', dataFileName),
                                                       log = log),silent = FALSE)
    if(class(data_wind)[1] == 'try-error'){
      log$error(base::paste0('Error reading in data file: ', DirInData, '/', dataFileName)) 
      stop()
    }else{
      log$debug(base::paste0('Successfully read in file: ',dataFileName))
      data_wind$readout_time <- as.POSIXct(data_wind$readout_time, origin="1970-01-01", tz="GMT")
    } 
  }
  
  # Read in plausibility flag file
  flagFileName <- base::list.files(DirInFlags,full.names=FALSE)
  flags_plauName <- flagFileName[base::grepl("plausibility", tolower(flagFileName)) & !base::grepl("zone.identifier", tolower(flagFileName))]
  if(length(flags_plauName)==0){
    log$error(base::paste0('Plausibility flag file not found in ', DirInFlags)) 
    stop()
  } else {
    flags_plau <- base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags, '/', flags_plauName),
                                                       log = log),silent = FALSE)
    if(class(flags_plau)[1] == 'try-error'){
      log$error(base::paste0('Error reading in plausibility flag file: ', DirInFlags, '/', flags_plauName)) 
      stop()
    }else{
      log$debug(base::paste0('Successfully read in file: ',flags_plauName))
    }
  }

  #read in buoy wind uncertainty coefficients and data
  ucrtFileName <- base::list.files(DirInUcrt,full.names=FALSE)
  ucrtFileName <- ucrtFileName[!base::grepl("zone.identifier", tolower(ucrtFileName))]
  ucrt_data<- base::try(NEONprocIS.base::def.read.parq(NameFile=base::paste0(DirInUcrt, '/', ucrtFileName), log=log), silent = FALSE)
  if(class(ucrt_data)[1] == 'try-error'){
    log$error(base::paste0('Error reading in uncertainty data file: ', DirInUcrt, '/', ucrtFileName)) 
    stop()
  }else{
    log$debug(base::paste0('Successfully read in uncertainty data file: ', ucrtFileName))
  }
  
  ucrtCoefFileName <- base::list.files(DirInUcrtCoef,full.names=FALSE)
  ucrtCoefFileName <- ucrtCoefFileName[!base::grepl("zone.identifier", tolower(ucrtCoefFileName))]
  if(base::length(ucrtCoefFileName) != 1){
      log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ",DirInUcrtCoef,"... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
      ucrtCoef <- base::list()
    } else {
      
      # Open the uncertainty file
      ucrtCoef  <- base::try(rjson::fromJSON(file=base::paste0(DirInUcrtCoef, '/', ucrtCoefFileName),simplify=TRUE),silent=FALSE)
      if(base::class(ucrtCoef) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', ucrtCoefFileName, ' is unreadable.')) 
        stop()
      }
      # Turn times to POSIX
      ucrtCoef <- base::lapply(ucrtCoef,FUN=function(idxUcrt){
        idxUcrt$start_date <- base::strptime(idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        idxUcrt$end_date <- base::strptime(idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        return(idxUcrt)
      })
      log$debug(base::paste0('Successfully read uncertainty coefficients from file: ',ucrtCoefFileName))
    }

  #merge with the buoy wind data
  data_wind <- merge(data_wind, ucrt_data[, c("readout_time", "direction_ucrtMeas", "direction_ucrtComb","direction_ucrtExpn")], by="readout_time", all.x=TRUE)


  #read in compass thresholds
  thresholdFileName<-base::list.files(DirInThresholds,full.names=FALSE)
  thresholdFileName <- thresholdFileName[!base::grepl("zone.identifier", tolower(thresholdFileName))]
  windThresholds<-base::try(NEONprocIS.qaqc::def.read.thsh.qaqc.df(NameFile = base::paste0(DirInThresholds, '/', thresholdFileName)),silent = FALSE)
  if(class(windThresholds)[1] == 'try-error'){
    log$warn(base::paste0('Failed to read threshold file: ',thresholdFileName))
    data_wind$magDecUcrtValue<-NA_real_
    data_wind$compassUcrtValue<-NA_real_
  }else{
    magDecUcrt <- windThresholds[(windThresholds$threshold_name=="2D Wind Direction Buoy Magnetic declination uncertainty"),]
    data_wind$magDecUcrtValue<-magDecUcrt$number_value[1]
    compassUcrt <- windThresholds[(windThresholds$threshold_name=="2D Wind Direction Buoy compass uncertainty"),]
    data_wind$compassUcrtValue<-compassUcrt$number_value[1]
  }


  testfile <- "NEON.D03.BARC.DP1.20059.001.103.100.002.WSDBuoy_2min.2025-12.expanded.20260504T185356Z.csv"
  data_wind<-read.csv(file=base::paste0("/home/ncatolico/Git/pfs/test/", testfile),header=TRUE,stringsAsFactors=FALSE)
  data_wind$readout_time<-as.POSIXct(data_wind$startDateTime,format="%Y-%m-%dT%H:%M:%OSZ",tz="GMT")
  data_wind<-data_wind[data_wind$readout_time>="2025-12-17 00:00:00" & data_wind$readout_time<"2025-12-18 00:00:00",]

  ################
  # Calculate the mean and variance with analytical two-pass method
  # In the first pass, the components of the average distance vector over an observation period with sample size n are calculated 
  # The first window begins at the nearest whole minute <= first timestamp in the series.
  anchor_time <- lubridate::floor_date(base::min(data_wind$readout_time, na.rm = TRUE), unit = 'minute')

  safe_mean <- function(x) {
    if (base::all(base::is.na(x))) {
      return(NA_real_)
    }
    base::mean(x, na.rm = TRUE)
  }

  # Build anchored window starts.
  dt_secs <- as.numeric(base::difftime(data_wind$readout_time, anchor_time, units = 'secs'))
  
  # Loop through each window aggregation period and calculate the mean and variance of wind direction
  for(j in 1:length(WndwAgr)){
    Wndw <- as.numeric(WndwAgr[j])
    data_wind_avg <- data_wind
    data_wind_avg$windowStart <- anchor_time + base::floor(dt_secs / (Wndw*60)) * (Wndw*60)    
    data_wind_avg$expUncert <- NA_real_

    # Uncertainty calculations for wind direction statistics by time bin
    data_wind_avg <- data_wind_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::group_modify(~{
        log$debug(base::paste0(
          'Calculating uncertainty for window aggregation period: ',
          WndwAgr[j],
          ' and time bin: ',
          .y$windowStart
        ))
        .x$expUncert <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(
          data = .x,
          VarUcrt = 'buoyWindDirMean',
          ucrtCoef = ucrtCoef
        )
        .x
      }) %>%
      dplyr::ungroup()
    
    # Compute first-pass vector components for each averaging period.
    data_wind_avg <- data_wind_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        yBar = safe_mean(base::sin(direction_rad)),
        xBar = safe_mean(base::cos(direction_rad)),
        n = base::sum(!base::is.na(direction_rad))
      ) %>%
      dplyr::ungroup()

    # Second pass — Eq (10): mean unit-vector wind direction (radians)
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        direction_unit_vector_mean = dplyr::case_when(
          xBar > 0 & yBar == 0 ~ 2 * pi,
          TRUE ~ (2 * pi + base::atan2(yBar, xBar)) %% (2 * pi)
        )
      )

    # Eq (12): minimum angular distance between each observation and the window mean
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        min_angular_distance = 2 * base::atan(base::tan(0.5 * (direction_rad - direction_unit_vector_mean)))
      )

    # Eq (13): mean of minimum angular distances over the window
    data_wind_avg <- data_wind_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        A_Tbar = safe_mean(min_angular_distance)
      ) %>%
      dplyr::ungroup()

    # Eq (14): arithmetic mean wind direction (radians)
    # theta_T = [2*pi + theta_bar + A_Tbar] %% (2*pi)
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        windDirMean = (2 * pi + direction_unit_vector_mean + A_Tbar) %% (2 * pi)
      )

    # Eq (15): sample variance of wind direction
    # s^2 = n^{-1} * sum(A_i^2) - A_Tbar^2
    data_wind_avg <- data_wind_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::mutate(
        windDirVar = safe_mean(min_angular_distance^2) - A_Tbar^2
      ) %>%
      dplyr::ungroup()

    # Eq (16): convert mean wind direction from radians to degrees
    # theta_T_deg = theta_T_rad * (180 / pi)
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        windDirMean = windDirMean * (180 / pi)
      )

    # Eq (17): convert sample variance from rad^2 to deg^2
    # s^2_deg = s^2_rad * (180/pi)^2
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        windDirVar = windDirVar * (180 / pi)^2
      )

    # calculate standard error of the mean wind direction for each observation window
    data_wind_avg <- data_wind_avg %>%
      dplyr::mutate(
        windDirSE = base::sqrt(windDirVar / n)
      )    

    # reduce file to one row per observation window
    data_wind_avg <- data_wind_avg %>%
      dplyr::group_by(windowStart) %>%
      dplyr::slice(1) %>%
      dplyr::ungroup()

    # only keep the necessary columns for further analysis
    statsOut <- data_wind_avg[, c("readout_time", "source_id", "site_id", "windDirMean", "windDirVar", "n","windDirSE")]


    # Write out stats file
    statsFileName<-paste0(DirOutStats,"/",tools::file_path_sans_ext(dataFileName), "_sciStats_", WndwAgr[j], ".parquet")
    rptOutData <- try(NEONprocIS.base::def.wrte.parq(data = statsOut,
                                                      NameFile = statsFileName,
                                                      Schm = SchmStatsOut),silent=TRUE)
    if(class(rptOutData)[1] == 'try-error'){
      log$error(base::paste0('Cannot write Data to ',statsFileName,'. ',attr(rptOutData, "condition")))
      stop()
    } else {
      log$info(base::paste0('Data written successfully in ', statsFileName))
    }




  }  
}



