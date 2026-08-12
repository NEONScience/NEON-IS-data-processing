##############################################################################################
#' @title Wrapper for buoy wind-specific quality flagging and compass correction

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}
#' 
#' @description Wrapper function. Uses thresholds to apply sensor-specific quality flags to buoy wind data and performs compass correction.

#'
#' @param DirIn Character value. The base file path to the input data, QA/QC plausibility flags and quality flag thresholds.
#'  
#' @param DirOutBase Character value. The base file path for the output data.
#' 
#' @param SensWind Character value. The name of the wind sensor.
#' 
#' @param SensCompass Character value. The name of the compass sensor.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
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
#' Nora Catolico (2026-07-10)
#'  Initial creation
#' Nora Catolico (2026-08-12)
#'  Added SensWind and SensCompass parameters. 
#'  Added floor date standardization to 4 second intervals for both wind and compass data.
##############################################################################################
wrap.wind.buoy.compass.correction <- function(DirIn,
                                  DirOutBase,
                                  SensWind=SensWind,
                                  SensCompass=SensCompass,
                                  SchmDataOut=NULL,
                                  SchmFlagsOut=NULL,
                                  DirSubCopy=NULL,
                                  log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)

  timeBgn <-InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  all_starts <- seq(timeBgn, timeEnd - 60, by = 60)
  
  DirIn_wind <- paste0(DirIn,"/",SensWind)
  DirIn_compass <- paste0(DirIn,"/",SensCompass)

  DirIn_wind_config<-NEONprocIS.base::def.dir.in(DirBgn = DirIn_wind,
                              nameDirSub = c('data'),
                              log = log)
  config <- base::unique(base::basename(DirIn_wind_config))
  DirInData_wind <- paste0(DirIn_wind_config,"/data")
  DirInLocations_wind <- paste0(DirIn_wind_config,"/location")
  
  DirIn_compass_config<-NEONprocIS.base::def.dir.in(DirBgn = DirIn_compass,
                              nameDirSub = c('data'),
                              log = log)  
  DirInData_compass <- paste0(DirIn_compass_config,"/data")
  DirInThresholds_compass <- paste0(DirIn_compass_config,"/threshold")
  
  #create output directories
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,"/",SensWind,"/",config[1],"/data")
  DirOutFlags <- base::paste0(DirOut,"/",SensWind,"/",config[1],"/flags") 
  base::dir.create(DirOutData,recursive=TRUE)
  base::dir.create(DirOutFlags,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,"/",SensWind,"/",config[1],"/",DirSubCopy),
                                       DirDest=base::paste0(DirOut,"/",SensWind,"/",config[1]),
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,"/",SensCompass,"/",config[1],"/",DirSubCopy),
                                       DirDest=base::paste0(DirOut,"/",SensCompass,"/",config[1]),
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  
  # Read in parquet file of buoy wind data.
  dataFileName_wind<-base::list.files(DirInData_wind,full.names=FALSE)
  if(length(dataFileName_wind)==0){
    log$error(base::paste0('Data file not found in ', DirInData_wind)) 
    stop()
  } else if(length(dataFileName_wind)>1){
    log$error(base::paste0('More than one data file found in ', DirInData_wind))
    stop()
  } else {
    data_wind<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData_wind, '/', dataFileName_wind),
                                                       log = log),silent = FALSE)
    if(class(data_wind)[1] == 'try-error'){
      log$error(base::paste0('Error reading in data file: ', DirInData_wind, '/', dataFileName_wind)) 
      stop()
    }else{
      log$debug(base::paste0('Successfully read in file: ',dataFileName_wind))
      data_wind$readout_time <- as.POSIXct(data_wind$readout_time, origin="1970-01-01", tz="GMT")
      if(all(data_wind$source_id == "99999")){
        #clean up empty df
        data_wind <- data_wind[0,]
        #add a row for each readout time in all_starts
        data_wind <- data_wind[rep(1, length(all_starts)), , drop = FALSE]
        data_wind$readout_time <- all_starts
      }
    }    
  }
  
  #Read in parquet file of buoy compass data.
  dataFileName_compass<-base::list.files(DirInData_compass,full.names=FALSE)
  if(length(dataFileName_compass)==0){
    log$error(base::paste0('compass file not found in ', DirInData_compass))
    stop()
  } else if(length(dataFileName_compass)>1){
    log$error(base::paste0('More than one compass data file found in ', DirInData_compass))
    stop()
  } else {
    data_compass<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInData_compass, '/', dataFileName_compass),
                                                       log = log),silent = FALSE)    
    if(class(data_compass)[1] == 'try-error'){
      log$error(base::paste0('Error reading in data file: ', DirInData_compass, '/', dataFileName_compass)) 
      stop()
    }else{
      log$debug(base::paste0('Successfully read in file: ',dataFileName_compass))
      data_compass$readout_time <- as.POSIXct(data_compass$readout_time, origin="1970-01-01", tz="GMT")
      if(all(data_compass$source_id == "99999")){
        #clean up empty df
        data_compass <- data_compass[0,]
        #add a row for each readout time in all_starts
        data_compass <- data_compass[rep(1, length(all_starts)), , drop = FALSE]
        data_compass$readout_time <- all_starts
      }
    }    
  }

  
  ###############
  #1. First need to make readout times consistent for the wind and compass data.
  # readings occur every 4 seconds for seconds 2 through 42 of each minute typically
  # convert readings to standard floor of each 4 second interval
  data_wind$readout_time <- as.POSIXct(floor(as.numeric(data_wind$readout_time) / 4) * 4, origin="1970-01-01", tz="GMT")

  if(length(dataFileName_compass)>0){
    data_compass$readout_time <- as.POSIXct(floor(as.numeric(data_compass$readout_time) / 4) * 4, origin="1970-01-01", tz="GMT")
    missing<-data_wind$readout_time[!data_wind$readout_time %in% data_compass$readout_time]
    #add rows to data_compass with missing readout times
    if(length(missing) > 0){
      newRows <- data_compass[rep(1, length(missing)), , drop = FALSE]
      newRows[] <- NA
      newRows$readout_time <- missing
      data_compass <- base::rbind(data_compass, newRows)
      data_compass <- data_compass[base::order(data_compass$readout_time),]
    }
    #remove rows from data_compass that are not in data_wind
    data_compass <- data_compass[data_compass$readout_time %in% data_wind$readout_time,]
  }  

  ###############
  #2. Apply dead band flag on uncorrected but calibrated wind data. Flag is informational only, does not go into final QF.
  data_wind$buoyWindDirDeadZone <- -1
  data_wind$buoyWindDirDeadZone[data_wind$direction_calibrated >= 355] <- 1
  data_wind$buoyWindDirDeadZone[data_wind$direction_calibrated < 355] <- 0
  data_wind$direction_calibrated[data_wind$buoyWindDirDeadZone == 1] <- 357.5
  log$debug(base::paste0('Applied dead band flag on wind wind data.'))


  #3. Magnetic declination and compass offset from thresholds
  if(length(dataFileName_compass)>0 && length(dir(DirInThresholds_compass)) > 0){
    #read in compass thresholds
    fileThsh <- base::dir(DirInThresholds_compass,full.names=TRUE)
    
    # Read in the thresholds file (read first file only, there should only be 1)
    if(base::length(fileThsh) > 1){
      fileThsh <- fileThsh[1]
      log$info(base::paste0('There is more than one threshold file in ',DirInThresholds_compass,'. Using ',fileThsh))
    }
    thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df((NameFile=base::paste0(fileThsh)))
    # Verify that the terms listed in the input parameters are included in the threshold files
    termTest <- c('vectorAverageHeading')
    exstThsh <- termTest %in% base::unique(thsh$term_name) # Do the terms exist in the thresholds
    if(base::sum(exstThsh) != base::length(termTest)){
      log$error(base::paste0('Thresholds for term(s): ',base::paste(termTest[!exstThsh],collapse=','),' do not exist in the thresholds file.')) 
    }

    #determine offset and magnetic declination thresholds for the buoy compass
    compassOffsets <- thsh[thsh$threshold_name == '2D wind direction buoy compass offset',]
    compassMags <- thsh[thsh$threshold_name == '2D wind direction buoy magnetic declination angle',]
    getThresholdByTime <- function(timeVals, thshDf) {
      if (nrow(thshDf) == 0) {
        return(rep(NA_real_, length(timeVals)))
      }
      vapply(timeVals, function(tt) {
        idx <- thshDf$start_date <= tt & (is.na(thshDf$end_date) | thshDf$end_date >= tt)
        vals <- thshDf$number_value[idx]
        if (length(vals) == 0) {
          NA_real_
        } else {
          as.numeric(vals[1])
        }
      }, numeric(1))
    }
    data_compass$compassOffset <- getThresholdByTime(data_compass$readout_time, compassOffsets)
    data_compass$compassMag <- getThresholdByTime(data_compass$readout_time, compassMags)

    #Instantaneous buoy compass direction must then be converted from unadjusted digital compass measurements
    #to magnetic-declination/offset-adjusted digital compass measurements
    data_compass$compass_direction_adjusted <- (data_compass$direction + data_compass$compassMag + data_compass$compassOffset) %% 360 
    
  }else{
    log$info(base::paste0('No buoy compass thresholds files in ',DirInThresholds_compass))
    data_compass$compass_direction_adjusted <- NA
  }  

  
  ###############
  #4. Apply azimuth from named location
  dirLocLocation <- base::dir(DirInLocations_wind,full.names=TRUE)
  data_wind$azimuth <- NA
  
  #Could be multiple source IDs in a day. Account for all.
  data_wind_blank <- data_wind[is.na(data_wind$source_id) | data_wind$source_id == "99999", ]
  data_wind_notblank<-data_wind[!is.na(data_wind$source_id) & data_wind$source_id!= "99999",]
  sources <- unique(data_wind_notblank$source_id)

  if(length(sources)>0){
    for(n in 1:length(sources)){
      source_n<-sources[n]
      data_wind_n <- data_wind_notblank[data_wind_notblank$source_id == source_n, ]
      #get location history
      if(!is.null(dirLocLocation) && any(grepl(source_n,dirLocLocation))){
        # Choose the _locations.json file
        LocationFile <- base::paste0(dirLocLocation[grep(source_n,dirLocLocation)])
        LocationFile <-LocationFile[1]
        log$debug(base::paste0("location datum(s) found, reading in: ",LocationFile))
        LocationHist <- NEONprocIS.base::def.loc.geo.hist(LocationFile, log = NULL)
      } else { 
        log$debug(base::paste0('No location data files in ',DirInLocations_wind, 'for source id ',source_n))
        LocationHist <-NULL
      }
      
      # Which location history matches each readout_time
      wind_all_n <- NULL
      if(!is.null(LocationHist) && length(LocationHist$CFGLOC)>0){
        for(i in 1:length(LocationHist$CFGLOC)){
          startDate<-LocationHist$CFGLOC[[i]]$start_date
          endDate<-LocationHist$CFGLOC[[i]]$end_date
          wind_subset<-data_wind_n[data_wind_n$readout_time>=startDate & data_wind_n$readout_time<endDate,]
          if(length(wind_subset$readout_time) > 0){
            if(is.null(LocationHist$CFGLOC[[i]]$gamma) || is.na(LocationHist$CFGLOC[[i]]$gamma)){
              wind_subset$azimuth <- 0
            }else{
              wind_subset$azimuth <- LocationHist$CFGLOC[[i]]$gamma
            }
          }
          if(i==1){
            wind_all_n <- wind_subset
          }else{
            wind_all_n <- rbind(wind_all_n,wind_subset)
          }
        }
          wind_n <- wind_all_n
        }else {
          wind_n <- data_wind_n  # no azimuth info, keep data as-is
        }
      
      if(n==1){
        wind_all <- wind_n
      }else{
        wind_all <- rbind(wind_all,wind_n)
      }
    }
    
    #add back in NA data
    windData <- rbind(wind_all, data_wind_blank)
    windData <- windData[order(windData$readout_time),]
  }else{
    windData <- data_wind_blank
  }

  # Merge the buoy compass adjusted direction with the wind data based on readout_time
  if(length(data_compass$readout_time)>0){
    # rename column compass$direction to compass_direction_raw
    names(data_compass)[names(data_compass) == "direction"] <- "compass_direction_raw"
    combined_data <- merge(windData, data_compass[, c("readout_time", "compass_direction_raw", "compass_direction_adjusted")], by="readout_time", all.x=TRUE)
    if(nrow(combined_data) != nrow(windData)){
      log$error(base::paste0('The number of rows in the merged wind data (',nrow(combined_data),') does not match the number of rows in the wind data (',nrow(windData),').'))
    }
  }else{
    combined_data <- windData
    combined_data$compass_direction_adjusted <- NA
    combined_data$compass_direction_raw <- NA
  }
  
  #The wind direction measurements corrected by buoy compass data is calculated by summing the uncorrected 
  #but calibrated wind direction measurements, the declination-adjusted compass measurements,
  #and the wind-monitor on-mast offset (azimuth) from the Named Location Database
  combined_data$direction_corrected <- (combined_data$direction_calibrated + combined_data$compass_direction_adjusted + combined_data$azimuth) %% 360

  ###############
  #5. set to 0 when no wind
  combined_data$direction_corrected[combined_data$speed_calibrated <= 0.5] <- 0
  combined_data$buoyWindDirCalmWind <- -1
  combined_data$buoyWindDirCalmWind[combined_data$speed_calibrated <= 0.5] <- 1
  combined_data$buoyWindDirCalmWind[combined_data$speed_calibrated > 0.5] <- 0

  ###############
  #6. Unit-vector mean wind direction must be converted from degrees to radians,
  #according to the meteorological coordinate system 
  combined_data$direction_corrected_rad <- combined_data$direction_corrected * (pi / 180)

  ###############
  # Write out files
  #only keep the necessary columns for further analysis
  dataOut <- combined_data[, c("readout_time", "source_id", "site_id", "speed_calibrated","compass_direction_raw","direction_calibrated","direction_corrected","direction_corrected_rad","azimuth")]
  flagsOut <- combined_data[, c("readout_time", "buoyWindDirDeadZone","buoyWindDirCalmWind")]
    
    
  #' Write out data file
  dataFilePathOut<-paste0(DirOutData,"/",dataFileName_wind)
  rptOutData <- try(NEONprocIS.base::def.wrte.parq(data = dataOut,
                                                    NameFile = dataFilePathOut,
                                                    Schm = SchmDataOut),silent=TRUE)
  if(class(rptOutData)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Data to ',dataFilePathOut,'. ',attr(rptOutData, "condition")))
    stop()
  } else {
    log$info(base::paste0('Data written successfully in ', dataFilePathOut))
  }
    
  #' Write out flags file
  flagFilePathOut<-paste0(DirOutFlags,"/",tools::file_path_sans_ext(dataFileName_wind),"_deadband.parquet")
  rptOutFlags <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,
                                                    NameFile = flagFilePathOut,
                                                    Schm = SchmFlagsOut),silent=TRUE)
  if(class(rptOutFlags)[1] == 'try-error'){
    log$error(base::paste0('Cannot write Flags to ',flagFilePathOut,'. ',attr(rptOutFlags, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', flagFilePathOut))
  }  
}



