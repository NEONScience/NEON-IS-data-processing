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
#' Initial creation
##############################################################################################
wrap.wind.buoy.compass.correction <- function(DirIn,
                                  DirOutBase,
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

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,"/rmyoung/",config[1],"/",DirSubCopy),
                                       DirDest=base::paste0(DirOut,"/rmyoung/",config[1]),
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,"/hmr3300/",config[1],"/",DirSubCopy),
                                       DirDest=base::paste0(DirOut,"/hmr3300/",config[1]),
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  
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

  
  ###############
  #1. First need to make readout times consistent for the rmyoung and hmr3300 data.
  if(length(dataFileName_hmr3300)>0){
    missing<-data_rmyoung$readout_time[!data_rmyoung$readout_time %in% data_hmr3300$readout_time]
    #add rows to data_hmr3300 with missing readout times
    if(length(missing) > 0){
      newRows <- data_hmr3300[rep(1, length(missing)), , drop = FALSE]
      newRows[] <- NA
      newRows$readout_time <- missing
      data_hmr3300 <- base::rbind(data_hmr3300, newRows)
      data_hmr3300 <- data_hmr3300[base::order(data_hmr3300$readout_time),]
    }
    #remove rows from data_hmr3300 that are not in data_rmyoung
    data_hmr3300 <- data_hmr3300[data_hmr3300$readout_time %in% data_rmyoung$readout_time,]
  }  

  ###############
  #2. Apply dead band flag on uncorrected but calibrated wind data. Flag is informational only, does not go into final QF.
  data_rmyoung$buoyWindDirDeadZone <- ifelse(
    is.na(data_rmyoung$direction_calibrated),
    -1,
    ifelse(data_rmyoung$direction_calibrated >= 355 | data_rmyoung$direction_calibrated == 0, 1, 0)
  )
  data_rmyoung$direction_calibrated[data_rmyoung$buoyWindDirDeadZone == 1] <- 357.5
  log$debug(base::paste0('Applied dead band flag on rmyoung wind data.'))


  #3. Magnetic declination and compass offset from thresholds
  if(length(dataFileName_hmr3300)>0 && length(DirInThresholds_hmr3300) > 0){
    #read in hmr3300 thresholds
    fileThsh <- base::dir(DirInThresholds_hmr3300,full.names=TRUE)
    
    # Read in the thresholds file (read first file only, there should only be 1)
    if(base::length(fileThsh) > 1){
      fileThsh <- fileThsh[1]
      log$info(base::paste0('There is more than one threshold file in ',DirInThresholds_hmr3300,'. Using ',fileThsh))
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
    data_hmr3300$compassOffset <- getThresholdByTime(data_hmr3300$readout_time, compassOffsets)
    data_hmr3300$compassMag <- getThresholdByTime(data_hmr3300$readout_time, compassMags)

    #Instantaneous buoy compass direction must then be converted from unadjusted digital compass measurements
    #to magnetic-declination/offset-adjusted digital compass measurements
    data_hmr3300$compass_direction_adjusted <- (data_hmr3300$direction + data_hmr3300$compassMag + data_hmr3300$compassOffset) %% 360 
    
  }else{
    log$info(base::paste0('No buoy compass thresholds files in ',DirInThresholds_hmr3300))
    data_hmr3300$compass_direction_adjusted <- NA
  }  

  
  ###############
  #4. Apply azimuth from named location
  dirLocLocation <- base::dir(DirInLocations_rmyoung,full.names=TRUE)
  data_rmyoung$azimuth <- NA
  
  #Could be multiple source IDs in a day. Account for all.
  data_rmyoung_blank <- data_rmyoung[is.na(data_rmyoung$source_id) | data_rmyoung$source_id == "99999", ]
  data_rmyoung_notblank<-data_rmyoung[!is.na(data_rmyoung$source_id) & data_rmyoung$source_id!= "99999",]
  sources <- unique(data_rmyoung_notblank$source_id)

  if(length(sources)>0){
    for(n in 1:length(sources)){
      source_n<-sources[n]
      data_rmyoung_n <- data_rmyoung_notblank[data_rmyoung_notblank$source_id == source_n, ]
      #get location history
      if(!is.null(dirLocLocation) && any(grepl(source_n,dirLocLocation))){
        # Choose the _locations.json file
        LocationFile <- base::paste0(dirLocLocation[grep(source_n,dirLocLocation)])
        LocationFile <-LocationFile[1]
        log$debug(base::paste0("location datum(s) found, reading in: ",LocationFile))
        LocationHist <- NEONprocIS.base::def.loc.geo.hist(LocationFile, log = NULL)
      } else { 
        log$debug(base::paste0('No location data files in ',DirInLocations_rmyoung, 'for source id ',source_n))
        LocationHist <-NULL
      }
      
      # Which location history matches each readout_time
      rmyoung_all_n <- NULL
      if(!is.null(LocationHist) && length(LocationHist$CFGLOC)>0){
        for(i in 1:length(LocationHist$CFGLOC)){
          startDate<-LocationHist$CFGLOC[[i]]$start_date
          endDate<-LocationHist$CFGLOC[[i]]$end_date
          rmyoung_subset<-data_rmyoung_n[data_rmyoung_n$readout_time>=startDate & data_rmyoung_n$readout_time<endDate,]
          if(length(rmyoung_subset$readout_time) > 0){
            if(is.null(LocationHist$CFGLOC[[i]]$gamma) || is.na(LocationHist$CFGLOC[[i]]$gamma)){
              rmyoung_subset$azimuth <- 0
            }else{
              rmyoung_subset$azimuth <- LocationHist$CFGLOC[[i]]$gamma
            }
          }
          if(i==1){
            rmyoung_all_n <- rmyoung_subset
          }else{
            rmyoung_all_n <- rbind(rmyoung_all_n,rmyoung_subset)
          }
        }
          rmyoung_n <- rmyoung_all_n
        }else {
          rmyoung_n <- data_rmyoung_n  # no azimuth info, keep data as-is
        }
      
      if(n==1){
        rmyoung_all <- rmyoung_n
      }else{
        rmyoung_all <- rbind(rmyoung_all,rmyoung_n)
      }
    }
    
    #add back in NA data
    rmyoungData <- rbind(rmyoung_all, data_rmyoung_blank)
    rmyoungData <- rmyoungData[order(rmyoungData$readout_time),]
  }else{
    rmyoungData <- data_rmyoung_notblank
  }

  # Merge the buoy compass adjusted direction with the rmyoung data based on readout_time
  if(length(data_hmr3300$readout_time)>0){
    wind_data <- merge(rmyoungData, data_hmr3300[, c("readout_time", "compass_direction_adjusted")], by="readout_time", all=TRUE)
  }else{
    wind_data <- rmyoungData
    wind_data$compass_direction_adjusted <- NA
  }
  
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
  # Write out files
  #only keep the necessary columns for further analysis
  dataOut <- wind_data[, c("readout_time", "source_id", "site_id", "speed_calibrated","direction_corrected_rad")]
  flagsOut <- wind_data[, c("readout_time", "source_id", "site_id", "buoyWindDirDeadZone")]
    
    
  #' Write out data file
  dataFilePathOut<-paste0(DirOutData_rmyoung,"/",dataFileName_rmyoung)
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
  flagFilePathOut<-paste0(DirOutFlags_rmyoung,"/",tools::file_path_sans_ext(dataFileName_rmyoung),"_deadband.parquet")    
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



